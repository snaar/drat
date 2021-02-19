use std::io::{self, Error, ErrorKind, Read, Seek, SeekFrom};

use byteorder::{LittleEndian, ReadBytesExt};
use bzip2::read::BzDecoder;
use flate2::read::DeflateDecoder;

use crate::decompress::crc32::Crc32Reader;

const LOCAL_FILE_HEADER_SIGNATURE: u32 = 0x04034b50;
const CENTRAL_FILE_HEADER_SIGNATURE: u32 = 0x02014b50;
const END_OF_CENTRAL_DIR_RECORD_SIGNATURE: u32 = 0x06054b50;
const ZIP64_END_OF_CENTRAL_DIR_RECORD_SIGNATURE: u32 = 0x06064b50;
const ZIP64_END_OF_CENTRAL_DIR_LOCATOR_SIGNATURE: u32 = 0x07064b50;

const MAX_EOCDR_SEARCH_BACK_OFFSET: i64 = 1 << 20; // minizip lib uses this constant
const LOCAL_HEADER_FIXED_FIELDS_SIZE: i64 = 22; // excludes signature and variable fields
const END_OF_CENTRAL_DIRECTORY_RECORD_FIXED_FIELDS_SIZE: i64 = 18; // excludes signature and variable fields
const ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_FIXED_FIELDS_SIZE: i64 = 16; // excludes signature and variable fields
const SIGNATURE_SIZE: i64 = 4; // all of the record signatures are 4 bytes

// based on https://www.pkware.com/documents/casestudies/APPNOTE.TXT
//
// this implementation supports a very limited subset of zip archives,
// as needed by specific use cases and no more than that;
// notably, only single compressed file per archive is supported

/*
4.3.6 Overall .ZIP file format:
     [local file header 1]
     [encryption header 1]
     [file data 1]
     [data descriptor 1]
     ...
     [local file header n]
     [encryption header n]
     [file data n]
     [data descriptor n]
     [archive decryption header]
     [archive extra data record]
     [central directory header 1]
     ...
     [central directory header n]
     [zip64 end of central directory record]
     [zip64 end of central directory locator]
     [end of central directory record]
*/

pub fn is_zip<R: Read + Seek>(reader: &mut R) -> io::Result<bool> {
    if reader.seek(SeekFrom::End(0))? < 4 {
        // if we don't even have 4 bytes then it's not a zip file
        return Ok(false);
    }
    reader.seek(SeekFrom::Start(0))?;
    Ok(reader.read_u32::<LittleEndian>()? == LOCAL_FILE_HEADER_SIGNATURE)
}

pub fn new_reader_for_single_file_zip_archive<R: 'static + Read + Seek>(
    mut reader: R,
) -> io::Result<(Box<dyn Read>, String)> {
    // what's happening:
    // - try to figure out where central directory headers are
    //     - read end of central directory record
    //     - try to read zip64 end of central directory locator
    //     - if the above locator is present, read zip64 end of central directory record
    // - read the central directory header for the single file we have to find where the data is
    // - seek to start of the file data and "take" the reader for the compressed data size
    // - wrap the file data up in the appropriate decompression/crc reader chain and return the reader

    find_eocdr_and_prepare_to_read(&mut reader)?;
    let eocdr = EndOfCentralDirRecord::read(&mut reader)?;

    let start_of_central_dir = get_start_of_central_dir_offset(&mut reader, &eocdr)?;
    reader.seek(SeekFrom::Start(start_of_central_dir))?;
    let central_dir_header = CentralDirHeader::read(&mut reader)?;

    reader.seek(SeekFrom::Start(central_dir_header.local_header_offset))?;
    if reader.read_u32::<LittleEndian>()? != LOCAL_FILE_HEADER_SIGNATURE {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "local file header signature mismatch",
        ));
    }
    reader.seek(SeekFrom::Current(LOCAL_HEADER_FIXED_FIELDS_SIZE))?;
    let file_name_length = reader.read_u16::<LittleEndian>()? as i64;
    let extra_field_length = reader.read_u16::<LittleEndian>()? as i64;
    reader.seek(SeekFrom::Current(file_name_length))?;
    reader.seek(SeekFrom::Current(extra_field_length))?;

    let reader = reader.take(central_dir_header.compressed_size);
    let crc32 = central_dir_header.crc32;

    let reader: Box<dyn Read> = match central_dir_header.compression_method {
        0 => {
            // "0 - The file is stored (no compression)"
            Box::new(Crc32Reader::new(reader, crc32))
        }
        8 => {
            // "8 - The file is Deflated"
            let deflate_reader = DeflateDecoder::new(reader);
            Box::new(Crc32Reader::new(deflate_reader, crc32))
        }
        12 => {
            // "12 - File is compressed using BZIP2 algorithm"
            let bzip2_reader = BzDecoder::new(reader);
            Box::new(Crc32Reader::new(bzip2_reader, crc32))
        }
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unsupported compression method",
            ));
        }
    };

    Ok((reader, central_dir_header.file_name))
}

fn find_eocdr_and_prepare_to_read<T: Read + io::Seek>(reader: &mut T) -> io::Result<()> {
    let file_size = reader.seek(io::SeekFrom::End(0))?;
    let max_back = std::cmp::min(file_size, MAX_EOCDR_SEARCH_BACK_OFFSET as u64) as i64;

    let mut pos = 4; // '4' to make sure we can read the signature
    while pos < max_back {
        reader.seek(io::SeekFrom::End(-pos))?;
        if reader.read_u32::<LittleEndian>()? == END_OF_CENTRAL_DIR_RECORD_SIGNATURE {
            return Ok(());
        }
        pos += 1;
    }

    Err(Error::new(
        ErrorKind::InvalidData,
        "end of central directory signature missing",
    ))
}

fn get_start_of_central_dir_offset<R: Read + Seek>(
    reader: &mut R,
    eocdr: &EndOfCentralDirRecord,
) -> io::Result<u64> {
    if reader
        .seek(SeekFrom::End(
            -(SIGNATURE_SIZE
                + ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_FIXED_FIELDS_SIZE
                + SIGNATURE_SIZE
                + END_OF_CENTRAL_DIRECTORY_RECORD_FIXED_FIELDS_SIZE
                + eocdr.zip_file_comment_length),
        ))
        .is_err()
    {
        // kinda weird, but possibly just a very small compressed file with no zip64 records, so
        // assume it's a file with no zip64 records and fall back to end of central directory record
        return Ok(eocdr.start_of_central_directory_offset as u64);
    }

    if reader.read_u32::<LittleEndian>()? != ZIP64_END_OF_CENTRAL_DIR_LOCATOR_SIGNATURE {
        return Ok(eocdr.start_of_central_directory_offset as u64);
    }

    let zip64_eocdr_offset = read_zip64_eocdl_and_get_eocdr_offset(reader)?;
    reader.seek(io::SeekFrom::Start(zip64_eocdr_offset))?;
    if reader.read_u32::<LittleEndian>()? != ZIP64_END_OF_CENTRAL_DIR_RECORD_SIGNATURE {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "wrong zip64 end of central directory signature",
        ));
    }

    Ok(read_zip64_eocdr_and_get_central_dir_offset(reader)?)
}

fn read_zip64_eocdl_and_get_eocdr_offset<T: Read>(reader: &mut T) -> io::Result<u64> {
    let disk_with_central_directory = reader.read_u32::<LittleEndian>()?;
    let end_of_central_directory_offset = reader.read_u64::<LittleEndian>()?;
    let number_of_disks = reader.read_u32::<LittleEndian>()?;

    if number_of_disks != 1 || disk_with_central_directory != 0 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "multi-disk zip archives not supported",
        ));
    }

    Ok(end_of_central_directory_offset)
}

fn read_zip64_eocdr_and_get_central_dir_offset<T: Read + io::Seek>(
    reader: &mut T,
) -> io::Result<u64> {
    let _record_size = reader.read_u64::<LittleEndian>()?;
    let _version_made_by = reader.read_u16::<LittleEndian>()?;
    let _version_needed_to_extract = reader.read_u16::<LittleEndian>()?;
    let disk_number = reader.read_u32::<LittleEndian>()?;
    let disk_with_central_directory = reader.read_u32::<LittleEndian>()?;
    let number_of_files_on_this_disk = reader.read_u64::<LittleEndian>()?;
    let number_of_files = reader.read_u64::<LittleEndian>()?;
    let _central_directory_size = reader.read_u64::<LittleEndian>()?;
    let central_directory_offset = reader.read_u64::<LittleEndian>()?;

    if number_of_files != 1 || number_of_files_on_this_disk != 1 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "only zip files with precisely single compressed file are supported",
        ));
    }

    if disk_number != 0 || disk_with_central_directory != 0 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "multi-disk zip archives not supported",
        ));
    }

    Ok(central_directory_offset)
}

struct EndOfCentralDirRecord {
    pub central_directory_size: u32,
    pub start_of_central_directory_offset: u32,
    pub zip_file_comment_length: i64,
}

impl EndOfCentralDirRecord {
    pub fn read<T: Read + Seek>(reader: &mut T) -> io::Result<EndOfCentralDirRecord> {
        let disk_number = reader.read_u16::<LittleEndian>()?;
        let disk_number_with_start_of_central_directory = reader.read_u16::<LittleEndian>()?;
        let number_of_central_directory_entries_on_this_disk = reader.read_u16::<LittleEndian>()?;
        let total_number_of_central_directory_entries = reader.read_u16::<LittleEndian>()?;
        let central_directory_size = reader.read_u32::<LittleEndian>()?;
        let start_of_central_directory_offset = reader.read_u32::<LittleEndian>()?;
        let zip_file_comment_length = reader.read_u16::<LittleEndian>()? as i64;
        reader.seek(SeekFrom::Current(zip_file_comment_length))?;

        if total_number_of_central_directory_entries != 1
            || number_of_central_directory_entries_on_this_disk != 1
        {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "only zip files with precisely single compressed file are supported",
            ));
        }

        if disk_number != 0 || disk_number_with_start_of_central_directory != 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "multi-disk zip archives not supported",
            ));
        }

        Ok(EndOfCentralDirRecord {
            central_directory_size,
            start_of_central_directory_offset,
            zip_file_comment_length,
        })
    }
}

struct CentralDirHeader {
    pub compression_method: u16,
    pub crc32: u32,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub file_name: String,
    pub local_header_offset: u64,
    pub central_header_start: u64,
}

impl CentralDirHeader {
    pub fn read<R: Read + Seek>(reader: &mut R) -> io::Result<CentralDirHeader> {
        let central_header_start = reader.seek(SeekFrom::Current(0))?;
        if reader.read_u32::<LittleEndian>()? != CENTRAL_FILE_HEADER_SIGNATURE {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "wrong central file header signature",
            ));
        }

        let _version_made_by = reader.read_u16::<LittleEndian>()?;
        let _version_needed_to_extract = reader.read_u16::<LittleEndian>()?;
        let bit_flag = reader.read_u16::<LittleEndian>()?;
        let compression_method = reader.read_u16::<LittleEndian>()?;
        let _last_mod_file_time = reader.read_u16::<LittleEndian>()?;
        let _last_mod_file_date = reader.read_u16::<LittleEndian>()?;
        let crc32 = reader.read_u32::<LittleEndian>()?;
        let compressed_size = reader.read_u32::<LittleEndian>()? as u64;
        let uncompressed_size = reader.read_u32::<LittleEndian>()? as u64;
        let file_name_length = reader.read_u16::<LittleEndian>()? as usize;
        let extra_field_length = reader.read_u16::<LittleEndian>()? as u64;
        let file_comment_length = reader.read_u16::<LittleEndian>()? as i64;
        let disk_number_start = reader.read_u16::<LittleEndian>()?;
        let _internal_file_attributes = reader.read_u16::<LittleEndian>()?;
        let _external_file_attributes = reader.read_u32::<LittleEndian>()?;
        let local_header_offset = reader.read_u32::<LittleEndian>()? as u64;

        let mut file_name_bytes = vec![0; file_name_length];
        reader.read_exact(&mut file_name_bytes)?;

        let (uncompressed_size, compressed_size, local_header_offset) = Self::read_extra_field(
            reader,
            extra_field_length,
            uncompressed_size,
            compressed_size,
            local_header_offset,
        )?;

        // this is not being relied on at the moment, but might as well do it
        reader.seek(SeekFrom::Current(file_comment_length))?;

        if bit_flag & 1 != 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "encryption not supported",
            ));
        }

        if disk_number_start != 0 && disk_number_start != 0xffff {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "multi-disk zip archives not supported",
            ));
        }

        let file_name = if bit_flag & (1 << 11) != 0 {
            String::from_utf8_lossy(&*file_name_bytes).into_owned()
        } else {
            file_name_bytes
                .into_iter()
                .map(|b| std::char::from_u32(if b <= 0x7f { b as u32 } else { 0xfffd }).unwrap())
                .collect()
        };

        Ok(CentralDirHeader {
            compression_method,
            crc32,
            compressed_size,
            uncompressed_size,
            file_name,
            local_header_offset,
            central_header_start,
        })
    }

    fn read_extra_field<R: Read + Seek>(
        reader: &mut R,
        extra_field_length: u64,
        mut uncompressed_size: u64,
        mut compressed_size: u64,
        mut local_header_offset: u64,
    ) -> io::Result<(u64, u64, u64)> {
        let mut total_bytes_read = 0;

        while total_bytes_read < extra_field_length {
            let header_id = reader.read_u16::<LittleEndian>()?;
            let data_size = reader.read_u16::<LittleEndian>()? as i64;

            let mut data_read = 0;
            if header_id == 0x0001 {
                if uncompressed_size == 0xffff_ffff {
                    uncompressed_size = reader.read_u64::<LittleEndian>()?;
                    data_read += 8;
                }
                if compressed_size == 0xffff_ffff {
                    compressed_size = reader.read_u64::<LittleEndian>()?;
                    data_read += 8;
                }
                if local_header_offset == 0xffff_ffff {
                    local_header_offset = reader.read_u64::<LittleEndian>()?;
                    data_read += 8;
                }
            }

            if data_read > data_size {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "malformed extensible data",
                ));
            }
            reader.seek(SeekFrom::Current(data_size - data_read))?;

            total_bytes_read += data_size as u64;
        }

        Ok((uncompressed_size, compressed_size, local_header_offset))
    }
}
