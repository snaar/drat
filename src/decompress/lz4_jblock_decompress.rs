use std::io;
use std::io::{Error, ErrorKind};

/// goal is to match LZ4*FastDecompressor.java, which doesn't quite match the official specs
pub fn lz4_jblock_decompress(
    buf_compressed: &[u8],
    buf_decompressed: &mut [u8],
) -> io::Result<usize> {
    let mut in_pos = 0;
    let mut out_pos = 0;

    loop {
        let token = buf_compressed[in_pos];
        in_pos += 1;

        let (literal_len, extra_in_pos) =
            read_multibyte_number(token >> 4, &buf_compressed[in_pos..]);
        in_pos += extra_in_pos;

        let buf_decompressed_remaining = buf_decompressed.len() - out_pos;
        if buf_decompressed_remaining - literal_len < 8 {
            if buf_decompressed_remaining != literal_len {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "lz4 terminal literal run does not fill output buffer exactly",
                ));
            }

            //TODO do fast copy of first (literal_len & 0xFFFFFFF8) bytes,
            // up until last (literal_len & 0x7) bytes
            buf_decompressed[out_pos..].copy_from_slice(&buf_compressed[in_pos..]);
            return Ok(buf_decompressed.len());
        }

        //TODO do fast copy of ((literal_len & 0xFFFFFFF8) + 8) bytes, since we know it's safe
        buf_decompressed[out_pos..out_pos + literal_len]
            .copy_from_slice(&buf_compressed[in_pos..in_pos + literal_len]);
        in_pos += literal_len;
        out_pos += literal_len;

        let ref_offset =
            (buf_compressed[in_pos] as usize) | ((buf_compressed[in_pos + 1] as usize) << 8);
        in_pos += 2;
        if ref_offset == 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "lz4 match offset should not be zero",
            ));
        }
        let ref_pos = out_pos - ref_offset;

        let (base_run_len, extra_in_pos) =
            read_multibyte_number(token & 0x0F, &buf_compressed[in_pos..]);
        in_pos += extra_in_pos;
        let run_len = base_run_len + 4;

        //TODO do some fast copying in the following blocks as well
        if ref_pos + run_len <= out_pos {
            // non-overlapping
            let (src, dst) = buf_decompressed.split_at_mut(out_pos);
            dst[..run_len].copy_from_slice(&src[ref_pos..ref_pos + run_len]);
            out_pos += run_len;
        } else {
            // overlapping
            let mut ref_pos = ref_pos;
            for _ in 0..run_len {
                buf_decompressed[out_pos] = buf_decompressed[ref_pos];
                out_pos += 1;
                ref_pos += 1;
            }
        }
    }
}

fn read_multibyte_number(base: u8, buf: &[u8]) -> (usize, usize) {
    assert!(base <= 0x0F);
    let mut in_pos = 0;
    let n = match base {
        15 => {
            let mut acc: usize = 15;
            loop {
                let v = buf[in_pos] as usize;
                in_pos += 1;
                acc += v;
                if v != 0xFF {
                    break;
                }
            }
            acc
        }
        n => n as usize,
    };
    (n, in_pos)
}
