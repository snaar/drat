use crate::args::Args;
use crate::file_record::FileRecord;
use crate::read_filter::ReadFilter;
use crate::result::CliResult;
use crate::write::{csv_sink, sink::Sink};

pub fn collate(mut argv: Args) -> CliResult<()> {
    let mut writer = csv_sink::CSVSink::new(&argv.output);
    let configs = argv.create_configs()?;

    // used to filter timestamps
    let read_filter;
    if argv.begin.is_some() || argv.end.is_some() {
        read_filter = Some(ReadFilter::new_from_args(&argv));
    } else {
        read_filter = None;
    }

    // creates file record for each file and add to vector
    let mut file_records = Vec::with_capacity(configs.len());
    for c in configs {
        file_records.push(FileRecord::new(c, argv.timestamp_column));
    }

    // sort, merge, and output
    if argv.has_headers && file_records.len() > 0 {
        let header = file_records[0].get_header();
        match header.len() {
            0 => println!("No header found."),
            _ => {
                writer.write_header(header);
            }
        };
    }

    let mut record_len = file_records.len();
    while record_len > 0 {
        let index = get_min_index(&file_records);
        let row = file_records[index].get_current_row().clone().unwrap();
        writer.write_row(&row);

        loop {
            if !file_records[index].next(&read_filter) {
                file_records.remove(index);
            }
            break;
        }
        record_len = file_records.len();
    }
    Ok(())
}

fn get_min_index(file_records: &Vec<FileRecord>) -> usize {
    let min =
        file_records.iter()
                    .enumerate()
                    .min_by(|&(_, i1), &(_, i2)|
                            i1.get_timestamp().cmp(&i2.get_timestamp())).unwrap();
    min.0
}

pub fn run(argv: Args) -> CliResult<()> {
    collate(argv)
}
