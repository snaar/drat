use args::Args;
use config::Config;
use file_record::FileRecord;
use read_filter::ReadFilter;
use result::CliResult;

pub fn collate(argv: Args) -> CliResult<()> {
    let mut writer = Config::new(&argv.output, argv.delimiter, argv.has_headers).writer()?;
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
        match header {
            None => println!("No header found."),
            Some(h) => {
                writer.write_record(h)?;
            }
        };
    }

    while file_records.len() > 0 {
        let mut min: (usize, u64) = (0, 0 as u64); // (index, row)
        let mut i = 0;
        for record in &file_records {
            if i == 0 || (record.get_timestamp() < min.1) {
                min.0 = i;
                min.1 = record.get_timestamp();
            }
            i += 1;
        }
        let index = min.0;
        writer.write_record(file_records[index].get_current_row().iter())?;
        if !file_records[index].next(&read_filter) {
            file_records.remove(index);
        }
    }
    writer.flush()?;
    Ok(())
}

pub fn run(argv: Args) -> CliResult<()> {
    collate(argv)
}
