use crate::args::Args;
use crate::dr::dr;
use crate::process::driver::single_input_driver::SingleInputDriver;
use crate::process::driver::file_record::FileRecord;
use crate::result::CliResult;
use crate::write;

pub fn collate(mut argv: Args) -> CliResult<()> {
    let configs = argv.create_configs()?;

    // used to filter timestamps
    let input_driver;
    if argv.begin.is_some() || argv.end.is_some() {
        input_driver = Some(SingleInputDriver::new_from_args(&argv));
    } else {
        input_driver = None;
    }

    // creates file record for each file and add to vector
    let mut file_records = Vec::with_capacity(configs.len());
    for c in configs {
        file_records.push(FileRecord::new(c));
    }

    // sort, merge, and output
    let header = file_records[0].get_header();
    let mut writer: Box<dr::Sink+'static> = write::factory::new_sink(&argv.output, header, &argv.csv_config);

    let mut record_len = file_records.len();
    while record_len > 0 {
        let index = get_min_index(&file_records);
        let row = file_records[index].get_current_row().clone().unwrap();
        writer.write_row(&row);

        loop {
            if !file_records[index].next(&input_driver) {
                file_records.remove(index);
            }
            break;
        }
        record_len = file_records.len();
    }
    Ok(())
}

fn get_min_index(file_records: &Vec<FileRecord>) -> usize {
    let min = file_records
        .iter()
        .enumerate()
        .min_by(|&(_, i1), &(_, i2)|
            i1.get_timestamp().cmp(&i2.get_timestamp())).unwrap();
    min.0
}

pub fn run(argv: Args) -> CliResult<()> {
    collate(argv)
}
