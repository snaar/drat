use chrono_tz::America::New_York;

use chopper::chopper::chopper::{ChopperDriver, DataSink, HeaderSink};
use chopper::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper::chopper::types::{self, Header, Row};
use chopper::driver::driver::Driver;
use chopper::error::CliResult;
use chopper::input::input_factory::InputFactory;
use chopper::source::csv_configs::{CSVOutputConfig, TimestampFmtConfig};
use chopper::source::csv_configs::{TimestampColConfig, TimestampConfig};
use chopper::source::csv_input_config::CSVInputConfig;
use chopper::source::source::Source;
use chopper::util::file::are_contents_same;
use chopper::util::tz::ChopperTz;
use chopper::write::factory;

#[test]
fn test_multi_row_output() {
    test().unwrap();
    assert!(are_contents_same(
        "./tests/output/test_multi_row_output.csv",
        "./tests/reference/test_multi_row_output.csv",
    )
    .unwrap());
}

fn test() -> CliResult<()> {
    setup_graph()?.drive()
}

fn setup_graph() -> CliResult<Box<dyn ChopperDriver>> {
    let input = "./tests/input/time_city.csv";
    let inputs = vec![input];
    let output = "./tests/output/test_multi_row_output.csv";

    // source reader and headers
    let ts_config = TimestampConfig::new(
        TimestampColConfig::Name("DateTime".to_owned()),
        TimestampFmtConfig::Auto,
        ChopperTz::from(New_York),
    );
    let csv_input_config = CSVInputConfig::new(ts_config);
    let mut input_factory = InputFactory::new(csv_input_config, None, None)?;
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    let node_filter = HeaderNode::HeaderSink(Box::new(MultiRowFilter {}));

    let csv_output_config = CSVOutputConfig::new_default();
    let header_sink = factory::new_header_sink(Some(output), Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);

    let chain = HeaderChain::new(vec![node_filter, node_output]);
    let graph = HeaderGraph::new(vec![chain]);

    Ok(Box::new(Driver::new(
        sources,
        graph,
        types::TIMESTAMP_RANGE_ALL,
        headers,
    )?))
}

struct MultiRowFilter {}

impl HeaderSink for MultiRowFilter {
    fn process_header(self: Box<Self>, _header: &mut Header) -> CliResult<Box<dyn DataSink>> {
        Ok(self)
    }
}

impl DataSink for MultiRowFilter {
    fn write_row(&mut self, io_rows: &mut Vec<Row>) -> CliResult<()> {
        let row = io_rows.get(0).unwrap().clone();
        io_rows.push(row.clone());
        io_rows.push(row);
        Ok(())
    }

    fn boxed(self) -> Box<dyn DataSink> {
        Box::new(self)
    }
}
