use chrono_tz::America::New_York;

use chopper::chopper::driver::ChopperDriver;
use chopper::chopper::error::ChopperResult;
use chopper::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper::chopper::sink::{DataSink, DynHeaderSink};
use chopper::chopper::types::{self, Header, Row};
use chopper::driver::driver::Driver;
use chopper::input::input_factory::InputFactoryBuilder;
use chopper::source::csv_input_config::CSVInputConfig;
use chopper::source::csv_timestamp_config::{
    TimestampColConfig, TimestampConfig, TimestampFmtConfig,
};
use chopper::source::source::Source;
use chopper::util::file::are_contents_same;
use chopper::util::tz::ChopperTz;
use chopper::write::factory::OutputFactory;

#[test]
fn test_multi_row_output() {
    test().unwrap();
    assert!(are_contents_same(
        "./tests/output/test_multi_row_output.csv",
        "./tests/reference/test_multi_row_output.csv",
    )
    .unwrap());
}

fn test() -> ChopperResult<()> {
    setup_graph()?.drive()
}

fn setup_graph() -> ChopperResult<Box<dyn ChopperDriver>> {
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
    let mut input_factory = InputFactoryBuilder::new()
        .with_csv_input_config(csv_input_config)
        .build()?;
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    let node_filter = HeaderNode::HeaderSink(Box::new(MultiRowFilter {}));

    let header_sink = OutputFactory::new().new_header_sink(Some(output))?;
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

impl DynHeaderSink for MultiRowFilter {
    fn process_header(self: Box<Self>, _header: &mut Header) -> ChopperResult<Box<dyn DataSink>> {
        Ok(self)
    }
}

impl DataSink for MultiRowFilter {
    fn write_row(&mut self, io_rows: &mut Vec<Row>) -> ChopperResult<()> {
        let row = io_rows.get(0).unwrap().clone();
        io_rows.push(row.clone());
        io_rows.push(row);
        Ok(())
    }
}
