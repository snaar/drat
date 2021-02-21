use chrono_tz::America::New_York;

use chopper::chopper::chopper::{ChopperDriver, Source};
use chopper::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper::chopper::types::{FieldValue, Header, TimestampRange};
use chopper::cli::util::YesNoAuto;
use chopper::driver::driver::Driver;
use chopper::error::CliResult;
use chopper::filter::column_filter_delete_col::ColumnFilterDelete;
use chopper::filter::row_filter_equal_value::RowFilterEqualValue;
use chopper::filter::row_filter_greater_value::RowFilterGreaterValue;
use chopper::input::input_factory::InputFactory;
use chopper::source::csv_configs::{CSVInputConfig, CSVOutputConfig, TimestampFmtConfig};
use chopper::source::csv_configs::{TimestampColConfig, TimestampConfig};
use chopper::util::file::are_contents_same;
use chopper::util::{timestamp_util, tz::ChopperTz};
use chopper::write::factory;

#[test]
fn test_filters() {
    test().unwrap();
    assert!(are_contents_same(
        "./tests/output/test_filters.csv",
        "./tests/reference/test_filters.csv"
    )
    .unwrap());
}

fn test() -> CliResult<()> {
    setup_graph()?.drive()
}

fn setup_graph() -> CliResult<Box<dyn ChopperDriver>> {
    let input = "./tests/input/time_city.csv";
    let inputs = vec![input];
    let output = "./tests/output/test_filters.csv";

    let begin = timestamp_util::parse_datetime_range_element("2018", &ChopperTz::from(New_York))?;
    let timestamp_range = TimestampRange {
        begin: Some(begin),
        end: None,
    };

    // source reader and headers
    let ts_config = TimestampConfig::new(
        TimestampColConfig::Name("DateTime".to_owned()),
        TimestampFmtConfig::Auto,
        ChopperTz::from(New_York),
    );
    let csv_input_config = CSVInputConfig::new(None, YesNoAuto::Auto, ts_config)?;
    let mut input_factory = InputFactory::new(csv_input_config, None, None)?;
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    // row filter equal
    let filter_equal =
        RowFilterEqualValue::new("String", FieldValue::String("New York".to_string()));
    let node_1 = HeaderNode::HeaderSink(filter_equal);

    // row filter greater
    let filter_greater =
        RowFilterGreaterValue::new("Double", FieldValue::String("10.0".to_string()));
    let node_2 = HeaderNode::HeaderSink(filter_greater);

    // col filter delete
    let filter_delete = ColumnFilterDelete::new("Char");
    let node_3 = HeaderNode::HeaderSink(filter_delete);

    // header sink
    let csv_output_config = CSVOutputConfig::new_default();
    let header_sink = factory::new_header_sink(Some(output), Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain = HeaderChain::new(vec![node_1, node_2, node_3, node_output]);

    let graph = HeaderGraph::new(vec![chain]);

    Ok(Box::new(Driver::new(
        sources,
        graph,
        timestamp_range,
        headers,
    )?))
}
