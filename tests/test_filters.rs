use chrono_tz::America::New_York;
use same_file::is_same_file;

use chopper_lib::chopper::chopper::{ChopperDriver, Source};
use chopper_lib::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper_lib::chopper::types::{self, FieldValue, Header};
use chopper_lib::driver::driver::Driver;
use chopper_lib::error::{self, CliResult};
use chopper_lib::filter::column_filter_delete_col::ColumnFilterDelete;
use chopper_lib::filter::row_filter_equal_value::RowFilterEqualValue;
use chopper_lib::filter::row_filter_greater_value::RowFilterGreaterValue;
use chopper_lib::input::input_factory::InputFactory;
use chopper_lib::source::csv_configs::{self, CSVInputConfig, CSVOutputConfig, DELIMITER_DEFAULT};
use chopper_lib::write::factory;

#[test]
fn test_filters() {
    error::handle_drive_error(filter());
    assert!(is_same_file
        ("./tests/output/test_filters.csv",
         "./tests/reference/filters.csv"
        ).unwrap());
}

fn filter() -> CliResult<()> {
    setup_graph()?.drive()
}

fn setup_graph() -> CliResult<Box<dyn ChopperDriver>> {
    let input = "./tests/input/time_city.csv";
    let inputs = vec![input];
    let output = "./tests/output/test_filters.csv";

    // source reader and headers
    let input_config = CSVInputConfig::new
        (csv_configs::DELIMITER_DEFAULT,
         true,
         0,
         None,
         None,
         None,
         New_York
        )?;
    let mut input_factory
        = InputFactory::new(Some(input_config), None, None)?;
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    // row filter equal
    let filter_equal
        = RowFilterEqualValue::new("State", FieldValue::String("CA".to_string()));
    let node_1 = HeaderNode::HeaderSink(filter_equal);

    // row filter greater
    let filter_greater
        = RowFilterGreaterValue::new("Year", FieldValue::String("2017".to_string()));
    let node_2 = HeaderNode::HeaderSink(filter_greater);

    // col filter delete
    let filter_delete = ColumnFilterDelete::new("Time");
    let node_3 = HeaderNode::HeaderSink(filter_delete);

    // header sink
    let csv_output_config = CSVOutputConfig::new(DELIMITER_DEFAULT, true);
    let header_sink = factory::new_header_sink
        (Some(output.to_string()), Some(csv_output_config))?;

    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain = HeaderChain::new(vec![node_1, node_2, node_3, node_output]);

    let graph = HeaderGraph::new(vec![chain]);

    Ok(Box::new(
        Driver::new(sources, graph, types::TIMESTAMP_RANGE_DEFAULT, headers)?))
}
