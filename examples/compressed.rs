use chopper_lib::chopper::chopper::{ChDriver, Source};
use chopper_lib::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper_lib::chopper::types::{self, Header};
use chopper_lib::driver::driver::Driver;
use chopper_lib::error::{self, CliResult};
use chopper_lib::input::input_factory::InputFactory;
use chopper_lib::source::csv_configs::{CSVInputConfig, CSVOutputConfig, DELIMITER_DEFAULT};
use chopper_lib::write::factory;

fn main() {
    error::handle_drive_error(compressed_example());
}

fn compressed_example() -> CliResult<()> {
    setup_graph()?.drive()
}

fn setup_graph() -> CliResult<Box<ChDriver>> {
    let csv_config = CSVInputConfig::new(",", true, 0)?;
    let input = "./examples/files/uspop_time.csv.gz";
    let inputs = vec![input];
    let output = None;

    let mut input_factory
        = InputFactory::new(Some(csv_config), None, None)?;
    let mut sources: Vec<Box<Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source
            = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    let csv_output_config = CSVOutputConfig::new(DELIMITER_DEFAULT, false);
    let header_sink = factory::new_header_sink(output, Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain = HeaderChain::new(vec![node_output]);

    let graph = HeaderGraph::new(vec![chain]);
    Ok(Box::new(
        Driver::new(sources, graph, types::DATA_RANGE_DEFAULT, headers)?))
}
