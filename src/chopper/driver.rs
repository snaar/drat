use crate::error::CliResult;

pub trait ChopperDriver {
    fn drive(&mut self) -> CliResult<()>;
}
