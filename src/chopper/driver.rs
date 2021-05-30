use crate::chopper::error::ChopperResult;

pub trait ChopperDriver {
    fn drive(&mut self) -> ChopperResult<()>;
}
