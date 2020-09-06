#[derive(Clone, Debug)]
pub enum InputType {
    Path(String),
    StdIn,
}

#[derive(Clone, Debug)]
pub enum InputFormat {
    Extension(String),
    Auto,
}

#[derive(Clone, Debug)]
pub struct Input {
    pub input: InputType,
    pub format: InputFormat,
}
