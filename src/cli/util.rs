use clap::arg_enum;

arg_enum! {
    #[derive(Clone, Copy, Debug)]
    pub enum YesNoAuto {
        Yes,
        No,
        Auto,
    }
}
