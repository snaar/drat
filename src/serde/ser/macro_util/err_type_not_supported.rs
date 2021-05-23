macro_rules! err_type_not_supported {
    ($($func:ident)*) => {
        $(err_type_not_supported_helper!{$func})*
    };
}

#[doc(hidden)]
macro_rules! err_type_not_supported_method {
    ($func:ident[$msg:expr, $ok:ident]($($arg:ident : $ty:ty),*)) => {
        #[inline]
        fn $func(self, $($arg: $ty),*) -> Result<Self::$ok, Self::Error> {
            $(
                let _ = $arg;
            )*
            Err(SerError::type_not_supported($msg))
        }
    };
}

#[doc(hidden)]
macro_rules! err_type_not_supported_method_with_generics {
    ($func:ident[$msg:expr, $ok:ident]($($arg:ident : $ty:ty),*)) => {
        #[inline]
        fn $func<T: Serialize + ?Sized>(self, $($arg: $ty),*) -> Result<Self::$ok, Self::Error> {
            $(
                let _ = $arg;
            )*
            Err(SerError::type_not_supported($msg))
        }
    };
}

#[doc(hidden)]
macro_rules! err_type_not_supported_helper {
    (bool) => {
        err_type_not_supported_method!{serialize_bool["bool", Ok](v: bool)}
    };
    (i8) => {
        err_type_not_supported_method!{serialize_i8["i8", Ok](v: i8)}
    };
    (i16) => {
        err_type_not_supported_method!{serialize_i16["i16", Ok](v: i16)}
    };
    (i32) => {
        err_type_not_supported_method!{serialize_i32["i32", Ok](v: i32)}
    };
    (i64) => {
        err_type_not_supported_method!{serialize_i64["i64", Ok](v: i64)}
    };
    (i128) => {
        serde_if_integer128! {
            err_type_not_supported_method!{serialize_i128["i128", Ok](v: i128)}
        }
    };
    (u8) => {
        err_type_not_supported_method!{serialize_u8["u8", Ok](v: u8)}
    };
    (u16) => {
        err_type_not_supported_method!{serialize_u16["u16", Ok](v: u16)}
    };
    (u32) => {
        err_type_not_supported_method!{serialize_u32["u32", Ok](v: u32)}
    };
    (u64) => {
        err_type_not_supported_method!{serialize_u64["u64", Ok](v: u64)}
    };
    (u128) => {
        serde_if_integer128! {
            err_type_not_supported_method!{serialize_u128["u128", Ok](v: u128)}
        }
    };
    (f32) => {
        err_type_not_supported_method!{serialize_f32["f32", Ok](v: f32)}
    };
    (f64) => {
        err_type_not_supported_method!{serialize_f64["f64", Ok](v: f64)}
    };
    (char) => {
        err_type_not_supported_method!{serialize_char["char", Ok](v: char)}
    };
    (str) => {
        err_type_not_supported_method!{serialize_str["&str", Ok](v: &str)}
    };
    (bytes) => {
        err_type_not_supported_method!{serialize_bytes["&[u8]", Ok](v: &[u8])}
    };
    (none) => {
        err_type_not_supported_method!{serialize_none["none", Ok]()}
    };
    (some) => {
        err_type_not_supported_method_with_generics!{serialize_some["some", Ok](value: &T)}
    };
    (unit) => {
        err_type_not_supported_method!{serialize_unit["unit", Ok]()}
    };
    (unit_struct) => {
        err_type_not_supported_method!{serialize_unit_struct["unit struct", Ok](name: &'static str)}
    };
    (unit_variant) => {
        err_type_not_supported_method!{serialize_unit_variant["unit variant", Ok](name: &'static str, variant_index: u32, variant: &'static str)}
    };
    (newtype_struct) => {
        err_type_not_supported_method_with_generics!{serialize_newtype_struct["newtype struct", Ok](name: &'static str, value: &T)}
    };
    (newtype_variant) => {
        err_type_not_supported_method_with_generics!{serialize_newtype_variant["newtype variant", Ok](name: &'static str, variant_index: u32, variant: &'static str, value: &T)}
    };
    (seq) => {
        err_type_not_supported_method!{serialize_seq["seq", SerializeSeq](len: Option<usize>)}
    };
    (tuple) => {
        err_type_not_supported_method!{serialize_tuple["tuple", SerializeTuple](len: usize)}
    };
    (tuple_struct) => {
        err_type_not_supported_method!{serialize_tuple_struct["tuple struct", SerializeTupleStruct](name: &'static str, len: usize)}
    };
    (tuple_variant) => {
        err_type_not_supported_method!{serialize_tuple_variant["tuple variant", SerializeTupleVariant](name: &'static str, variant_index: u32, variant: &'static str, len: usize)}
    };
    (map) => {
        err_type_not_supported_method!{serialize_map["map", SerializeMap](len: Option<usize>)}
    };
    (struct) => {
        err_type_not_supported_method!{serialize_struct["struct", SerializeStruct](name: &'static str, len: usize)}
    };
    (struct_variant) => {
        err_type_not_supported_method!{serialize_struct_variant["struct variant", SerializeStructVariant](name: &'static str, variant_index: u32, variant: &'static str, len: usize)}
    };
}
