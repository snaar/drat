macro_rules! return_error {
    (<$err_func:ident> $($func:ident)*) => {
        $(return_error_helper!{$func $err_func})*
    };
}

#[doc(hidden)]
macro_rules! return_error_method {
    ($func:ident[$msg:expr, $ok:ident, $err_func:ident]($($arg:ident : $ty:ty),*)) => {
        #[inline]
        fn $func(self, $($arg: $ty),*) -> Result<Self::$ok, Self::Error> {
            $(
                let _ = $arg;
            )*
            Err(SerError::$err_func($msg))
        }
    };
}

#[doc(hidden)]
macro_rules! return_error_method_with_generics {
    ($func:ident[$msg:expr, $ok:ident, $err_func:ident]($($arg:ident : $ty:ty),*)) => {
        #[inline]
        fn $func<T: Serialize + ?Sized>(self, $($arg: $ty),*) -> Result<Self::$ok, Self::Error> {
            $(
                let _ = $arg;
            )*
            Err(SerError::$err_func($msg))
        }
    };
}

#[doc(hidden)]
macro_rules! return_error_helper {
    (bool $err_func:ident) => {
        return_error_method!{serialize_bool["bool", Ok, $err_func](v: bool)}
    };
    (i8 $err_func:ident) => {
        return_error_method!{serialize_i8["i8", Ok, $err_func](v: i8)}
    };
    (i16 $err_func:ident) => {
        return_error_method!{serialize_i16["i16", Ok, $err_func](v: i16)}
    };
    (i32 $err_func:ident) => {
        return_error_method!{serialize_i32["i32", Ok, $err_func](v: i32)}
    };
    (i64 $err_func:ident) => {
        return_error_method!{serialize_i64["i64", Ok, $err_func](v: i64)}
    };
    (i128 $err_func:ident) => {
        serde_if_integer128! {
            return_error_method!{serialize_i128["i128", Ok, $err_func](v: i128)}
        }
    };
    (u8 $err_func:ident) => {
        return_error_method!{serialize_u8["u8", Ok, $err_func](v: u8)}
    };
    (u16 $err_func:ident) => {
        return_error_method!{serialize_u16["u16", Ok, $err_func](v: u16)}
    };
    (u32 $err_func:ident) => {
        return_error_method!{serialize_u32["u32", Ok, $err_func](v: u32)}
    };
    (u64 $err_func:ident) => {
        return_error_method!{serialize_u64["u64", Ok, $err_func](v: u64)}
    };
    (u128 $err_func:ident) => {
        serde_if_integer128! {
            return_error_method!{serialize_u128["u128", Ok, $err_func](v: u128)}
        }
    };
    (f32 $err_func:ident) => {
        return_error_method!{serialize_f32["f32", Ok, $err_func](v: f32)}
    };
    (f64 $err_func:ident) => {
        return_error_method!{serialize_f64["f64", Ok, $err_func](v: f64)}
    };
    (char $err_func:ident) => {
        return_error_method!{serialize_char["char", Ok, $err_func](v: char)}
    };
    (str $err_func:ident) => {
        return_error_method!{serialize_str["&str", Ok, $err_func](v: &str)}
    };
    (bytes $err_func:ident) => {
        return_error_method!{serialize_bytes["&[u8]", Ok, $err_func](v: &[u8])}
    };
    (none $err_func:ident) => {
        return_error_method!{serialize_none["none", Ok, $err_func]()}
    };
    (some $err_func:ident) => {
        return_error_method_with_generics!{serialize_some["some", Ok, $err_func](value: &T)}
    };
    (unit $err_func:ident) => {
        return_error_method!{serialize_unit["unit", Ok, $err_func]()}
    };
    (unit_struct $err_func:ident) => {
        return_error_method!{serialize_unit_struct["unit struct", Ok, $err_func](name: &'static str)}
    };
    (unit_variant $err_func:ident) => {
        return_error_method!{serialize_unit_variant["unit variant", Ok, $err_func](name: &'static str, variant_index: u32, variant: &'static str)}
    };
    (newtype_struct $err_func:ident) => {
        return_error_method_with_generics!{serialize_newtype_struct["newtype struct", Ok, $err_func](name: &'static str, value: &T)}
    };
    (newtype_variant $err_func:ident) => {
        return_error_method_with_generics!{serialize_newtype_variant["newtype variant", Ok, $err_func](name: &'static str, variant_index: u32, variant: &'static str, value: &T)}
    };
    (seq $err_func:ident) => {
        return_error_method!{serialize_seq["seq", SerializeSeq, $err_func](len: Option<usize>)}
    };
    (tuple $err_func:ident) => {
        return_error_method!{serialize_tuple["tuple", SerializeTuple, $err_func](len: usize)}
    };
    (tuple_struct $err_func:ident) => {
        return_error_method!{serialize_tuple_struct["tuple struct", SerializeTupleStruct, $err_func](name: &'static str, len: usize)}
    };
    (tuple_variant $err_func:ident) => {
        return_error_method!{serialize_tuple_variant["tuple variant", SerializeTupleVariant, $err_func](name: &'static str, variant_index: u32, variant: &'static str, len: usize)}
    };
    (map $err_func:ident) => {
        return_error_method!{serialize_map["map", SerializeMap, $err_func](len: Option<usize>)}
    };
    (struct $err_func:ident) => {
        return_error_method!{serialize_struct["struct", SerializeStruct, $err_func](name: &'static str, len: usize)}
    };
    (struct_variant $err_func:ident) => {
        return_error_method!{serialize_struct_variant["struct variant", SerializeStructVariant, $err_func](name: &'static str, variant_index: u32, variant: &'static str, len: usize)}
    };
}
