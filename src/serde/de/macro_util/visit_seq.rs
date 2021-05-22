// based on serde::forward_to_deserialize_any

macro_rules! visit_seq {
    (<$visitor:ident: Visitor<$lifetime:tt>> $($func:ident)*) => {
        $(visit_seq_helper!{$func<$lifetime, $visitor>})*
    };
    // This case must be after the previous one.
    ($($func:ident)*) => {
        $(visit_seq_helper!{$func<'de, V>})*
    };
}

#[doc(hidden)]
macro_rules! visit_seq_method {
    ($func:ident<$l:tt, $v:ident>($($arg:ident : $ty:ty),*)) => {
        #[inline]
        fn $func<$v>(self, $($arg: $ty,)* visitor: $v) -> Result<$v::Value, Self::Error>
        where
            $v: serde::de::Visitor<$l>,
        {
            $(
                let _ = $arg;
            )*
            visitor.visit_seq(self)
        }
    };
}

#[doc(hidden)]
macro_rules! visit_seq_helper {
    (any<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_any<$l, $v>()}
    };
    (bool<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_bool<$l, $v>()}
    };
    (i8<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_i8<$l, $v>()}
    };
    (i16<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_i16<$l, $v>()}
    };
    (i32<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_i32<$l, $v>()}
    };
    (i64<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_i64<$l, $v>()}
    };
    (i128<$l:tt, $v:ident>) => {
        serde_if_integer128! {
            visit_seq_method!{deserialize_i128<$l, $v>()}
        }
    };
    (u8<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_u8<$l, $v>()}
    };
    (u16<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_u16<$l, $v>()}
    };
    (u32<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_u32<$l, $v>()}
    };
    (u64<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_u64<$l, $v>()}
    };
    (u128<$l:tt, $v:ident>) => {
        serde_if_integer128! {
            visit_seq_method!{deserialize_u128<$l, $v>()}
        }
    };
    (f32<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_f32<$l, $v>()}
    };
    (f64<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_f64<$l, $v>()}
    };
    (char<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_char<$l, $v>()}
    };
    (str<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_str<$l, $v>()}
    };
    (string<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_string<$l, $v>()}
    };
    (bytes<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_bytes<$l, $v>()}
    };
    (byte_buf<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_byte_buf<$l, $v>()}
    };
    (option<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_option<$l, $v>()}
    };
    (unit<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_unit<$l, $v>()}
    };
    (unit_struct<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_unit_struct<$l, $v>(name: &'static str)}
    };
    (newtype_struct<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_newtype_struct<$l, $v>(name: &'static str)}
    };
    (seq<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_seq<$l, $v>()}
    };
    (tuple<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_tuple<$l, $v>(len: usize)}
    };
    (tuple_struct<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_tuple_struct<$l, $v>(name: &'static str, len: usize)}
    };
    (map<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_map<$l, $v>()}
    };
    (struct<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_struct<$l, $v>(name: &'static str, fields: &'static [&'static str])}
    };
    (enum<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_enum<$l, $v>(name: &'static str, variants: &'static [&'static str])}
    };
    (identifier<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_identifier<$l, $v>()}
    };
    (ignored_any<$l:tt, $v:ident>) => {
        visit_seq_method!{deserialize_ignored_any<$l, $v>()}
    };
}
