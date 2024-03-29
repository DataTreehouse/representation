use crate::{RepresentationError, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use log::warn;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{Literal, NamedNodeRef, Term};
use polars_core::datatypes::TimeUnit;
use polars_core::prelude::{AnyValue, DataType, Field};
use std::str::FromStr;
use polars::prelude::SearchSortedSide::Any;

//This code is copied and modified from Chrontext, which has identical licensing
pub fn sparql_literal_to_any_value<'a>(
    value: &str,
    language: Option<&str>,
    datatype: &Option<NamedNodeRef<'a>>,
) -> (AnyValue<'static>, NamedNodeRef<'a>) {
    let (anyv, dt) = if let Some(datatype) = datatype {
        let datatype = *datatype;
        let literal_value = if datatype == xsd::STRING {
            AnyValue::StringOwned(value.into())
        } else if datatype == xsd::ANY_URI {
            AnyValue::StringOwned(value.into())
        } else if datatype == xsd::UNSIGNED_INT {
            let u = u32::from_str(value).expect("Integer parsing error");
            AnyValue::from(u)
        } else if datatype == xsd::UNSIGNED_BYTE {
            let u = u8::from_str(value).expect("Integer parsing error");
            AnyValue::from(u)
        } else if datatype == xsd::UNSIGNED_SHORT {
            let u = u16::from_str(value).expect("Integer parsing error");
            AnyValue::from(u)
        } else if datatype == xsd::UNSIGNED_LONG || datatype == xsd::NON_NEGATIVE_INTEGER {
            let u = u64::from_str(value).expect("Integer parsing error");
            AnyValue::from(u)
        } else if datatype == xsd::BYTE {
            let i = i8::from_str(value).expect("Integer parsing error");
            AnyValue::from(i)
        } else if datatype == xsd::SHORT {
            let i = i16::from_str(value).expect("Integer parsing error");
            AnyValue::from(i)
        } else if datatype == xsd::INTEGER || datatype == xsd::LONG {
            let i = i64::from_str(value).expect("Integer parsing error");
            AnyValue::from(i)
        } else if datatype == xsd::INT {
            let i = i32::from_str(value).expect("Integer parsing error");
            AnyValue::from(i)
        } else if datatype == xsd::DOUBLE || datatype == xsd::DECIMAL {
            let d = f64::from_str(value).expect("Integer parsing error");
            AnyValue::from(d)
        } else if datatype == xsd::FLOAT {
            let f = f32::from_str(value).expect("Integer parsing error");
            AnyValue::from(f)
        } else if datatype == xsd::BOOLEAN {
            let b = bool::from_str(value).expect("Boolean parsing error");
            AnyValue::Boolean(b)
        } else if datatype == xsd::DATE_TIME {
            let dt_without_tz = value.parse::<NaiveDateTime>();
            if let Ok(dt) = dt_without_tz {
                AnyValue::Datetime(
                    dt.timestamp_nanos_opt().unwrap(),
                    TimeUnit::Nanoseconds,
                    &None,
                )
            } else {
                let dt_without_tz = value.parse::<DateTime<Utc>>();
                if let Ok(dt) = dt_without_tz {
                    AnyValue::Datetime(
                        dt.naive_utc().timestamp_nanos_opt().unwrap(),
                        TimeUnit::Nanoseconds,
                        &None,
                    )
                } else {
                    warn!("Could not parse datetime: {}", value);
                    AnyValue::Null
                }
            }
        } else if datatype == xsd::DATE {
            let parsed = NaiveDate::parse_from_str(value, "%Y-%m-%d").unwrap();
            let dur = parsed.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            AnyValue::Date(dur.num_days() as i32)
        } else if datatype == rdf::LANG_STRING {
            //Not using StringOwned here causes corruption..
            let val = AnyValue::StringOwned(value.into());
            let lang = AnyValue::StringOwned(language.unwrap().into());
            let polars_fields: Vec<Field> = vec![
                Field::new(LANG_STRING_VALUE_FIELD, DataType::String),
                Field::new(LANG_STRING_LANG_FIELD, DataType::String),
            ];
            let av = AnyValue::StructOwned(Box::new((vec![val, lang], polars_fields)));
            av
        } else {
            AnyValue::StringOwned(value.into())
        };
        (literal_value, datatype)
    } else {
        (AnyValue::StringOwned(value.into()), xsd::STRING)
    };
    (anyv, dt)
}

pub fn parse_literal_as_primitive<T: std::str::FromStr>(
    l: Literal,
) -> Result<T, RepresentationError> {
    let parsed = l.value().parse().map_err(|_x| {
        RepresentationError::InvalidLiteralError(format!("Could not parse as literal {}", l))
    })?;
    Ok(parsed)
}

pub fn parse_term_as_primitive<T: std::str::FromStr>(term: Term) -> Result<T, RepresentationError> {
    match term {
        Term::Literal(l) => parse_literal_as_primitive(l),
        _ => Err(RepresentationError::InvalidLiteralError(format!(
            "Wrong term type when trying to parse literal {}",
            term
        ))),
    }
}
