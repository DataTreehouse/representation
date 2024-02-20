use std::collections::HashMap;
use crate::{literal_iri_to_namednode, RDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD, literal_blanknode_to_blanknode};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{Literal, NamedNodeRef, Variable};
use polars::export::rayon::iter::{ParallelIterator};
use polars_core::frame::DataFrame;
use polars_core::prelude::{DataType};
use spargebra::term::Term;
use crate::multitype::{MULTI_BLANK_DT, MULTI_IRI_DT};

//From sparesults, need public fields.
#[derive(Debug)]
pub struct QuerySolutions {
    pub variables: Vec<Variable>,
    pub solutions: Vec<Vec<Option<Term>>>,
}
pub fn df_as_result(df: DataFrame, dtypes: &HashMap<String, RDFNodeType>) -> QuerySolutions {
    if df.height() == 0 {
        let variables = dtypes.keys().map(|x| Variable::new(x).unwrap()).collect();
        return QuerySolutions {
            variables,
            solutions: vec![],
        };
    }
    let mut all_terms = vec![];
    let mut variables = vec![];
    for (k, v) in dtypes {
        if let Ok(ser) = df.column(k) { //TODO: Perhaps correct this upstream?
            variables.push(Variable::new_unchecked(k));
            let terms: Vec<_> = match v {
                RDFNodeType::IRI => ser
                    .cast(&DataType::Utf8)
                    .unwrap()
                    .utf8()
                    .unwrap()
                    .par_iter()
                    .map(|x| x.map(|x| Term::NamedNode(literal_iri_to_namednode(x))))
                    .collect(),
                RDFNodeType::BlankNode => ser
                    .cast(&DataType::Utf8)
                    .unwrap()
                    .utf8()
                    .unwrap()
                    .par_iter()
                    .map(|x| x.map(|x| Term::BlankNode(literal_blanknode_to_blanknode(x))))
                    .collect(),
                RDFNodeType::Literal(l) => match l.as_ref() {
                    rdf::LANG_STRING => {
                        let value_ser = ser
                            .struct_()
                            .unwrap()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                            .unwrap();
                        let value_iter = value_ser.utf8().unwrap().into_iter();
                        let lang_ser = ser
                            .struct_()
                            .unwrap()
                            .field_by_name(LANG_STRING_LANG_FIELD)
                            .unwrap();
                        let lang_iter = lang_ser.utf8().unwrap().into_iter();
                        value_iter
                            .zip(lang_iter)
                            .map(|(value, lang)| {
                                if let Some(value) = value {
                                    if let Some(lang) = lang {
                                        Some(Term::Literal(
                                            Literal::new_language_tagged_literal_unchecked(value, lang),
                                        ))
                                    } else {
                                        panic!()
                                    }
                                } else {
                                    None
                                }
                            })
                            .collect()
                    }
                    xsd::STRING => ser
                        .cast(&DataType::Utf8)
                        .unwrap()
                        .utf8()
                        .unwrap()
                        .par_iter()
                        .map(|x| x.map(|x| Term::Literal(Literal::new_simple_literal(x))))
                        .collect(),
                    dt => ser
                        .cast(&DataType::Utf8)
                        .unwrap()
                        .utf8()
                        .unwrap()
                        .par_iter()
                        .map(|x| {
                            x.map(|x| Term::Literal(Literal::new_typed_literal(x, dt.into_owned())))
                        })
                        .collect(),
                },
                RDFNodeType::None => {
                    panic!()
                }
                RDFNodeType::MultiType => {
                    let value_ser = ser
                        .struct_()
                        .unwrap()
                        .field_by_name(MULTI_VALUE_COL)
                        .unwrap()
                        .cast(&DataType::Utf8)
                        .unwrap();
                    let value_vec: Vec<_> = value_ser.utf8().unwrap().par_iter().collect();
                    let lang_ser = ser
                        .struct_()
                        .unwrap()
                        .field_by_name(MULTI_LANG_COL)
                        .unwrap()
                        .cast(&DataType::Utf8)
                        .unwrap();
                    let lang_vec: Vec<_> = lang_ser.utf8().unwrap().par_iter().collect();
                    let dt_ser = ser
                        .struct_()
                        .unwrap()
                        .field_by_name(MULTI_DT_COL)
                        .unwrap()
                        .cast(&DataType::Utf8)
                        .unwrap();
                    let dt_vec: Vec<_> = dt_ser.utf8().unwrap().par_iter().collect();
                    (0..df.height())
                        .map(|i| {
                            let value = value_vec.get(i).unwrap();
                            let lang = lang_vec.get(i).unwrap();
                            let dt = dt_vec.get(i).unwrap();
                            if let Some(value) = *value {
                                if let Some(lang) = *lang {
                                    Some(Term::Literal(
                                        Literal::new_language_tagged_literal_unchecked(value, lang),
                                    ))
                                } else if let Some(dt) = *dt {
                                    match dt {
                                        MULTI_IRI_DT => {
                                            Some(Term::NamedNode(literal_iri_to_namednode(value)))
                                        }
                                        MULTI_BLANK_DT => {
                                            Some(Term::BlankNode(literal_blanknode_to_blanknode(value)))
                                        }
                                        _ => Some(Term::Literal(Literal::new_typed_literal(
                                            value,
                                            literal_iri_to_namednode(dt),
                                        ))),
                                    }
                                } else {
                                    panic!()
                                }
                            } else {
                                None
                            }
                        })
                        .collect()
                }
            };
            all_terms.push(terms);
        }
    }
    let mut solns = vec![];
    for _i in 0..df.height() {
        let mut soln = vec![];
        for tl in &mut all_terms {
            soln.push(tl.pop().unwrap());
        }
        solns.push(soln);
    }
    solns.reverse();
    QuerySolutions {
        variables,
        solutions: solns,
    }
}


pub fn primitive_polars_type_to_literal_type(data_type: &DataType) -> Option<NamedNodeRef> {
    match data_type {
        DataType::Boolean => Some(xsd::BOOLEAN),
        DataType::Int8 => Some(xsd::BYTE),
        DataType::Int16 => Some(xsd::SHORT),
        DataType::UInt8 => Some(xsd::UNSIGNED_BYTE),
        DataType::UInt16 => Some(xsd::UNSIGNED_SHORT),
        DataType::UInt32 => Some(xsd::UNSIGNED_INT),
        DataType::UInt64 => Some(xsd::UNSIGNED_LONG),
        DataType::Int32 => Some(xsd::INT),
        DataType::Int64 => Some(xsd::LONG),
        DataType::Float32 => Some(xsd::FLOAT),
        DataType::Float64 => Some(xsd::DOUBLE),
        DataType::Utf8 => Some(xsd::STRING),
        DataType::Date => Some(xsd::DATE),
        DataType::Datetime(_, Some(_)) => Some(xsd::DATE_TIME_STAMP),
        DataType::Datetime(_, None) => Some(xsd::DATE_TIME),
        DataType::Duration(_) => Some(xsd::DURATION),
        DataType::Categorical(_) => Some(xsd::STRING),
        _ => {None}
    }
}