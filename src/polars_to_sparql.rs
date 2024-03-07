use crate::multitype::all_multi_main_cols;
use crate::{
    literal_blanknode_to_blanknode, literal_iri_to_namednode, BaseRDFNodeType, RDFNodeType,
    LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD,
};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{Literal, NamedNodeRef, Variable};
use polars::export::rayon::iter::ParallelIterator;
use polars::prelude::{as_struct, col, IntoLazy};
use polars_core::frame::DataFrame;
use polars_core::prelude::{DataType, Series};
use spargebra::term::Term;
use std::collections::HashMap;
use std::vec::IntoIter;

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
    let height = df.height();
    for (k, v) in dtypes {
        if let Ok(ser) = df.column(k) {
            //TODO: Perhaps correct this upstream?
            variables.push(Variable::new_unchecked(k));
            let terms: Vec<_> = match v {
                RDFNodeType::None
                | RDFNodeType::IRI
                | RDFNodeType::BlankNode
                | RDFNodeType::Literal(..) => basic_rdf_node_type_series_to_term_vec(
                    ser,
                    &BaseRDFNodeType::from_rdf_node_type(v),
                ),
                RDFNodeType::MultiType(types) => {
                    let mut iters: Vec<IntoIter<Option<Term>>> = vec![];
                    for (t, colname) in types.iter().zip(all_multi_main_cols(types)) {
                        let v = if t.is_lang_string() {
                            let mut lf = DataFrame::new(vec![
                                ser.struct_()
                                    .unwrap()
                                    .field_by_name(LANG_STRING_VALUE_FIELD)
                                    .unwrap()
                                    .clone(),
                                ser.struct_()
                                    .unwrap()
                                    .field_by_name(LANG_STRING_LANG_FIELD)
                                    .unwrap()
                                    .clone(),
                            ])
                            .unwrap()
                            .lazy();
                            lf = lf.with_column(
                                as_struct(vec![
                                    col(LANG_STRING_LANG_FIELD),
                                    col(LANG_STRING_VALUE_FIELD),
                                ])
                                .alias(&colname),
                            );
                            let df = lf.collect();
                            let ser = df.unwrap().drop_in_place(&colname).unwrap();
                            basic_rdf_node_type_series_to_term_vec(&ser, t)
                        } else {
                            basic_rdf_node_type_series_to_term_vec(
                                &ser.struct_().unwrap().field_by_name(&colname).unwrap(),
                                t,
                            )
                        };
                        iters.push(v.into_iter())
                    }
                    let mut final_terms = vec![];
                    for _ in 0..height {
                        let mut use_term = None;
                        for iter in iters.iter_mut() {
                            if let Some(term) = iter.next() {
                                if let Some(term) = term {
                                    use_term = Some(term);
                                }
                            }
                        }
                        final_terms.push(use_term);
                    }
                    final_terms
                }
            };
            all_terms.push(terms);
        }
    }
    let mut solns = vec![];
    for _i in 0..height {
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

pub fn basic_rdf_node_type_series_to_term_vec(
    ser: &Series,
    base_rdfnode_type: &BaseRDFNodeType,
) -> Vec<Option<Term>> {
    match base_rdfnode_type {
        BaseRDFNodeType::IRI => ser
            .cast(&DataType::String)
            .unwrap()
            .str()
            .unwrap()
            .par_iter()
            .map(|x| x.map(|x| Term::NamedNode(literal_iri_to_namednode(x))))
            .collect(),
        BaseRDFNodeType::BlankNode => ser
            .cast(&DataType::String)
            .unwrap()
            .str()
            .unwrap()
            .par_iter()
            .map(|x| x.map(|x| Term::BlankNode(literal_blanknode_to_blanknode(x))))
            .collect(),
        BaseRDFNodeType::Literal(l) => match l.as_ref() {
            rdf::LANG_STRING => {
                let value_ser = ser
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_VALUE_FIELD)
                    .unwrap();
                let value_iter = value_ser.str().unwrap().into_iter();
                let lang_ser = ser
                    .struct_()
                    .unwrap()
                    .field_by_name(LANG_STRING_LANG_FIELD)
                    .unwrap();
                let lang_iter = lang_ser.str().unwrap().into_iter();
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
                .cast(&DataType::String)
                .unwrap()
                .str()
                .unwrap()
                .par_iter()
                .map(|x| x.map(|x| Term::Literal(Literal::new_simple_literal(x))))
                .collect(),
            dt => ser
                .cast(&DataType::String)
                .unwrap()
                .str()
                .unwrap()
                .par_iter()
                .map(|x| x.map(|x| Term::Literal(Literal::new_typed_literal(x, dt.into_owned()))))
                .collect(),
        },
        BaseRDFNodeType::None => {
            let mut v = vec![];
            for _ in 0..ser.len() {
                v.push(None);
            }
            v
        }
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
        DataType::String => Some(xsd::STRING),
        DataType::Date => Some(xsd::DATE),
        DataType::Datetime(_, Some(_)) => Some(xsd::DATE_TIME_STAMP),
        DataType::Datetime(_, None) => Some(xsd::DATE_TIME),
        DataType::Duration(_) => Some(xsd::DURATION),
        DataType::Categorical(_, _) => Some(xsd::STRING),
        _ => None,
    }
}
