use crate::{BaseRDFNodeType, RDFNodeType, LANG_STRING_LANG_FIELD, LANG_STRING_VALUE_FIELD};
use oxrdf::vocab::{rdf};
use polars::prelude::{as_struct, col, lit, LazyFrame, LiteralValue, Expr, JoinArgs, JoinType};
use polars_core::frame::DataFrame;
use polars_core::prelude::{DataType};
use std::collections::{HashMap, HashSet};
use polars_core::datatypes::CategoricalOrdering;
use uuid::Uuid;

pub const MULTI_IRI_DT: &str = "I";
pub const MULTI_BLANK_DT: &str = "B";
pub const MULTI_NONE_DT: &str = "N";

pub const BOUND_SUFFIX: &str = "_bound";

fn is_dt(s: &str) -> String {
    format!("{s}_is")
}
pub fn convert_lf_col_to_multitype(lf: LazyFrame, c: &str, dt: &RDFNodeType) -> LazyFrame {
    match dt {
        RDFNodeType::IRI => lf.with_column(
            as_struct(vec![
                col(c).alias(MULTI_IRI_DT),
                lit(true).alias(&is_dt(MULTI_IRI_DT)),
            ])
            .alias(c),
        ),
        RDFNodeType::BlankNode => lf.with_column(
            as_struct(vec![
                col(c)
                    .cast(DataType::Categorical(None, CategoricalOrdering::Physical))
                    .alias(MULTI_BLANK_DT),
                lit(true).alias(&is_dt(MULTI_BLANK_DT)),
            ])
            .alias(c),
        ),
        RDFNodeType::Literal(l) => {
            if rdf::LANG_STRING == l.as_ref() {
                lf.with_column(
                    as_struct(vec![
                        col(c)
                            .struct_()
                            .field_by_name(LANG_STRING_VALUE_FIELD)
                            .alias(LANG_STRING_VALUE_FIELD),
                        col(c)
                            .struct_()
                            .field_by_name(LANG_STRING_LANG_FIELD)
                            .alias(LANG_STRING_LANG_FIELD),
                        lit(true).alias(&is_dt(LANG_STRING_VALUE_FIELD)),
                    ])
                    .alias(c),
                )
            } else {
                let colname = non_multi_type_string(&BaseRDFNodeType::from_rdf_node_type(dt));
                lf.with_column(
                    as_struct(vec![
                        col(c).alias(&colname),
                        lit(true).alias(&is_dt(&colname)),
                    ])
                    .alias(c),
                )
            }
        }
        RDFNodeType::None => {
            panic!()
        }
        RDFNodeType::MultiType(..) => lf,
    }
}

pub fn non_multi_type_string(dt: &BaseRDFNodeType) -> String {
    match dt {
        BaseRDFNodeType::IRI => MULTI_IRI_DT.to_string(),
        BaseRDFNodeType::BlankNode => MULTI_BLANK_DT.to_string(),
        BaseRDFNodeType::Literal(l) => l.to_string(),
        BaseRDFNodeType::None => MULTI_NONE_DT.to_string(),
    }
}

fn as_sorted_vec(types: HashSet<BaseRDFNodeType>) -> Vec<BaseRDFNodeType> {
    let mut types:Vec<_> = types.into_iter().collect();
    types.sort();
    types
}

pub fn create_join_compatible_solution_mappings(
    mut left_mappings: LazyFrame,
    mut left_datatypes: HashMap<String, RDFNodeType>,
    mut right_mappings: LazyFrame,
    mut right_datatypes: HashMap<String, RDFNodeType>,
    inner: bool,
) -> (
    LazyFrame,
    HashMap<String, RDFNodeType>,
    LazyFrame,
    HashMap<String, RDFNodeType>,
) {
    let mut new_left_datatypes = HashMap::new();
    let mut new_right_datatypes = HashMap::new();
    for (v, right_dt) in &right_datatypes {
        if let Some(left_dt) = left_datatypes.get(v) {
            if right_dt != left_dt {
                if let RDFNodeType::MultiType(left_types) = left_dt {
                    if let RDFNodeType::MultiType(right_types) = right_dt {
                        let right_set: HashSet<BaseRDFNodeType> = HashSet::from_iter(right_types.clone().into_iter());
                        let left_set: HashSet<BaseRDFNodeType> = HashSet::from_iter(left_types.clone().into_iter());
                        if inner {
                            //let left_remove = &left_set - &right_set;
                            //let right_remove = &right_set - &left_set;
                            let mut keep: Vec<_> = left_set.intersection(&right_set).cloned().collect();
                            keep.sort();
                            if keep.is_empty() {
                                left_mappings = left_mappings.filter(lit(false)).with_column(lit(true).alias(v));
                                right_mappings = right_mappings.filter(lit(false)).with_column(lit(true).alias(v));
                                new_left_datatypes.insert(v.clone(), RDFNodeType::None);
                                new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                            } else if keep.len() == 1 {
                                let t = keep.get(0).unwrap();
                                left_mappings = force_convert_multicol_to_single_col(left_mappings, v, t);
                                right_mappings = force_convert_multicol_to_single_col(right_mappings, v, t);
                                new_left_datatypes.insert(v.clone(), t.as_rdf_node_type());
                                new_right_datatypes.insert(v.clone(), t.as_rdf_node_type());
                            } else {
                                let all_main_cols = all_multi_main_cols(&keep);
                                let mut is_col_expr: Option<Expr> = None;
                                for c in all_main_cols {
                                    let e = col(v).struct_().field_by_name(&is_dt(&c));
                                    is_col_expr = if let Some(is_col_expr) = is_col_expr {
                                        Some(is_col_expr.or(e))
                                    } else {
                                        Some(e)
                                    };
                                }
                                let all_cols = all_multi_cols(&keep);
                                let mut struct_cols = vec![];
                                for c in &all_cols {
                                    struct_cols.push(col(v).struct_().field_by_name(c).alias(c));
                                }

                                left_mappings = left_mappings.filter(
                                    is_col_expr.as_ref().unwrap().clone()
                                ).with_column(
                                    as_struct(struct_cols.clone()).alias(v)
                                );

                                right_mappings = right_mappings.filter(
                                    is_col_expr.as_ref().unwrap().clone()
                                ).with_column(
                                    as_struct(struct_cols.clone()).alias(v)
                                );
                                new_left_datatypes.insert(v.to_string(), RDFNodeType::MultiType(keep.clone()));
                                new_right_datatypes.insert(v.to_string(), RDFNodeType::MultiType(keep.clone()));
                            }
                        } else {
                            let mut right_keep: Vec<_> = left_set.intersection(&right_set).cloned().collect();
                            right_keep.sort();
                            if right_keep.is_empty() {
                                right_mappings = right_mappings.filter(lit(false)).with_column(lit(LiteralValue::Null).cast(DataType::Null).alias(v));
                                new_right_datatypes.insert(v.to_string(), RDFNodeType::None);
                            } else if right_keep.len() == 1 {
                                let t = right_keep.get(0).unwrap();
                                right_mappings = force_convert_multicol_to_single_col(right_mappings, v, t);
                                new_right_datatypes.insert(v.to_string(), t.as_rdf_node_type());
                            } else {
                                let all_main_cols = all_multi_main_cols(&right_keep);
                                let mut is_col_expr: Option<Expr> = None;
                                for c in all_main_cols {
                                    let e = col(v).struct_().field_by_name(&is_dt(&c));
                                    is_col_expr = if let Some(is_col_expr) = is_col_expr {
                                        Some(is_col_expr.or(e))
                                    } else {
                                        Some(e)
                                    };
                                }
                                let all_cols = all_multi_cols(&right_keep);
                                let mut struct_cols = vec![];
                                for c in &all_cols {
                                    struct_cols.push(col(v).struct_().field_by_name(c).alias(c));
                                }

                                right_mappings = right_mappings.filter(
                                    is_col_expr.unwrap().clone()
                                ).with_column(
                                    as_struct(struct_cols.clone()).alias(v)
                                );
                                new_right_datatypes.insert(v.to_string(), RDFNodeType::MultiType(right_keep.clone()));
                            }
                        }
                    } else { //right not multi
                        let base_right = BaseRDFNodeType::from_rdf_node_type(right_dt);
                        if inner {
                            if left_types.contains(&base_right) {
                                left_mappings =
                                    force_convert_multicol_to_single_col(left_mappings, v, &BaseRDFNodeType::from_rdf_node_type(right_dt));
                                new_left_datatypes.insert(v.clone(), right_dt.clone());
                            } else {
                                left_mappings = left_mappings.filter(lit(false)).with_column(lit(LiteralValue::Null).cast(DataType::Null).alias(v));
                                new_left_datatypes.insert(v.clone(), RDFNodeType::None);
                                right_mappings = right_mappings.filter(lit(false)).with_column(lit(LiteralValue::Null).cast(DataType::Null).alias(v));
                                new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                            }
                        } else {
                            if !left_types.contains(&base_right) {
                                right_mappings = right_mappings.filter(lit(false)).with_column(lit(LiteralValue::Null).cast(DataType::Null).alias(v));
                                new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                            }
                        }
                    }
                } else {
                    let left_basic_dt = BaseRDFNodeType::from_rdf_node_type(left_dt);
                    if let RDFNodeType::MultiType(right_types) = right_dt { //Left not multi
                        if inner {
                            if right_types.contains(&left_basic_dt) {
                                right_mappings =
                                    force_convert_multicol_to_single_col(right_mappings, v, &left_basic_dt);
                                new_right_datatypes.insert(v.clone(), left_dt.clone());
                            } else {
                                left_mappings = left_mappings.filter(lit(false)).with_column(lit(LiteralValue::Null).cast(DataType::Null).alias(v));
                                new_left_datatypes.insert(v.clone(), RDFNodeType::None);
                                right_mappings = right_mappings.filter(lit(false)).with_column(lit(LiteralValue::Null).cast(DataType::Null).alias(v));
                                new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                            }
                        } else {
                           if right_types.contains(&left_basic_dt) {
                               right_mappings =
                                   force_convert_multicol_to_single_col(right_mappings, v, &left_basic_dt);
                               new_right_datatypes.insert(v.clone(), left_dt.clone());
                           } else {
                               right_mappings = right_mappings.filter(lit(false)).with_column(lit(LiteralValue::Null).cast(DataType::Null).alias(v));
                               new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                           }
                        }
                    } else {
                        if inner {
                            left_mappings = left_mappings.filter(lit(false)).with_column(lit(LiteralValue::Null).cast(DataType::Null).alias(v));
                            new_left_datatypes.insert(v.clone(), RDFNodeType::None);
                            right_mappings = right_mappings.filter(lit(false)).with_column(lit(LiteralValue::Null).cast(DataType::Null).alias(v));
                            new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                        } else {
                            right_mappings = right_mappings.filter(lit(false)).with_column(lit(LiteralValue::Null).cast(DataType::Null).alias(v));
                            new_right_datatypes.insert(v.clone(), RDFNodeType::None);
                        }
                    }
                }
            }
        }
    }
    left_datatypes.extend(new_left_datatypes);
    right_datatypes.extend(new_right_datatypes);
    (
        left_mappings,
        left_datatypes,
        right_mappings,
        right_datatypes,
    )
}

pub fn all_multi_cols(dts: &Vec<BaseRDFNodeType>) -> Vec<String> {
    let mut all_cols = vec![];

    for d in dts {
        let colname = non_multi_type_string(d);
        if d.is_lang_string() {
            all_cols.push(LANG_STRING_LANG_FIELD.to_string());
        }
        all_cols.push(colname);
    }
    all_cols
}

pub fn all_multi_main_cols(dts: &Vec<BaseRDFNodeType>) -> Vec<String> {
    let mut all_cols = vec![];

    for d in dts {
        let colname = non_multi_type_string(d);
        all_cols.push(colname);
    }
    all_cols
}

pub fn all_multi_and_is_cols(dts: &Vec<BaseRDFNodeType>) -> Vec<String> {
    let mut all_cols = vec![];

    for d in dts {
        let colname = non_multi_type_string(d);
        if d.is_lang_string() {
            all_cols.push(LANG_STRING_LANG_FIELD.to_string());
        }
        all_cols.push(is_dt(&colname));
        all_cols.push(colname);
    }
    all_cols
}

pub fn force_convert_multicol_to_single_col(
    mut lf: LazyFrame,
    c: &str,
    dt: &BaseRDFNodeType,
) -> LazyFrame {
    lf = lf.filter(
        col(c)
            .struct_()
            .field_by_name(&is_dt(&non_multi_type_string(dt))),
    );
    known_convert_lf_multicol_to_single(lf, c, dt)
}

pub fn known_convert_lf_multicol_to_single(
    mut lf: LazyFrame,
    c: &str,
    dt: &BaseRDFNodeType,
) -> LazyFrame {
    if dt.is_lang_string() {
        lf = lf.with_column(
            as_struct(vec![
                col(c).struct_().field_by_name(LANG_STRING_VALUE_FIELD),
                col(c).struct_().field_by_name(LANG_STRING_LANG_FIELD),
            ])
            .alias(c),
        );
    } else {
        lf = lf.with_column(
            col(c)
                .struct_()
                .field_by_name(&non_multi_type_string(&dt))
                .alias(c),
        )
    }
    lf
}

pub fn explode_multicols<'a>(mut mappings: LazyFrame, multicols: &'a HashMap<String, RDFNodeType>, prefix:&str) -> (LazyFrame, HashMap<&'a String, (Vec<String>, Vec<String>)>) {
    let mut exprs = vec![];
    let mut out_map = HashMap::new();
    for (c,t) in multicols {
        if let RDFNodeType::MultiType(types) = t{
            let inner_cols = all_multi_and_is_cols(types);
            let mut prefixed_inner_cols = vec![];
            for inner in &inner_cols {
                let prefixed_inner = format!("{c}{inner}");
                exprs.push(
                    col(c).struct_().field_by_name(&inner).alias(&prefixed_inner)
                );
                prefixed_inner_cols.push(prefixed_inner);
            }
            out_map.insert(c, (inner_cols, prefixed_inner_cols));
        } else {
            panic!("Should not be called with columns that do not have multiple types")
        }
    }
    mappings = mappings.with_columns(exprs);
    mappings = mappings.drop(multicols.keys());
    (mappings, out_map)
}

pub fn implode_multicolumns(mapping:LazyFrame, map: HashMap<&String, (Vec<String>, Vec<String>)>) -> LazyFrame {
    let mut structs = vec![];
    let mut drop_cols = vec![];
    for (k, (inner_cols, prefixed_inner_cols)) in map {
        let mut struct_exprs = vec![];
        for (inner_col, prefixed_inner_col) in inner_cols.iter().zip(prefixed_inner_cols.iter()) {
            struct_exprs.push(col(prefixed_inner_col).alias(inner_col));
        }
        drop_cols.extend(prefixed_inner_cols);
        structs.push(as_struct(struct_exprs));
    }
    mapping.with_columns(
        structs
    ).drop(drop_cols)
}

pub fn join_workaround(
    left_mappings: LazyFrame,
    left_datatypes: &HashMap<String, RDFNodeType>,
    right_mappings: LazyFrame,
    right_datatypes: &HashMap<String, RDFNodeType>,
    join_args: JoinArgs,
) -> LazyFrame {
    let mut left_explode = HashMap::new();
    let mut right_explode = HashMap::new();
    for (c, t) in left_datatypes {
        if let RDFNodeType::MultiType(..) = t {
            left_explode.insert(c.clone(), t.clone());
        }
    }
    for (c, t) in right_datatypes {
        if let RDFNodeType::MultiType(..) = t {
            right_explode.insert(c.clone(), t.clone());
        }
    }
    let prefix = Uuid::new_v4().to_string();
    let (mut left_mappings, left_exploded) = explode_multicols(left_mappings, &left_explode, &prefix);
    let (right_mappings, mut right_exploded) = explode_multicols(right_mappings, &right_explode, &prefix);

    let mut on = vec![];
    let mut no_join = false;
    for (c, t) in left_datatypes {
        if right_datatypes.contains_key(c) {
            let left_set: HashSet<_> = if let Some((_, prefixed_inner_columns)) = left_exploded.get(c) {
                prefixed_inner_columns.iter().collect()
            } else {
                HashSet::from([c])
            };
            let right_set: HashSet<_> = if let Some((_, prefixed_inner_columns)) = right_exploded.get(c) {
                prefixed_inner_columns.iter().collect()
            } else {
                HashSet::from([c])
            };
            let intersection:Vec<_> = left_set.intersection(&right_set).map(|x|col(*x)).collect();
            if intersection.is_empty() {
                no_join = true;
                break;
            } else {
                on.extend(intersection);
            }
        }
    }
    if no_join {
        left_mappings = left_mappings.join(right_mappings, [lit(false)], [lit(true)], join_args);
    } else if !on.is_empty() {
        left_mappings = left_mappings.join(right_mappings, &on, &on, join_args);
    } else {
        left_mappings = left_mappings.join(right_mappings, &[], &[], JoinType::Cross.into());
    }
    let mut unified_exploded = HashMap::new();
    for (c, (mut left_inner_columns, mut left_prefixed_inner_columns)) in left_exploded {
        if let Some((right_inner_columns, right_prefixed_inner_columns)) = right_exploded.remove(c) {
            for (r, pr) in right_inner_columns.into_iter().zip(right_prefixed_inner_columns.into_iter()) {
                if !left_inner_columns.contains(&r) {
                    left_inner_columns.push(r);
                    left_prefixed_inner_columns.push(pr);
                }
            }
        }
        unified_exploded.insert(c, (left_inner_columns, left_prefixed_inner_columns));
    }
    unified_exploded.extend(right_exploded.into_iter());

    left_mappings = implode_multicolumns(left_mappings, unified_exploded);
    left_mappings
}

pub fn lf_printer(lf: &LazyFrame) {
    let df = lf_destruct(lf);
    println!("DF: {}", df);
}

pub fn lf_destruct(lf: &LazyFrame) -> DataFrame {
    todo!()
    // let df = lf.clone().collect().unwrap();
    // let colnames: Vec<_> = df
    //     .get_column_names()
    //     .iter()
    //     .map(|x| x.to_string())
    //     .collect();
    // let mut series_vec = vec![];
    // for c in colnames {
    //     let ser = df.column(&c).unwrap();
    //     if let DataType::Categorical(_) = ser.dtype() {
    //         series_vec.push(ser.cast(&DataType::String).unwrap());
    //     } else if let DataType::Struct(fields) = ser.dtype() {
    //         if fields.len() == 3 {
    //             let mut tmp_lf = DataFrame::new(vec![ser.clone()]).unwrap().lazy();
    //             let value_name = format!("{}_{}", c, MULTI_VALUE_COL);
    //             let lang_name = format!("{}_{}", c, MULTI_LANG_COL);
    //             let dt_name = format!("{}_{}", c, MULTI_DT_COL);
    //             tmp_lf = tmp_lf.with_columns([
    //                 col(&c)
    //                     .struct_()
    //                     .field_by_name(MULTI_VALUE_COL)
    //                     .cast(DataType::String)
    //                     .alias(&value_name),
    //                 col(&c)
    //                     .struct_()
    //                     .field_by_name(MULTI_LANG_COL)
    //                     .cast(DataType::String)
    //                     .alias(&lang_name),
    //                 col(&c)
    //                     .struct_()
    //                     .field_by_name(MULTI_DT_COL)
    //                     .cast(DataType::String)
    //                     .alias(&dt_name),
    //             ]);
    //             let mut tmp_df = tmp_lf.collect().unwrap();
    //             series_vec.push(tmp_df.drop_in_place(&value_name).unwrap());
    //             series_vec.push(tmp_df.drop_in_place(&lang_name).unwrap());
    //             series_vec.push(tmp_df.drop_in_place(&dt_name).unwrap());
    //         } else {
    //             series_vec.push(ser.clone());
    //         }
    //     } else {
    //         series_vec.push(ser.clone());
    //     }
    // }
    // DataFrame::new(series_vec).unwrap()
}

// pub fn join_workaround(
//     mut left_mappings: LazyFrame,
//     left_datatypes: &HashMap<String, RDFNodeType>,
//     mut right_mappings: LazyFrame,
//     right_datatypes: &HashMap<String, RDFNodeType>,
//     how: JoinArgs,
// ) -> LazyFrame {
//     let mut multi_cols = vec![];
//     let mut join_cols = vec![];
//     for (k, dt) in left_datatypes {
//         if right_datatypes.contains_key(k) {
//             if let RDFNodeType::MultiType(left_types) = dt {
//                 let left_suffix = uuid::Uuid::new_v4().to_string();
//                 let all_left_cols = all_multi_and_is_cols(left_types);
//                 left_mappings = split_multi_col(left_mappings, k, &all_left_cols, &suffix);
//                 right_mappings = split_multi_col(right_mappings, k, &col_value, &col_lang, &col_dt);
//
//                 join_cols.push(col_value.clone());
//                 join_cols.push(col_lang.clone());
//                 join_cols.push(col_dt.clone());
//
//                 multi_cols.push((k, col_value, col_lang, col_dt));
//             } else {
//                 join_cols.push(k.to_string());
//             }
//         }
//     }
//     let join_col_expr: Vec<_> = join_cols.into_iter().map(|x| col(&x)).collect();
//     let mut joined = left_mappings.join(right_mappings, &join_col_expr, &join_col_expr, how);
//     for (k, v, lang, dt) in multi_cols {
//         joined = combine_multi_col(joined, k, &v, &lang, &dt);
//     }
//     joined
// }
//
// fn split_multi_col(
//     lf: LazyFrame,
//     k: &str,
//     col_value: &str,
//     col_lang: &str,
//     col_dt: &str,
// ) -> LazyFrame {
//     lf.with_columns([
//         col(k)
//             .struct_()
//             .field_by_name(MULTI_VALUE_COL)
//             .alias(col_value),
//         col(k)
//             .struct_()
//             .field_by_name(MULTI_LANG_COL)
//             .alias(col_lang),
//         col(k).struct_().field_by_name(MULTI_DT_COL).alias(col_dt),
//     ])
//     .drop_columns(vec![k])
// }
//
// fn combine_multi_col(
//     lf: LazyFrame,
//     k: &str,
//     col_value: &str,
//     col_lang: &str,
//     col_dt: &str,
// ) -> LazyFrame {
//     lf.with_column(
//         as_struct(vec![
//             col(col_value).alias(MULTI_VALUE_COL),
//             col(col_dt).alias(MULTI_DT_COL),
//             col(col_lang).alias(MULTI_LANG_COL),
//         ])
//         .alias(k),
//     )
//     .drop_columns(vec![col_value, col_lang, col_dt])
// }
