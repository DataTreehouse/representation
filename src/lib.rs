pub mod literals;
pub mod multitype;
pub mod polars_to_sparql;
pub mod query_context;
pub mod solution_mapping;
pub mod sparql_to_polars;

use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNode, NamedNode, NamedNodeRef, NamedOrBlankNode, Term};
use polars_core::prelude::{DataType, TimeUnit};
use spargebra::term::TermPattern;
use std::fmt::{Display, Formatter};
use thiserror::*;

#[derive(Debug, Error)]
pub enum RepresentationError {
    #[error("Invalid literal `{0}`")]
    InvalidLiteralError(String),
}

pub const LANG_STRING_VALUE_FIELD: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>";
pub const LANG_STRING_LANG_FIELD: &str = "l";

const RDF_NODE_TYPE_IRI: &str = "IRI";
const RDF_NODE_TYPE_BLANK_NODE: &str = "Blank";
const RDF_NODE_TYPE_NONE: &str = "None";

#[derive(PartialEq, Clone)]
pub enum TripleType {
    ObjectProperty,
    StringProperty,
    LangStringProperty,
    NonStringProperty,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RDFNodeType {
    IRI,
    BlankNode,
    Literal(NamedNode),
    None,
    MultiType(Vec<BaseRDFNodeType>),
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
pub enum BaseRDFNodeType {
    IRI,
    BlankNode,
    Literal(NamedNode),
    None,
}

impl BaseRDFNodeType {
    pub fn from_rdf_node_type(r: &RDFNodeType) -> BaseRDFNodeType {
        match r {
            RDFNodeType::IRI => BaseRDFNodeType::IRI,
            RDFNodeType::BlankNode => BaseRDFNodeType::BlankNode,
            RDFNodeType::Literal(l) => BaseRDFNodeType::Literal(l.clone()),
            RDFNodeType::None => BaseRDFNodeType::None,
            RDFNodeType::MultiType(_) => {
                panic!()
            }
        }
    }

    pub fn is_lang_string(&self) -> bool {
        if let BaseRDFNodeType::Literal(l) = self {
            l.as_ref() == rdf::LANG_STRING
        } else {
            false
        }
    }

    pub fn as_rdf_node_type(&self) -> RDFNodeType {
        match self {
            BaseRDFNodeType::IRI => RDFNodeType::IRI,
            BaseRDFNodeType::BlankNode => RDFNodeType::BlankNode,
            BaseRDFNodeType::Literal(l) => RDFNodeType::Literal(l.clone()),
            BaseRDFNodeType::None => RDFNodeType::None,
        }
    }
}

impl Display for BaseRDFNodeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BaseRDFNodeType::IRI => {
                write!(f, "{RDF_NODE_TYPE_IRI}")
            }
            BaseRDFNodeType::BlankNode => {
                write!(f, "{RDF_NODE_TYPE_BLANK_NODE}")
            }
            BaseRDFNodeType::Literal(l) => {
                write!(f, "{}", l)
            }
            BaseRDFNodeType::None => {
                write!(f, "{RDF_NODE_TYPE_NONE}")
            }
        }
    }
}

impl Display for RDFNodeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RDFNodeType::IRI => {
                write!(f, "{RDF_NODE_TYPE_IRI}")
            }
            RDFNodeType::BlankNode => {
                write!(f, "{RDF_NODE_TYPE_BLANK_NODE}")
            }
            RDFNodeType::Literal(l) => {
                write!(f, "{}", l)
            }
            RDFNodeType::None => {
                write!(f, "{RDF_NODE_TYPE_NONE}")
            }
            RDFNodeType::MultiType(types) => {
                let type_strings: Vec<_> = types.iter().map(|x| x.to_string()).collect();
                write!(f, "Multiple({})", type_strings.join(", "))
            }
        }
    }
}

impl RDFNodeType {
    pub fn infer_from_term_pattern(tp: &TermPattern) -> Option<Self> {
        match tp {
            TermPattern::NamedNode(_) => Some(RDFNodeType::IRI),
            TermPattern::BlankNode(_) => None,
            TermPattern::Literal(l) => Some(RDFNodeType::Literal(l.datatype().into_owned())),
            TermPattern::Variable(_v) => None,
            _ => {
                unimplemented!()
            }
        }
    }

    pub fn is_lang_string(&self) -> bool {
        if let RDFNodeType::Literal(l) = self {
            l.as_ref() == rdf::LANG_STRING
        } else {
            false
        }
    }

    pub fn is_lit_type(&self, nnref: NamedNodeRef) -> bool {
        if let RDFNodeType::Literal(l) = self {
            if l.as_ref() == nnref {
                return true;
            }
        }
        false
    }

    pub fn is_bool(&self) -> bool {
        self.is_lit_type(xsd::BOOLEAN)
    }

    pub fn is_float(&self) -> bool {
        self.is_lit_type(xsd::FLOAT)
    }

    pub fn is_numeric(&self) -> bool {
        match self {
            RDFNodeType::Literal(l) => {
                matches!(
                    l.as_ref(),
                    xsd::INT
                        | xsd::BYTE
                        | xsd::SHORT
                        | xsd::INTEGER
                        | xsd::UNSIGNED_BYTE
                        | xsd::UNSIGNED_INT
                        | xsd::DECIMAL
                        | xsd::FLOAT
                        | xsd::DOUBLE
                )
            }
            _ => false,
        }
    }

    pub fn find_triple_type(&self) -> TripleType {
        match self {
            RDFNodeType::IRI | RDFNodeType::BlankNode => TripleType::ObjectProperty,
            RDFNodeType::Literal(lit) => {
                if lit.as_ref() == xsd::STRING {
                    TripleType::StringProperty
                } else if lit.as_ref() == rdf::LANG_STRING {
                    TripleType::LangStringProperty
                } else {
                    TripleType::NonStringProperty
                }
            }
            RDFNodeType::None => {
                panic!()
            }
            RDFNodeType::MultiType(..) => {
                panic!()
            }
        }
    }

    pub fn polars_data_type(&self) -> DataType {
        match self {
            RDFNodeType::IRI => DataType::String,
            RDFNodeType::BlankNode => DataType::String,
            RDFNodeType::Literal(l) => match l.as_ref() {
                xsd::STRING => DataType::String,
                xsd::UNSIGNED_INT => DataType::UInt32,
                xsd::UNSIGNED_LONG => DataType::UInt64,
                xsd::INTEGER | xsd::LONG => DataType::Int64,
                xsd::INT => DataType::Int32,
                xsd::DOUBLE | xsd::DECIMAL => DataType::Float64,
                xsd::FLOAT => DataType::Float32,
                xsd::BOOLEAN => DataType::Boolean,
                xsd::DATE_TIME => DataType::Datetime(TimeUnit::Nanoseconds, None),
                n => {
                    todo!("Datatype {} not supported yet", n)
                }
            },
            RDFNodeType::None => DataType::Boolean,
            RDFNodeType::MultiType(..) => todo!(),
        }
    }
}

pub fn literal_iri_to_namednode(s: &str) -> NamedNode {
    NamedNode::new_unchecked(&s[1..(s.len() - 1)])
}

pub fn literal_blanknode_to_blanknode(b: &str) -> BlankNode {
    BlankNode::new_unchecked(&b[2..b.len()])
}

pub fn owned_term_to_named_or_blank_node(t: Term) -> Option<NamedOrBlankNode> {
    match t {
        Term::NamedNode(nn) => Some(NamedOrBlankNode::NamedNode(nn)),
        Term::BlankNode(bl) => Some(NamedOrBlankNode::BlankNode(bl)),
        _ => None,
    }
}

pub fn owned_term_to_named_node(t: Term) -> Option<NamedNode> {
    match t {
        Term::NamedNode(nn) => Some(nn),
        _ => None,
    }
}
