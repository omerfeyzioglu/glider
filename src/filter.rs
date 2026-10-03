//! Query-only metadata predicates. Stored metadata remains string to string.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Filter {
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Eq(String, String),
    Ne(String, String),
    In(String, Vec<String>),
    Nin(String, Vec<String>),
    Exists(String, bool),
    Gt(String, f64),
    Gte(String, f64),
    Lt(String, f64),
    Lte(String, f64),
}

impl Filter {
    pub fn equality(pairs: &[(&str, &str)]) -> Self {
        Self::And(
            pairs
                .iter()
                .map(|(k, v)| Self::Eq((*k).into(), (*v).into()))
                .collect(),
        )
    }

    /// Parse the HTTP grammar with bounded recursion and leaf/list counts.
    pub fn parse(value: &Value) -> Result<Self, String> {
        let mut leaves = 0;
        parse_object(value, 0, &mut leaves)
    }

    pub fn matches(&self, metadata: &BTreeMap<String, String>) -> bool {
        use Filter::*;
        match self {
            And(parts) => parts.iter().all(|part| part.matches(metadata)),
            Or(parts) => parts.iter().any(|part| part.matches(metadata)),
            Not(part) => !part.matches(metadata),
            Eq(k, v) => metadata.get(k) == Some(v),
            Ne(k, v) => metadata.get(k) != Some(v),
            In(k, values) => metadata.get(k).is_some_and(|v| values.contains(v)),
            Nin(k, values) => metadata.get(k).is_none_or(|v| !values.contains(v)),
            Exists(k, present) => metadata.contains_key(k) == *present,
            Gt(k, n) => number(metadata, k).is_some_and(|v| v > *n),
            Gte(k, n) => number(metadata, k).is_some_and(|v| v >= *n),
            Lt(k, n) => number(metadata, k).is_some_and(|v| v < *n),
            Lte(k, n) => number(metadata, k).is_some_and(|v| v <= *n),
        }
    }

    /// Equality conditions guaranteed by the outer conjunction. Disjunctions
    /// and negations supply no routing requirements.
    pub fn required_equalities(&self) -> Vec<(&str, &str)> {
        let mut pairs = Vec::new();
        self.collect_equalities(&mut pairs);
        pairs
    }

    pub fn equality_pairs(&self) -> Option<Vec<(&str, &str)>> {
        match self {
            Self::And(parts) => {
                let mut pairs = Vec::new();
                for part in parts {
                    pairs.extend(part.equality_pairs()?);
                }
                Some(pairs)
            }
            Self::Eq(k, v) => Some(vec![(k, v)]),
            _ => None,
        }
    }

    fn collect_equalities<'a>(&'a self, pairs: &mut Vec<(&'a str, &'a str)>) {
        match self {
            Self::And(parts) => parts.iter().for_each(|part| part.collect_equalities(pairs)),
            Self::Eq(k, v) => pairs.push((k, v)),
            _ => {}
        }
    }

    /// Resident vectors alone prove the result only for the declared equality
    /// (possibly repeated). Other conditions require metadata from blocks.
    pub fn only_equality(&self, key: &str, value: &str) -> bool {
        match self {
            Self::And(parts) => parts.iter().all(|part| part.only_equality(key, value)),
            Self::Eq(k, v) => k == key && v == value,
            _ => false,
        }
    }
}

fn number(metadata: &BTreeMap<String, String>, key: &str) -> Option<f64> {
    metadata
        .get(key)?
        .parse::<f64>()
        .ok()
        .filter(|n| n.is_finite())
}

fn parse_object(value: &Value, depth: usize, leaves: &mut usize) -> Result<Filter, String> {
    if depth > 8 {
        return Err("filter nesting depth exceeds 8".into());
    }
    let object = value.as_object().ok_or("filter must be an object")?;
    let mut parts = Vec::new();
    for (key, value) in object {
        match key.as_str() {
            "$and" | "$or" => {
                let array = value
                    .as_array()
                    .ok_or_else(|| format!("{key} must be an array"))?;
                let children = array
                    .iter()
                    .map(|v| parse_object(v, depth + 1, leaves))
                    .collect::<Result<Vec<_>, _>>()?;
                parts.push(if key == "$and" {
                    Filter::And(children)
                } else {
                    Filter::Or(children)
                });
            }
            "$not" => parts.push(Filter::Not(Box::new(parse_object(
                value,
                depth + 1,
                leaves,
            )?))),
            _ if key.starts_with('$') => return Err(format!("unknown filter operator {key}")),
            _ => match value {
                Value::String(v) => push(&mut parts, leaves, Filter::Eq(key.clone(), v.clone()))?,
                Value::Object(operators) if !operators.is_empty() => {
                    for (operator, argument) in operators {
                        let condition = match operator.as_str() {
                            "$eq" => Filter::Eq(key.clone(), string(argument, operator)?),
                            "$ne" => Filter::Ne(key.clone(), string(argument, operator)?),
                            "$in" => Filter::In(key.clone(), strings(argument, operator)?),
                            "$nin" => Filter::Nin(key.clone(), strings(argument, operator)?),
                            "$exists" => Filter::Exists(
                                key.clone(),
                                argument.as_bool().ok_or("$exists must be a boolean")?,
                            ),
                            "$gt" => Filter::Gt(key.clone(), numeric(argument, operator)?),
                            "$gte" => Filter::Gte(key.clone(), numeric(argument, operator)?),
                            "$lt" => Filter::Lt(key.clone(), numeric(argument, operator)?),
                            "$lte" => Filter::Lte(key.clone(), numeric(argument, operator)?),
                            _ => return Err(format!("unknown filter operator {operator}")),
                        };
                        push(&mut parts, leaves, condition)?;
                    }
                }
                _ => {
                    return Err(format!(
                        "filter key {key} needs a string or nonempty operator object"
                    ))
                }
            },
        }
    }
    Ok(Filter::And(parts))
}

fn push(parts: &mut Vec<Filter>, leaves: &mut usize, condition: Filter) -> Result<(), String> {
    *leaves += 1;
    if *leaves > 64 {
        return Err("filter exceeds 64 leaf conditions".into());
    }
    parts.push(condition);
    Ok(())
}

fn string(value: &Value, operator: &str) -> Result<String, String> {
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("{operator} needs a string"))
}

fn strings(value: &Value, operator: &str) -> Result<Vec<String>, String> {
    let array = value
        .as_array()
        .ok_or_else(|| format!("{operator} needs an array"))?;
    if array.len() > 1024 {
        return Err(format!("{operator} exceeds 1024 values"));
    }
    array.iter().map(|v| string(v, operator)).collect()
}

fn numeric(value: &Value, operator: &str) -> Result<f64, String> {
    let number = value.as_f64().filter(|n| n.is_finite());
    number.ok_or_else(|| format!("{operator} needs a finite JSON number"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn grammar_and_limits() {
        let valid = json!({"kind":"a","age":{"$gt":1.5,"$lte":10},"$or":[{"x":{"$in":["a","b"]}},{"$not":{"x":{"$exists":true}}}]});
        assert_eq!(
            Filter::parse(&valid).unwrap().required_equalities(),
            vec![("kind", "a")]
        );
        assert_eq!(
            Filter::parse(&json!({"$and":[{"kind":"a"},{"nested":{"$eq":"b"}}]}))
                .unwrap()
                .required_equalities(),
            vec![("kind", "a"), ("nested", "b")]
        );
        for bad in [
            json!(null),
            json!({"$bad":1}),
            json!({"$and":{}}),
            json!({"$or":[1]}),
            json!({"$not":1}),
            json!({"x":1}),
            json!({"x":{}}),
            json!({"x":{"$bad":1}}),
            json!({"x":{"$eq":1}}),
            json!({"x":{"$ne":false}}),
            json!({"x":{"$in":1}}),
            json!({"x":{"$in":[1]}}),
            json!({"x":{"$nin":1}}),
            json!({"x":{"$nin":[1]}}),
            json!({"x":{"$exists":"yes"}}),
            json!({"x":{"$gt":"2"}}),
            json!({"x":{"$gte":null}}),
            json!({"x":{"$lt":false}}),
            json!({"x":{"$lte":"2"}}),
        ] {
            assert!(Filter::parse(&bad).is_err(), "accepted {bad}");
        }
        let mut deep = json!({});
        for _ in 0..8 {
            deep = json!({"$not":deep});
        }
        Filter::parse(&deep).unwrap();
        deep = json!({"$not":deep});
        assert!(Filter::parse(&deep).unwrap_err().contains("depth"));
        let exactly_64 = Value::Object((0..64).map(|n| (n.to_string(), json!("v"))).collect());
        Filter::parse(&exactly_64).unwrap();
        let many = Value::Object((0..65).map(|n| (n.to_string(), json!("v"))).collect());
        assert!(Filter::parse(&many).unwrap_err().contains("64"));
        for op in ["$in", "$nin"] {
            let valid = Value::Object(serde_json::Map::from_iter([(
                "x".into(),
                Value::Object(serde_json::Map::from_iter([(
                    op.into(),
                    json!(vec!["v"; 1024]),
                )])),
            )]));
            Filter::parse(&valid).unwrap();
            let operators = serde_json::Map::from_iter([(op.into(), json!(vec!["v"; 1025]))]);
            let filter = Value::Object(serde_json::Map::from_iter([(
                "x".into(),
                Value::Object(operators),
            )]));
            assert!(Filter::parse(&filter).unwrap_err().contains("1024"));
        }
    }

    #[test]
    fn evaluation() {
        let metadata = BTreeMap::from([
            ("n".into(), "2.5".into()),
            ("bad".into(), "NaN".into()),
            ("infinite".into(), "inf".into()),
        ]);
        for filter in [
            json!({"n":{"$gt":2,"$lte":2.5}}),
            json!({"missing":{"$ne":"x","$nin":["y"]}}),
            json!({"$not":{"missing":{"$exists":true}}}),
            json!({"$or":[{"n":"no"},{"n":{"$gte":2.5}}]}),
        ] {
            assert!(
                Filter::parse(&filter).unwrap().matches(&metadata),
                "{filter}"
            );
        }
        for filter in [
            json!({"n":{"$lt":2}}),
            json!({"bad":{"$gt":0}}),
            json!({"infinite":{"$gt":0}}),
            json!({"missing":{"$lte":3}}),
            json!({"missing":{"$in":[]}}),
            json!({"n":{"$nin":["2.5"]}}),
        ] {
            assert!(
                !Filter::parse(&filter).unwrap().matches(&metadata),
                "{filter}"
            );
        }
    }
}
