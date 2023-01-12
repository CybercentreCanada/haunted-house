use serde::{Serialize, Deserialize};
use anyhow::Result;


#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug)]
pub enum Query {
    Not(Box<Query>),
    And(Vec<Query>),
    Or(Vec<Query>),
    Literal(Vec<u8>),
}

#[cfg(test)]
mod test {
    use super::Query;


    #[test]
    fn to_json() {
        let query = Query::Or(vec![
            Query::And(vec![
                Query::String("pending_timers".to_string()),
                Query::String("pending_batch".to_string())
            ]),
            Query::String("get_bucket_range".to_string())
        ]);

        assert_eq!(serde_json::to_string(&query).unwrap(), r#"{"Or":[{"And":[{"String":"pending_timers"},{"String":"pending_batch"}]},{"String":"get_bucket_range"}]}"#)
    }

}


pub struct QueryResult {
    query: Query,
    warning_messages: Vec<String>,
}

impl QueryResult {
    pub fn from_src(rule_src: &str) -> Result<QueryResult> {

        let parser = yara_x::parser::Parser::new();
        let ast = parser.build_ast(rule_src)?;
        let mut warnings = vec![];

        let namespace = if ast.namespaces.len() == 1 {
            &ast.namespaces[0]
        } else {
            return Err(anyhow::anyhow!("Multiple namespaces, which search rule to use unclear."));
        };

        let rule = if let Some(rule) = namespace.rules.last() {
            if namespace.rules.len() > 1 {
                warnings.push(format!("Multiple rules in search, using last rule: {}", rule.identifier.name));
            }
            rule
        } else {
            return Err(anyhow::anyhow!("No rules in query."));
        };

        let query = build_query(&rule.condition, &rule.patterns.unwrap_or_default())?;

        Ok(Self {
            query,
            warning_messages: warnings,
        })
    }
}

use yara_x::ast::{Value, Pattern};

enum Part {
    Query(Query),
    Constant(Value),
}

impl From<Query> for Part {
    fn from(item: Query) -> Self { Part::Query(item) }
}

impl From<Value> for Part {
    fn from(item: Value) -> Self { Part::Constant(item) }
}

fn build_query(expr: &yara_x::ast::Expr, patterns: &Vec<Pattern>) -> Result<Query> {
    match try_build_expr(expr, patterns)? {
        Part::Query(query) => Ok(query),
        Part::Constant(_) => Err(anyhow::anyhow!("Yara condition resolved to value?")),
    }
}


fn try_build_expr(expr: &yara_x::ast::Expr, patterns: &Vec<Pattern>) -> Result<Part> {
    Ok(match expr {
        yara_x::ast::Expr::True { .. } => Value::Bool(true).into(),
        yara_x::ast::Expr::False { .. } => Value::Bool(false).into(),
        yara_x::ast::Expr::Filesize { .. } => Value::Unknown.into(),
        yara_x::ast::Expr::Entrypoint { .. } => Value::Unknown.into(),
        yara_x::ast::Expr::Literal(literal) => literal.value.clone().into(),
        yara_x::ast::Expr::Ident(_) => todo!(),
        yara_x::ast::Expr::PatternMatch(pattern) => {
            let pattern = match patterns.iter().find(|item| item.identifier().name == pattern.identifier.name) {
                Some(p) => p,
                None => return Err(anyhow::anyhow!("")),
            };

            query_from_pattern(pattern)?.into()
        },
        yara_x::ast::Expr::PatternCount(pattern) => {
            match &pattern.range {
                None => {
                    let pattern = match patterns.iter().find(|item| item.identifier().name == pattern.name) {
                        Some(p) => p,
                        None => return Err(anyhow::anyhow!("")),
                    };
                    query_from_pattern(pattern)?.into()
                }
                Some(range) => {
                    let pattern = match patterns.iter().find(|item| item.identifier().name == pattern.name) {
                        Some(p) => p,
                        None => return Err(anyhow::anyhow!("")),
                    };
                    let query = query_from_pattern(pattern)?;
                    if range.upper_bound == 0 {
                        Query::Not(query).into()
                    } else {
                        query.into()
                    }
                },
            }
        },
        yara_x::ast::Expr::PatternOffset(_) => todo!(),
        yara_x::ast::Expr::PatternLength(_) => todo!(),
        yara_x::ast::Expr::Lookup(_) => todo!(),
        yara_x::ast::Expr::FieldAccess(_) => todo!(),
        yara_x::ast::Expr::FnCall(_) => todo!(),
        yara_x::ast::Expr::Not(expr) => todo!(),
        yara_x::ast::Expr::And(_) => todo!(),
        yara_x::ast::Expr::Or(_) => todo!(),
        yara_x::ast::Expr::Minus(_) => todo!(),
        yara_x::ast::Expr::Add(_) => todo!(),
        yara_x::ast::Expr::Sub(_) => todo!(),
        yara_x::ast::Expr::Mul(_) => todo!(),
        yara_x::ast::Expr::Div(_) => todo!(),
        yara_x::ast::Expr::Modulus(_) => todo!(),
        yara_x::ast::Expr::BitwiseNot(_) => todo!(),
        yara_x::ast::Expr::Shl(_) => todo!(),
        yara_x::ast::Expr::Shr(_) => todo!(),
        yara_x::ast::Expr::BitwiseAnd(_) => todo!(),
        yara_x::ast::Expr::BitwiseOr(_) => todo!(),
        yara_x::ast::Expr::BitwiseXor(_) => todo!(),
        yara_x::ast::Expr::Eq(_) => todo!(),
        yara_x::ast::Expr::Ne(_) => todo!(),
        yara_x::ast::Expr::Lt(_) => todo!(),
        yara_x::ast::Expr::Gt(_) => todo!(),
        yara_x::ast::Expr::Le(_) => todo!(),
        yara_x::ast::Expr::Ge(_) => todo!(),
        yara_x::ast::Expr::Contains(_) => todo!(),
        yara_x::ast::Expr::IContains(_) => todo!(),
        yara_x::ast::Expr::StartsWith(_) => todo!(),
        yara_x::ast::Expr::IStartsWith(_) => todo!(),
        yara_x::ast::Expr::EndsWith(_) => todo!(),
        yara_x::ast::Expr::IEndsWith(_) => todo!(),
        yara_x::ast::Expr::IEquals(_) => todo!(),
        yara_x::ast::Expr::Of(_) => todo!(),
        yara_x::ast::Expr::ForOf(_) => todo!(),
        yara_x::ast::Expr::ForIn(_) => todo!(),
    })
}


fn query_from_pattern(pattern: &Pattern) -> Result<Query> {
    todo!()
}
