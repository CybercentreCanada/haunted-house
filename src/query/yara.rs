//!
//! Functions to extract filter queries from yara signatures
//!

use base64::Engine;
use boreal_parser::expression::Expression;
use boreal_parser::file::YaraFileComponent;
use boreal_parser::rule::VariableDeclaration;
use itertools::Itertools;

use crate::error::{Result, ErrorKinds};
use crate::query::phrases::PhraseQuery as Query;


/// Struct used internal to this module to capture queries and warnings together
pub struct ParsedQuery {
    /// The query a given expression branch has created (or not)
    pub query: Option<Query>,
    /// Warnings generated by unsupported features in a branch.
    /// Note that warnings can be generated without a query
    pub warnings: Vec<String>,
}

impl From<Query> for ParsedQuery {
    fn from(query: Query) -> Self {
        Self { query: Some(query), warnings: Default::default() }
    }
}

impl From<Option<Query>> for ParsedQuery {
    fn from(query: Option<Query>) -> Self {
        Self { query, warnings: Default::default() }
    }
}

impl From<String> for ParsedQuery {
    fn from(warning: String) -> Self {
        Self { query: None, warnings: vec![warning] }
    }
}

impl From<&str> for ParsedQuery {
    fn from(warning: &str) -> Self {
        Self { query: None, warnings: vec![warning.to_owned()] }
    }
}

/// Convert a yara signature into a filter query
pub fn yara_signature_to_phrase_query(signature: &str) -> Result<(Query, Vec<String>)> {
    let file = boreal_parser::parse(signature)?;

    let mut signature = None;
    for component in file.components {
        if let YaraFileComponent::Rule(rule) = component{
            if signature.is_none() {
                signature = Some(rule);
            } else {
                return Err(ErrorKinds::YaraRuleError("Only one rule at a time supported".to_string()))
            }
        }
    }

    let signature = if let Some(rule) = signature {
        rule
    } else {
        return Err(ErrorKinds::YaraRuleError("At least one rule must be provided".to_string()))
    };

    let mut res = parse_expression(&signature.condition, &signature.variables, None)?;
    res.warnings.sort_unstable();
    res.warnings.dedup();
    match res.query {
        Some(query) => Ok((query, res.warnings)),
        None => Err(ErrorKinds::YaraRuleError("Could not build filter from rule".to_string())),
    }
}

/// take a declared variable and build a set of byte queries from it
fn variable_to_query(var: &VariableDeclaration) -> Result<ParsedQuery> {
    if var.modifiers.private {
        return Ok("Private strings not used in filtering.".into())
    }

    use boreal_parser::rule::VariableDeclarationValue;
    let mut patterns: ParsedQuery = match &var.value {
        VariableDeclarationValue::Bytes(bytes) => Query::Literal(bytes.clone()).into(),
        VariableDeclarationValue::Regex(regex) => parse_regex(regex)?,
        VariableDeclarationValue::HexString(hex) => parse_strings(hex, 3)?,
    };

    // Apply nocase and add a warning
    if var.modifiers.nocase {
        if let Some(query) = &mut patterns.query {
            query.make_case_insensitive();
        }
    }

    // interleave null in string
    if var.modifiers.wide {
        if let Some(query) = patterns.query {
            let wide = query.map_literals(&null_pad);

            // include ascii version
            patterns.query = Some(if var.modifiers.ascii {
                Query::or(vec![wide, query]).unwrap()
            } else {
                wide
            });
        }
    }

    // Xor modifier, providing the range of byte values to xor
    if let Some((min, max)) = var.modifiers.xor {
        if let Some(query) = patterns.query {
            let mut parts = vec![];

            for xor in min..=max {
                parts.push(query.map_literals(&|bytes| xor_mask(bytes, xor)));
            }

            // include ascii version
            patterns.query = Query::or(parts);
        }
    };

    // Base64 modifier.
    if let Some(rules) = &var.modifiers.base64 {
        if let Some(query) = &mut patterns.query {
            let alphabet = if let Some(alphabet) = rules.alphabet {
                alphabet
            } else {
                *b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
            };

            *query = Query::or(vec![
                query.map_literals(&|bytes|base64_encode(0, bytes, alphabet)),
                query.map_literals(&|bytes|base64_encode(1, bytes, alphabet)),
                query.map_literals(&|bytes|base64_encode(2, bytes, alphabet)),
            ]).unwrap();

            if rules.wide {
                let wide = query.map_literals(&null_pad);

                // include ascii version
                *query = if rules.ascii {
                    Query::or(vec![wide, query.clone()]).unwrap()
                } else {
                    wide
                };
            }
        }
    }

    Ok(patterns)
}

/// lossy base64 encode based on windows of data with the ends truncated to be consistent with the original content
fn base64_encode(permute: u8, data: &[u8], alphabet: [u8;64]) -> Vec<u8> {
    let alphabet = base64::alphabet::Alphabet::new(&String::from_utf8(alphabet.to_vec()).unwrap()).unwrap();
    let engine = base64::engine::general_purpose::GeneralPurpose::new(&alphabet, base64::engine::general_purpose::PAD);

    let mut buffer = match permute {
        0 => vec![],
        1 => vec![0],
        _ => vec![0, 0]
    };
    buffer.extend(data);
    let mut output = engine.encode(buffer);

    if output.ends_with('=') {
        while output.ends_with('=') { output.pop(); }
        output.pop();
    }

    match permute {
        0 => &output[0..],
        1 => &output[2..],
        _ => &output[3..],
    }.as_bytes().to_vec()
}

// /// Produce an ascii lowercase version of a query
// fn to_lowercase(values: &[u8]) -> Vec<u8> {
//     let mut data = vec![];
//     for byte in values {
//         data.push(byte.to_ascii_lowercase())
//     }
//     data
// }

// /// Produce an ascii uppercase version of a query
// fn to_uppercase(values: &[u8]) -> Vec<u8> {
//     let mut data = vec![];
//     for byte in values {
//         data.push(byte.to_ascii_uppercase())
//     }
//     data
// }

/// Add null padding to every byte in the give byte sequence
fn null_pad(values: &[u8]) -> Vec<u8> {
    let mut data = vec![];
    for byte in values {
        data.push(*byte);
        data.push(0);
    }
    data
}

/// Add null padding to every byte in the give byte sequence
fn xor_mask(values: &[u8], mask: u8) -> Vec<u8> {
    let mut data = vec![];
    for byte in values {
        data.push(*byte ^ mask);
    }
    data
}

/// break a hex string into its understandable parts
fn produce_strings(values: &Vec<boreal_parser::hex_string::Token>, limit: usize) -> Result<Vec<Vec<u8>>> {
    use boreal_parser::hex_string::Token;
    let mut strings = vec![];
    let mut current = vec![vec![]];

    for token in values {
        match token {
            Token::Byte(byte) => {
                for item in current.iter_mut() {
                    item.push(*byte)
                }
            },
            Token::NotByte(_) |
            Token::MaskedByte(_, _) |
            Token::NotMaskedByte(_, _) |
            Token::Jump(_) => {
                for path in current {
                    if path.len() >= limit {
                        strings.push(path);
                    }
                }
                current = vec![vec![]];
            }
            Token::Alternatives(sequences) => {
                let mut produced = vec![];
                for sequence in sequences {
                    for tail in produce_strings(sequence, 1)? {
                        for mut head in current.clone() {
                            head.extend(tail.clone());
                            produced.push(head);
                        }
                    }
                }
                if produced.is_empty() {
                    for path in current {
                        if path.len() >= limit {
                            strings.push(path);
                        }
                    }
                    current = vec![vec![]];
                } else {
                    current = produced;
                }
            },
        }
    }

    for path in current {
        if path.len() >= limit {
            strings.push(path);
        }
    }
    Ok(strings)
}

/// given a hex string with potential wildcards, try to produce a reasonable number of variants
fn parse_strings(values: &Vec<boreal_parser::hex_string::Token>, limit: usize) -> Result<ParsedQuery> {
    let mut strings = produce_strings(values, limit)?;
    if strings.is_empty() {
        Ok(None.into())
    } else if strings.len() == 1 {
        Ok(Query::Literal(strings.pop().unwrap()).into())
    } else {
        Ok(Query::or(strings.into_iter().map(Query::Literal).collect()).into())
    }
}

/// generate strings from regex
fn produce_regex(node: &boreal_parser::regex::Node, limit: usize) -> Vec<(bool, Vec<u8>, bool)> {
    use boreal_parser::regex::Node;
    match node {
        Node::Alternation(paths) => {
            let mut collected: Vec<(bool, Vec<u8>, bool)> = vec![];
            for path in paths {
                for (start, segment, trailing) in produce_regex(path, 0) {
                    if segment.len() >= limit {
                        collected.push((start, segment, trailing));
                    }
                }
            }
            collected
        },
        // if we are interested in zero length segments, produce one that starts AFTER here but is empty
        Node::Class(_) |
        Node::Dot => if limit == 0 {
            vec![(false, vec![], true)]
        } else {
            vec![]
        },
        // if we are interested in zero length segments, produce one that starts HERE but is empty
        Node::Empty |
        Node::Assertion(_) => if limit == 0 {
            vec![(true, vec![], true)]
        } else {
            vec![]
        },
        Node::Concat(nodes) => {
            let mut complete = vec![];
            let mut trailing = vec![(true, vec![])];

            for node in nodes {
                let initial_trailing = trailing;
                let mut produced_trailing = vec![];
                for (start, data, finish) in produce_regex(node, 0) {
                    match (start, finish) {
                        (true, true) => {
                            for (start, mut prefix) in initial_trailing.clone() {
                                prefix.extend(data.clone());
                                produced_trailing.push((start, prefix));
                            }
                        },
                        (true, false) => {
                            for (start, mut prefix) in initial_trailing.clone() {
                                prefix.extend(data.clone());
                                if prefix.len() >= limit {
                                    complete.push((start, prefix, false));
                                }
                            }
                        },
                        (false, true) => {
                            produced_trailing.push((false, data));
                        },
                        (false, false) => {
                            if data.len() >= limit {
                                complete.push((false, data, false));
                            }
                        }
                    }
                    if !start {
                        for (start, prefix) in initial_trailing.clone() {
                            if prefix.len() >= limit {
                                complete.push((start, prefix, false));
                            }
                        }
                    }
                }
                if produced_trailing.is_empty() {
                    trailing = vec![(false, vec![])]
                } else {
                    trailing = produced_trailing;
                }
            }

            for (start, data) in trailing {
                if data.len() >= limit {
                    complete.push((start, data, true))
                }
            }

            complete.sort_unstable();
            complete.dedup();
            complete
        },
        Node::Literal(content) => if limit <= 1 {
            vec![(true, vec![content.byte], true)]
        } else {
            vec![]
        },
        Node::Char(content) => if limit <= 1 {
            vec![(true, content.c.to_string().as_bytes().to_vec(), true)]
        } else {
            vec![]
        },
        Node::Group(node) => produce_regex(node, limit),
        Node::Repetition { node, kind, .. } => {
            let mut paths = vec![];
            if limit == 0 {
                use boreal_parser::regex::{RepetitionKind::{*}, RepetitionRange};
                match kind {
                    ZeroOrOne => { paths.push((true, vec![], true)); },
                    ZeroOrMore => { paths.push((true, vec![], true)); },
                    OneOrMore => {},
                    Range(range) => {
                        if let RepetitionRange::Bounded(a, _) = range {
                            if *a == 0 {
                                paths.push((true, vec![], true));
                            }
                        }
                    },
                }
            }

            for (start, branch, finish) in produce_regex(node, 0) {
                if branch.len() > limit {
                    paths.push((start, branch.clone(), finish));
                    paths.push((false, branch.clone(), finish));
                    paths.push((start, branch.clone(), false));
                    paths.push((false, branch, false));
                }
            }

            paths.sort_unstable();
            paths.dedup();
            paths
        },
    }
}

/// Turn a regex into a filter query where possible
fn parse_regex(expr: &boreal_parser::regex::Regex) -> Result<ParsedQuery> {
    // Get the literal segments from the regex
    let literals = produce_regex(&expr.ast, 3);

    // transform them into query parts
    let parts = if expr.case_insensitive {
        literals.into_iter().map(|(_, x, _)|Query::InsensitiveLiteral(x)).collect_vec()
    } else {
        literals.into_iter().map(|(_, x, _)|Query::Literal(x)).collect_vec()
    };

    // Or over those parts (or use the single value if only one)
    Ok(ParsedQuery { query: Query::or(parts), warnings: vec![] })
}

/// Combine the output of branches that are both relevant to the search
fn combine_or(a: ParsedQuery, b: ParsedQuery) -> Result<ParsedQuery> {
    let mut warnings = a.warnings;
    warnings.extend(b.warnings);

    let a = match a.query {
        Some(query) => query,
        None => return Ok(ParsedQuery { query: b.query, warnings }),
    };

    let b = match b.query {
        Some(query) => query,
        None => return Ok(ParsedQuery { query: Some(a), warnings }),
    };

    Ok(ParsedQuery { query: Query::or(vec![a, b]), warnings })
}

/// select some variables
fn select_variables(set: &boreal_parser::expression::VariableSet, strings: &[VariableDeclaration]) -> Vec<VariableDeclaration> {
    if set.elements.is_empty() {
        return strings.to_vec()
    }

    let mut selected = vec![];
    for item in &set.elements {
        if item.is_wildcard {
            for entry in strings {
                if entry.name.starts_with(&item.name) {
                    selected.push(entry.clone())
                }
            }
        } else {
            for entry in strings {
                if entry.name == item.name {
                    selected.push(entry.clone())
                }
            }
        }
    }
    selected
}

/// Based on the condition figure out which strings need to be considered
pub (crate) fn parse_expression(expr: &Expression, strings: &[VariableDeclaration], scope_variable: Option<&VariableDeclaration>) -> Result<ParsedQuery> {
    use boreal_parser::expression::ExpressionKind;
    match &expr.expr {
        ExpressionKind::Filesize => Ok(None.into()),
        ExpressionKind::Entrypoint => Ok(None.into()),
        ExpressionKind::ReadInteger { .. } => Ok(None.into()),
        ExpressionKind::Integer(_) => Ok(None.into()),
        ExpressionKind::Double(_) => Ok(None.into()),
        ExpressionKind::Count(target) |
        ExpressionKind::CountInRange { variable_name: target, .. } |
        ExpressionKind::Offset { variable_name: target, .. } |
        ExpressionKind::Length { variable_name: target, .. } |
        ExpressionKind::Variable(target) |
        ExpressionKind::VariableAt { variable_name: target, .. } |
        ExpressionKind::VariableIn { variable_name: target, .. } => {
            let var = if target.is_empty() {
                scope_variable
            } else {
                strings.iter().find(|var|var.name == *target)
            };

            match var {
                Some(var) => variable_to_query(var),
                None => Err(ErrorKinds::YaraRuleError(format!("Variable not found: {target}")))
            }
        },
        ExpressionKind::Matches(expr, _) |
        ExpressionKind::BitwiseNot(expr) |
        ExpressionKind::Not(expr) |
        ExpressionKind::Neg(expr) |
        ExpressionKind::Defined(expr) => {
            parse_expression(expr, strings, scope_variable)
        },
        ExpressionKind::Add(a, b) |
        ExpressionKind::Sub(a, b) |
        ExpressionKind::Mul(a, b) |
        ExpressionKind::Div(a, b) |
        ExpressionKind::Mod(a, b) |
        ExpressionKind::BitwiseXor(a, b) |
        ExpressionKind::BitwiseAnd(a, b) |
        ExpressionKind::BitwiseOr(a, b) |
        ExpressionKind::ShiftLeft(a, b) |
        ExpressionKind::ShiftRight(a, b) |
        ExpressionKind::Cmp { left: a, right: b, .. } |
        ExpressionKind::Eq(a, b) |
        ExpressionKind::NotEq(a, b) => {
            combine_or(parse_expression(a, strings, scope_variable)?, parse_expression(b, strings, scope_variable)?)
        },
        ExpressionKind::And(expr) => {
            let mut parts = vec![];
            let mut collected_warnings = vec![];
            for expr in expr {
                let ParsedQuery{query, warnings} = parse_expression(expr, strings, scope_variable)?;
                collected_warnings.extend(warnings);
                if let Some(part) = query {
                    parts.push(part);
                }
            }
            Ok(ParsedQuery { query: Query::and(parts), warnings: collected_warnings })
        },
        ExpressionKind::Or(expr) => {
            let mut parts = vec![];
            let mut collected_warnings = vec![];
            for expr in expr {
                let ParsedQuery{query, warnings} = parse_expression(expr, strings, scope_variable)?;
                collected_warnings.extend(warnings);
                if let Some(part) = query {
                    parts.push(part);
                }
            }
            Ok(ParsedQuery { query: Query::or(parts), warnings: collected_warnings })
        },
        ExpressionKind::Boolean(_) => Ok(None.into()),
        ExpressionKind::Bytes(data) => Ok(Query::Literal(data.clone()).into()),
        ExpressionKind::Identifier(_) => Ok(None.into()),
        ExpressionKind::Regex(regex) => parse_regex(regex),

        ExpressionKind::Contains { haystack: a, needle: b, case_insensitive } |
        ExpressionKind::StartsWith { expr: a, prefix: b, case_insensitive } |
        ExpressionKind::EndsWith { expr: a, suffix: b, case_insensitive } => {
            let mut out = combine_or(parse_expression(a, strings, scope_variable)?, parse_expression(b, strings, scope_variable)?)?;
            if *case_insensitive {
                if let Some(query) = &mut out.query {
                    query.make_case_insensitive();
                }
            }
            Ok(out)
        },

        ExpressionKind::IEquals(a, b) => {
            let mut out = combine_or(parse_expression(a, strings, scope_variable)?, parse_expression(b, strings, scope_variable)?)?;
            if let Some(query) = &mut out.query {
                query.make_case_insensitive();
            }
            Ok(out)
        },

        ExpressionKind::For { selection, set, body } => {
            let variables = select_variables(set, strings);
            let mut warnings = vec![];
            let mut bodies = vec![];
            match body {
                Some(body) => {
                    for var in variables {
                        let ParsedQuery { query, warnings: w } = parse_expression(body, strings, Some(&var))?;
                        if let Some(query) = query {
                            bodies.push(query);
                        }
                        warnings.extend(w);
                    }
                },
                None => {
                    for var in variables {
                        let ParsedQuery { query, warnings: w } = variable_to_query(&var)?;
                        if let Some(query) = query {
                            bodies.push(query);
                        }
                        warnings.extend(w);
                    }
                }
            };

            if bodies.is_empty() {
                return Ok(ParsedQuery { query: None, warnings })
            }

            match selection {
                boreal_parser::expression::ForSelection::Any => Ok(ParsedQuery { query: Query::or(bodies), warnings }),
                boreal_parser::expression::ForSelection::All => Ok(ParsedQuery { query: Query::and(bodies), warnings }),
                boreal_parser::expression::ForSelection::None => Ok(None.into()),
                boreal_parser::expression::ForSelection::Expr { expr, as_percent } => {
                    let count = parse_expression_as_count(expr, *as_percent, bodies.len())?;
                    Ok(ParsedQuery { query: Query::min_of(count, bodies), warnings })
                },
            }
        },

        ExpressionKind::ForIn { selection, set, .. } |
        ExpressionKind::ForAt { selection, set, .. } => {
            let variables = select_variables(set, strings);
            let mut warnings = vec![];
            let mut bodies = vec![];
            for var in variables {
                let ParsedQuery { query, warnings: w } = variable_to_query(&var)?;
                if let Some(query) = query {
                    bodies.push(query);
                }
                warnings.extend(w);
            }

            if bodies.is_empty() {
                return Ok(ParsedQuery { query: None, warnings })
            }

            match selection {
                boreal_parser::expression::ForSelection::Any => Ok(ParsedQuery { query: Query::or(bodies), warnings }),
                boreal_parser::expression::ForSelection::All => Ok(ParsedQuery { query: Query::and(bodies), warnings }),
                boreal_parser::expression::ForSelection::None => Ok(None.into()),
                boreal_parser::expression::ForSelection::Expr { expr, as_percent } => {
                    let count = parse_expression_as_count(expr, *as_percent, bodies.len())?;
                    Ok(ParsedQuery { query: Query::min_of(count, bodies), warnings })
                },
            }
        },
        ExpressionKind::ForRules { .. } =>
            Err(ErrorKinds::YaraRuleError("References to other yara rules not supported.".to_owned())),
        ExpressionKind::ForIdentifiers { .. } =>
            Err(ErrorKinds::YaraRuleError("Unsupported loop used. Please share this rule for more information.".to_owned())),
    }
}

/// resolve a number where one is expected in the yara signature
fn parse_expression_as_count(expr: &Expression, percent: bool, size: usize) -> Result<i32> {
    use boreal_parser::expression::ExpressionKind;
    match &expr.expr {
        ExpressionKind::Integer(value) => if percent {
            Ok(((*value as f64 / 100.0) * size as f64).ceil() as i32)
        } else {
            Ok(*value as i32)
        },
        ExpressionKind::Double(value) => if percent {
            Ok(((*value / 100.0) * size as f64).ceil() as i32)
        } else {
            Ok(value.ceil() as i32)
        },
        _ => Err(ErrorKinds::YaraRuleError(format!("Could not resolve count in loop: {expr:?}")))
    }
}


#[cfg(test)]
mod test {
    use crate::query::yara::yara_signature_to_phrase_query;

    use super::Query;
    use super::produce_regex;

    #[test]
    fn simple_string() {
        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule test_sig {
                strings:
                    $first = "Content_Types"

                condition:
                    $first
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::Literal(b"Content_Types".to_vec()));
    }

    #[test]
    fn wide_string() {
        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule test_sig {
                strings:
                    $first = "Content_Types" wide

                condition:
                    #first > 1
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::Literal(b"C\0o\0n\0t\0e\0n\0t\0_\0T\0y\0p\0e\0s\0".to_vec()));

        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule test_sig {
                strings:
                    $first = "Content_Types" wide ascii

                condition:
                    #first > 1
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::or(vec![
            Query::Literal(b"C\0o\0n\0t\0e\0n\0t\0_\0T\0y\0p\0e\0s\0".to_vec()),
            Query::Literal(b"Content_Types".to_vec()),
        ]).unwrap());
    }

    #[test]
    fn wildcards() {
        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule test_sig {
                strings:
                    $first = {43 6f 6e ?? 65 6e 74}
                    $second = {43 6f 6e 74 ?? 6e 74}

                condition:
                    $first or $second
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::or(vec![
            Query::Literal(b"Con".to_vec()),
            Query::Literal(b"Cont".to_vec()),
            Query::Literal(b"ent".to_vec()),
        ]).unwrap());
    }

    #[test]
    fn alternation() {

        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule test_sig {
                strings:
                    $first = {43 6f 6e (74 | 75) 65 6e 74}

                condition:
                    $first
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::or(vec![
            Query::Literal(b"Content".to_vec()),
            Query::Literal(b"Conuent".to_vec()),
        ]).unwrap());

        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule test_sig {
                strings:
                    $first = {43 ((6f | 6f 6e) | 74 65 6e 74 | 44 ??)}

                condition:
                    $first
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::or(vec![
            Query::Literal(b"Con".to_vec()),
            Query::Literal(b"Ctent".to_vec()),
        ]).unwrap());
    }

    #[test]
    fn xor() {
        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule test_sig {
                strings:
                    $first = "\x00\x10\xff" xor(0x00-0x03)

                condition:
                    $first
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::or(vec![
            Query::Literal(vec![0x00, 0x10, 0xff]),
            Query::Literal(vec![0x01, 0x11, 0xfe]),
            Query::Literal(vec![0x02, 0x12, 0xfd]),
            Query::Literal(vec![0x03, 0x13, 0xfc]),
        ]).unwrap());
    }

    #[test]
    fn base64() {
        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule Base64Example1 {
                strings:
                    $a = "This program cannot" base64

                condition:
                    $a
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::or(vec![
            Query::Literal(b"RoaXMgcHJvZ3JhbSBjYW5ub3".to_vec()),
            Query::Literal(b"UaGlzIHByb2dyYW0gY2Fubm90".to_vec()),
            Query::Literal(b"VGhpcyBwcm9ncmFtIGNhbm5vd".to_vec()),
        ]).unwrap());
    }

    #[test]
    fn regex_components() {
        assert_eq!(
            produce_regex(&boreal_parser::regex::parse_regex("/in|inp|input/").unwrap().ast, 3),
            vec![(true, b"inp".to_vec(), true), (true, b"input".to_vec(), true)]
        );
        assert_eq!(
            produce_regex(&boreal_parser::regex::parse_regex("/(n|.|)/").unwrap().ast, 0),
            vec![(true, b"n".to_vec(), true), (false, b"".to_vec(), true), (true, b"".to_vec(), true)]
        );
        assert_eq!(
            produce_regex(&boreal_parser::regex::parse_regex("/abc(n|.|)xyz/").unwrap().ast, 3),
            vec![(false, b"xyz".to_vec(), true), (true, b"abc".to_vec(), false), (true, b"abcnxyz".to_vec(), true), (true, b"abcxyz".to_vec(), true)]
        );
    }

    #[test]
    fn for_body() {
        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule Base64Example1 {
                strings:
                    $a1 = "1111"
                    $a2 = "2222"
                    $b1 = "3333"
                    $x = "xx"

                condition:
                    for any of ($a*, $b1) : ( $ and $x )
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::or(vec![
            Query::and(vec![
                Query::Literal(b"1111".to_vec()),
                Query::Literal(b"xx".to_vec()),
            ]).unwrap(),
            Query::and(vec![
                Query::Literal(b"2222".to_vec()),
                Query::Literal(b"xx".to_vec()),
            ]).unwrap(),
            Query::and(vec![
                Query::Literal(b"3333".to_vec()),
                Query::Literal(b"xx".to_vec()),
            ]).unwrap(),
        ]).unwrap());
    }

    #[test]
    fn n_of() {
        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule Base64Example1 {
                strings:
                    $a1 = "1111"
                    $a2 = "2222"
                    $b1 = "3333"
                    $x = "xx"

                condition:
                    51.5% of them
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::min_of(3, vec![
            Query::Literal(b"1111".to_vec()),
            Query::Literal(b"2222".to_vec()),
            Query::Literal(b"3333".to_vec()),
            Query::Literal(b"xx".to_vec()),
        ]).unwrap());

        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule Base64Example1 {
                strings:
                    $a1 = "1111"
                    $a2 = "2222"
                    $b1 = "3333"
                    $x = "xx"

                condition:
                    2 of them
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::min_of(2, vec![
            Query::Literal(b"1111".to_vec()),
            Query::Literal(b"2222".to_vec()),
            Query::Literal(b"3333".to_vec()),
            Query::Literal(b"xx".to_vec()),
        ]).unwrap());
    }

    #[test]
    fn for_at() {
        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule Base64Example1 {
                strings:
                    $a1 = "1111"
                    $a2 = "2222"
                    $b1 = "3333"
                    $x = "xx"

                condition:
                    2 of them at 0
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::min_of(2, vec![
            Query::Literal(b"1111".to_vec()),
            Query::Literal(b"2222".to_vec()),
            Query::Literal(b"3333".to_vec()),
            Query::Literal(b"xx".to_vec()),
        ]).unwrap());
    }

    #[test]
    fn test_open_source_rule() {
        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule mysql_database_presence
            {
                meta:
                    author="CYB3RMX"
                    description="This rule checks MySQL database presence"

                strings:
                    $db = "MySql.Data"
                    $db1 = "MySqlCommand"
                    $db2 = "MySqlConnection"
                    $db3 = "MySqlDataReader"
                    $db4 = "MySql.Data.MySqlClient"

                condition:
                    (any of ($db*))
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::or(vec![
            Query::Literal(b"MySql.Data".to_vec()),
            Query::Literal(b"MySql.Data.MySqlClient".to_vec()),
            Query::Literal(b"MySqlCommand".to_vec()),
            Query::Literal(b"MySqlConnection".to_vec()),
            Query::Literal(b"MySqlDataReader".to_vec()),
        ]).unwrap());

        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule Str_Win32_Wininet_Library
            {

                meta:
                    author = "@adricnet"
                    description = "Match Windows Inet API library declaration"
                    method = "String match"
                    reference = "https://github.com/dfirnotes/rules"

                strings:
                    $wininet_lib = "WININET.dll" nocase

                condition:
                    (all of ($wininet*))
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::InsensitiveLiteral(b"WININET.dll".to_vec()));

        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule win_files_operation {
                meta:
                    author = "x0r"
                    description = "Affect private profile"
                version = "0.1"
                strings:
                    $f1 = "kernel32.dll" nocase
                    $c1 = "WriteFile"
                    $c2 = "SetFilePointer"
                    $c3 = "WriteFile"
                    $c4 = "ReadFile"
                    $c5 = "DeleteFileA"
                    $c6 = "CreateFileA"
                    $c7 = "FindFirstFileA"
                    $c8 = "MoveFileExA"
                    $c9 = "FindClose"
                    $c10 = "SetFileAttributesA"
                    $c11 = "CopyFile"

                condition:
                    $f1 and 3 of ($c*)
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::and(vec![
            Query::InsensitiveLiteral(b"kernel32.dll".to_vec()),
            Query::min_of(3, vec![
                Query::Literal(b"CopyFile".to_vec()),
                Query::Literal(b"CreateFileA".to_vec()),
                Query::Literal(b"DeleteFileA".to_vec()),
                Query::Literal(b"FindClose".to_vec()),
                Query::Literal(b"FindFirstFileA".to_vec()),
                Query::Literal(b"MoveFileExA".to_vec()),
                Query::Literal(b"ReadFile".to_vec()),
                Query::Literal(b"SetFileAttributesA".to_vec()),
                Query::Literal(b"SetFilePointer".to_vec()),
                Query::Literal(b"WriteFile".to_vec()),
                Query::Literal(b"WriteFile".to_vec()),
            ]).unwrap()
        ]).unwrap());

        let (query, warnings) = yara_signature_to_phrase_query(r#"
            rule screenshot {
                meta:
                    author = "x0r"
                    description = "Take screenshot"
                version = "0.1"
                strings:
                    $d1 = "Gdi32.dll" nocase
                    $d2 = "User32.dll" nocase
                    $c1 = "BitBlt"
                    $c2 = "GetDC"
                condition:
                    1 of ($d*) and 1 of ($c*)
            }
        "#).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(query, Query::and(vec![
            Query::or(vec![
                Query::Literal(b"BitBlt".to_vec()),
                Query::Literal(b"GetDC".to_vec()),
            ]).unwrap(),
            Query::or(vec![
                Query::InsensitiveLiteral(b"Gdi32.dll".to_vec()),
                Query::InsensitiveLiteral(b"User32.dll".to_vec()),
            ]).unwrap(),
        ]).unwrap());
    }

}