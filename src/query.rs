use std::fmt::Display;
use std::str::FromStr;

use itertools::Itertools;
use serde::{Serialize, Deserialize};

/// A query that can be run against the trigram filters.
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug)]
pub enum Query {
    /// Only include files that satisify all of the listed queries.
    And(Vec<Query>),
    /// Include files if they satisify any of the listed queries.
    Or(Vec<Query>),
    /// A literal binary blob we are looking for
    Literal(Vec<u8>),
    /// Include files only if they satisify enough of the listed queries.
    MinOf(i32, Vec<Query>)
}

impl FromStr for Query {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_ursa::query(s)
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&match self {
            // Query::Not(sub) => format!("not({sub})"),
            Query::And(sub) => format!("({})", sub.iter().join(" & ")),
            Query::Or(sub) => format!("({})", sub.iter().join(" | ")),
            Query::Literal(bytes) => format!("{{{}}}", hex::encode(bytes)),
            Query::MinOf(num, expr) => format!("(min {num} of ({}))", expr.iter().map(|x|x.to_string()).join(","))
        })
    }
}

/// Submodule containing the logic for deserilazing a query from the mquery format.
/// A rough grammar for what the parser is doing is included as comments
mod parse_ursa {
    use nom::branch::alt;
    use nom::bytes::complete::{tag, is_not, is_a};
    use nom::character::complete::{multispace0, multispace1, digit1};
    use nom::combinator::{opt, map_res};
    use nom::error::ParseError;
    use nom::multi::{many1, separated_list1};
    use nom::sequence::{delimited, tuple, preceded};
    use nom::IResult;
    use super::Query;

    /// Load a query consuming the entire string or error
    pub fn query(input: &str) -> anyhow::Result<Query> {
        let (remain, query) = match parse_query(input) {
            Ok(result) => result,
            Err(err) => return Err(anyhow::anyhow!("Could not parse query string: {err}")),
        };
        if !remain.is_empty() {
            return Err(anyhow::anyhow!("Could not parse query string trailing data: {remain}"))
        }
        return Ok(query)
    }

    /// a wrapper to strip whitespace from before and after the combinator
    fn ws<'a, F, O, E: ParseError<&'a str>>(inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
    where
    F: FnMut(&'a str) -> IResult<&'a str, O, E>,
    {
        delimited(multispace0, inner, multispace0)
    }

    /// parse_query: parse_sequence
    fn parse_query(input: &str) -> IResult<&str, Query> {
        let (remain, query) = ws(parse_sequence)(input)?;
        return Ok((remain, query))
    }

    /// parse_brackets: "(" (parse_of | parse_query) ")"
    fn parse_brackets(input: &str) -> IResult<&str, Query> {
        let (remain, query) = delimited(ws(tag("(")), alt((parse_of, parse_query)), ws(tag(")")))(input)?;
        return Ok((remain, query))
    }

    /// parse_of: "min" number "of" "(" parse_atom ("," parse_atom)* ")"
    fn parse_of(input: &str) -> IResult<&str, Query> {
        let (remain, (_, _, _, num, _, _, items, _)) = tuple((multispace0, tag("min"), multispace1, map_res(digit1, |s: &str| s.parse::<i32>()), ws(tag("of")), ws(tag("(")), separated_list1(ws(tag(",")), parse_atom), ws(tag(")"))))(input)?;
        return Ok((remain, Query::MinOf(num, items)))
    }

    /// A hex value between {}
    fn parse_hex(input: &str) -> IResult<&str, Query> {
        let (remain, value) = delimited(tag("{"), many1(is_a("0123456789abcdefABCDEF")), tag("}"))(input)?;
        let value = value.join("");
        let value = hex::decode(value).unwrap();
        return Ok((remain, Query::Literal(value)))
    }

    /// A quoted utf-8 string
    fn parse_string(input: &str) -> IResult<&str, Query> {
        let (remain, (_, value, _)) = ws(tuple((tag("\""), many1(alt((tag("\\\\"), tag("\\\""), is_not("\"")))), tag("\""))))(input)?;
        let literal = value.join("");
        return Ok((remain, Query::Literal(literal.into_bytes())));
    }

    /// parse_atom: parse_brackets | parse_hex | parse_string
    fn parse_atom(input: &str) -> IResult<&str, Query> {
        let (remain, query) = ws(alt((parse_brackets, parse_hex, parse_string)))(input)?;
        return Ok((remain, query))
    }

    /// parse_sequence: parse_atom [and_tail | or_tail]
    fn parse_sequence(input: &str) -> IResult<&str, Query> {
        let (remain, (query, ops)) = tuple((parse_atom, opt(alt((and_tail, or_tail)))))(input)?;

        // This should only match on operations that are written as suffixes
        let query = match ops {
            Some(parts) => match parts {
                // Query::Not(_) => panic!(),
                Query::And(mut parts) => {
                    parts.insert(0, query);
                    Query::And(parts)
                },
                Query::Or(mut parts) => {
                    parts.insert(0, query);
                    Query::Or(parts)
                },
                Query::Literal(_) => panic!(),
                Query::MinOf(_, _) => panic!(),
            },
            None => query,
        };
        return Ok((remain, query))
    }

    /// and_tail: ("&" parse_atom)+
    fn and_tail(input: &str) -> IResult<&str, Query> {
        let (remain, query) = many1(preceded(ws(tag("&")), parse_atom))(input)?;
        return Ok((remain, Query::And(query)))
    }

    /// or_tail: ("|" parse_atom)+
    fn or_tail(input: &str) -> IResult<&str, Query> {
        let (remain, query) = many1(preceded(ws(tag("|")), parse_atom))(input)?;
        return Ok((remain, Query::Or(query)))
    }
}


#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::Query;

    #[test]
    fn parsing() {
        assert_eq!(Query::from_str(r#""xyz""#).unwrap(), Query::Literal(b"xyz".to_vec()));

        assert_eq!(Query::from_str(r#""xyz" & "www""#).unwrap(), Query::And(vec![
            Query::Literal(b"xyz".to_vec()),
            Query::Literal(b"www".to_vec()),
        ]));

        assert_eq!(Query::from_str(r#"({112233} | "xxx") & "hmm""#).unwrap(), Query::And(vec![
            Query::Or(vec![
                Query::Literal(vec![17, 34, 51]),
                Query::Literal(b"xxx".to_vec()),
            ]),
            Query::Literal(b"hmm".to_vec()),
        ]));
    }

    #[test]
    fn parse_of() {
        assert_eq!(Query::from_str("(min 1 of ({737472696e67}))").unwrap(), Query::MinOf(1, vec![Query::Literal(vec![0x73, 0x74, 0x72, 0x69, 0x6e, 0x67])]));
        assert_eq!(Query::from_str("(min 1 of ({73747269}, {6e67}))").unwrap(), Query::MinOf(1, vec![Query::Literal(vec![0x73, 0x74, 0x72, 0x69]), Query::Literal(vec![0x6e, 0x67])]));
    }

    #[test]
    fn parsing_whitespace() {
        let xyz = Query::Literal(b"xyz".to_vec());
        assert_eq!(Query::from_str(r#""xyz""#).unwrap(), xyz);
        assert_eq!(Query::from_str(r#" "xyz""#).unwrap(), xyz);
        assert_eq!(Query::from_str(r#""xyz" "#).unwrap(), xyz);
        assert_eq!(Query::from_str(r#"  "xyz""#).unwrap(), xyz);
        assert_eq!(Query::from_str(r#""xyz"  "#).unwrap(), xyz);
        assert_eq!(Query::from_str(r#"   "xyz" "#).unwrap(), xyz);


        let abc = Query::Literal(b"abc".to_vec());
        let compound = Query::And(vec![xyz.clone(), abc.clone()]);
        assert_eq!(Query::from_str(r#""xyz"&"abc""#).unwrap(), compound);
        assert_eq!(Query::from_str(r#""xyz" &"abc""#).unwrap(), compound);
        assert_eq!(Query::from_str(r#""xyz"& "abc""#).unwrap(), compound);
        assert_eq!(Query::from_str(r#""xyz" & "abc" "#).unwrap(), compound);
        assert_eq!(Query::from_str(r#""xyz"  &  "abc" "#).unwrap(), compound);

        let compound = Query::And(vec![xyz.clone(), abc, xyz]);
        assert_eq!(Query::from_str(r#""xyz"&"abc"&"xyz""#).unwrap(), compound);
        assert_eq!(Query::from_str(r#""xyz" &"abc"&"xyz""#).unwrap(), compound);
        assert_eq!(Query::from_str(r#""xyz"& "abc"& "xyz""#).unwrap(), compound);
        assert_eq!(Query::from_str(r#""xyz" & "abc" &"xyz""#).unwrap(), compound);
        assert_eq!(Query::from_str(r#""xyz"  &  "abc" &   "xyz""#).unwrap(), compound);

    }
}