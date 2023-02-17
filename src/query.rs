use std::fmt::Display;
use std::str::FromStr;

use itertools::Itertools;
use serde::{Serialize, Deserialize};
// use anyhow::Result;


#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug)]
pub enum Query {
    // Not(Box<Query>),
    And(Vec<Query>),
    Or(Vec<Query>),
    Literal(Vec<u8>),
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
        })
    }
}

mod parse_ursa {
    use nom::branch::alt;
    use nom::bytes::complete::{tag, is_not, is_a};
    use nom::character::complete::multispace0;
    use nom::combinator::opt;
    use nom::error::ParseError;
    use nom::multi::{many1};
    use nom::sequence::{delimited, tuple, preceded};
    use nom::{IResult};
    use super::Query;

    pub fn query(input: &str) -> anyhow::Result<Query> {
        let (remain, query) = match parse_query(input) {
            Ok(result) => result,
            Err(err) => return Err(anyhow::anyhow!("Could not parse access string: {err}")),
        };
        if !remain.is_empty() {
            return Err(anyhow::anyhow!("Could not parse access string trailing data: {remain}"))
        }
        return Ok(query)
    }

    fn ws<'a, F, O, E: ParseError<&'a str>>(inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
    where
    F: FnMut(&'a str) -> IResult<&'a str, O, E>,
    {
        delimited(
        multispace0,
        inner,
        multispace0
        )
    }

    fn parse_query<'a>(input: &'a str) -> IResult<&'a str, Query> {
        let (remain, query) = ws(parse_sequence)(input)?;
        return Ok((remain, query))
    }

    fn parse_brackets(input: &str) -> IResult<&str, Query> {
        let (remain, query) = delimited(ws(tag("(")), parse_query, ws(tag(")")))(input)?;
        return Ok((remain, query))
    }

    // fn parse_not(input: &str) -> IResult<&str, Query> {
    //     let (remain, query) = delimited(pair(ws(tag_no_case("not")), ws(tag("("))), parse_query, ws(tag(")")))(input)?;
    //     return Ok((remain, Query::Not(Box::new(query))))
    // }

    fn parse_hex(input: &str) -> IResult<&str, Query> {
        let (remain, value) = delimited(tag("{"), many1(is_a("0123456789abcdefABCDEF")), tag("}"))(input)?;
        let value = value.join("");
        let value = hex::decode(value).unwrap();
        return Ok((remain, Query::Literal(value)))
    }

    fn parse_string(input: &str) -> IResult<&str, Query> {
        let (remain, (_, value, _)) = ws(tuple((tag("\""), many1(alt((tag("\\\\"), tag("\\\""), is_not("\"")))), tag("\""))))(input)?;
        let literal = value.join("");
        return Ok((remain, Query::Literal(literal.into_bytes())));
    }

    fn parse_atom<'a>(input: &'a str) -> IResult<&'a str, Query> {
        let (remain, query) = delimited(multispace0, alt((parse_brackets, parse_hex, parse_string/* , parse_not*/)), multispace0)(input)?;
        return Ok((remain, query))
    }

    fn parse_sequence<'a>(input: &'a str) -> IResult<&'a str, Query> {
        let (remain, (query, ops)) = tuple((parse_atom, opt(alt((and_tail, or_tail)))))(input)?;
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
            },
            None => query,
        };
        return Ok((remain, query))
    }

    fn and_tail(input: &str) -> IResult<&str, Query> {
        let (remain, query) = many1(preceded(ws(tag("&")), parse_atom))(input)?;
        return Ok((remain, Query::And(query)))
    }

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