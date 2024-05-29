//! Definition and parsing for trigram filter queries

use std::fmt::Display;

use itertools::Itertools;
use serde::{Serialize, Deserialize};

/// An intermediate form where a query has been built from a yara
/// signature that describes the search in terms of phrases of literals
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug)]
pub enum PhraseQuery {
    /// Only include files that satisify all of the listed queries.
    And(Vec<PhraseQuery>),
    /// Include files if they satisify any of the listed queries.
    Or(Vec<PhraseQuery>),
    /// A literal binary blob we are looking for
    Literal(Vec<u8>),
    /// Include files only if they satisify enough of the listed queries.
    MinOf(i32, Vec<PhraseQuery>),
    /// A case insensitive literal blob
    /// all ascii characters in the blob should be treated as OR for upper and lowercase varients
    InsensitiveLiteral(Vec<u8>),
}

impl PhraseQuery {
    pub fn or(items: Vec<PhraseQuery>) -> Option<Self> {
        let mut produced = vec![];
        for item in items {
            if let PhraseQuery::Or(parts) = item {
                produced.extend(parts.into_iter());
            } else {
                produced.push(item);
            }
        }

        produced.sort_unstable();
        produced.dedup();
        if produced.len() <= 1 {
            return produced.pop()
        }
        Some(Self::Or(produced))
    }

    pub fn and(items: Vec<PhraseQuery>) -> Option<Self> {
        let mut produced = vec![];
        for item in items {
            if let PhraseQuery::And(parts) = item {
                produced.extend(parts.into_iter());
            } else {
                produced.push(item);
            }
        }

        produced.sort_unstable();
        produced.dedup();
        if produced.len() <= 1 {
            return produced.pop()
        }
        Some(Self::And(produced))
    }

    pub fn min_of(count: i32, mut items: Vec<PhraseQuery>) -> Option<Self> {
        if count == 1 {
            return Self::or(items);
        } else if count == items.len() as i32 {
            return Self::and(items);
        }

        items.sort_unstable();
        Some(Self::MinOf(count, items))
    }

    /// Transform all the literal sequences in a query
    pub fn map_literals<F>(&self, map: &F) -> Self
        where F: Fn(&[u8]) -> Vec<u8>
    {
        match self {
            PhraseQuery::And(items) => PhraseQuery::And(items.iter().map(|item| item.map_literals(map)).collect_vec()),
            PhraseQuery::Or(items) => PhraseQuery::Or(items.iter().map(|item| item.map_literals(map)).collect_vec()),
            PhraseQuery::Literal(bytes) => PhraseQuery::Literal(map(bytes)),
            PhraseQuery::MinOf(num, items) => PhraseQuery::MinOf(*num, items.iter().map(|item| item.map_literals(map)).collect_vec()),
            PhraseQuery::InsensitiveLiteral(bytes) => PhraseQuery::InsensitiveLiteral(map(bytes)),
        }
    }

    pub fn make_case_insensitive(&mut self) {
        match self {
            PhraseQuery::And(items) => for item in items {
                item.make_case_insensitive();
            },
            PhraseQuery::Or(items) => for item in items {
                item.make_case_insensitive();
            },
            PhraseQuery::Literal(value) => {
                let mut items = vec![];
                items.append(value);
                *self = PhraseQuery::InsensitiveLiteral(items);
            },
            PhraseQuery::MinOf(_, items) => for item in items {
                item.make_case_insensitive();
            },
            PhraseQuery::InsensitiveLiteral(_) => {},
        }
    }
}
// impl FromStr for PhraseQuery {
//     type Err = anyhow::Error;

//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         parse_ursa::query(s)
//     }
// }

impl Display for PhraseQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&match self {
            // Query::Not(sub) => format!("not({sub})"),
            PhraseQuery::And(sub) => format!("({})", sub.iter().join(" & ")),
            PhraseQuery::Or(sub) => format!("({})", sub.iter().join(" | ")),
            PhraseQuery::Literal(bytes) => format!("{{{}}}", hex::encode(bytes)),
            PhraseQuery::MinOf(num, expr) => format!("(min {num} of ({}))", expr.iter().map(|x|x.to_string()).join(",")),
            PhraseQuery::InsensitiveLiteral(bytes) => format!("{{{}}}/i", hex::encode(bytes)),
        })
    }
}

// /// Submodule containing the logic for deserilazing a query from the mquery format.
// /// A rough grammar for what the parser is doing is included as comments
// mod parse_ursa {
//     use nom::branch::alt;
//     use nom::bytes::complete::{tag, is_not, is_a};
//     use nom::character::complete::{multispace0, multispace1, digit1};
//     use nom::combinator::{opt, map_res};
//     use nom::error::ParseError;
//     use nom::multi::{many1, separated_list1};
//     use nom::sequence::{delimited, tuple, preceded};
//     use nom::IResult;
//     use super::PhraseQuery;

//     /// Load a query consuming the entire string or error
//     pub fn query(input: &str) -> anyhow::Result<PhraseQuery> {
//         let (remain, query) = match parse_query(input) {
//             Ok(result) => result,
//             Err(err) => return Err(anyhow::anyhow!("Could not parse query string: {err}")),
//         };
//         if !remain.is_empty() {
//             return Err(anyhow::anyhow!("Could not parse query string trailing data: {remain}"))
//         }
//         return Ok(query)
//     }

//     /// a wrapper to strip whitespace from before and after the combinator
//     fn ws<'a, F, O, E: ParseError<&'a str>>(inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
//     where
//     F: FnMut(&'a str) -> IResult<&'a str, O, E>,
//     {
//         delimited(multispace0, inner, multispace0)
//     }

//     /// parse_query: parse_sequence
//     fn parse_query(input: &str) -> IResult<&str, PhraseQuery> {
//         let (remain, query) = ws(parse_sequence)(input)?;
//         return Ok((remain, query))
//     }

//     /// parse_brackets: "(" (parse_of | parse_query) ")"
//     fn parse_brackets(input: &str) -> IResult<&str, PhraseQuery> {
//         let (remain, query) = delimited(ws(tag("(")), alt((parse_of, parse_query)), ws(tag(")")))(input)?;
//         return Ok((remain, query))
//     }

//     /// parse_of: "min" number "of" "(" parse_atom ("," parse_atom)* ")"
//     fn parse_of(input: &str) -> IResult<&str, PhraseQuery> {
//         let (remain, (_, _, _, num, _, _, items, _)) = tuple((multispace0, tag("min"), multispace1, map_res(digit1, |s: &str| s.parse::<i32>()), ws(tag("of")), ws(tag("(")), separated_list1(ws(tag(",")), parse_atom), ws(tag(")"))))(input)?;
//         return Ok((remain, PhraseQuery::MinOf(num, items)))
//     }

//     /// A hex value between {}
//     fn parse_hex(input: &str) -> IResult<&str, PhraseQuery> {
//         let (remain, value) = delimited(tag("{"), many1(is_a("0123456789abcdefABCDEF")), tag("}"))(input)?;
//         let value = value.join("");
//         let value = hex::decode(value).unwrap();
//         return Ok((remain, PhraseQuery::Literal(value)))
//     }

//     /// A quoted utf-8 string
//     fn parse_string(input: &str) -> IResult<&str, PhraseQuery> {
//         let (remain, (_, value, _)) = ws(tuple((tag("\""), many1(alt((tag("\\\\"), tag("\\\""), is_not("\"")))), tag("\""))))(input)?;
//         let literal = value.join("");
//         return Ok((remain, PhraseQuery::Literal(literal.into_bytes())));
//     }

//     /// parse_atom: parse_brackets | parse_hex | parse_string
//     fn parse_atom(input: &str) -> IResult<&str, PhraseQuery> {
//         let (remain, query) = ws(alt((parse_brackets, parse_hex, parse_string)))(input)?;
//         return Ok((remain, query))
//     }

//     /// parse_sequence: parse_atom [and_tail | or_tail]
//     fn parse_sequence(input: &str) -> IResult<&str, PhraseQuery> {
//         let (remain, (query, ops)) = tuple((parse_atom, opt(alt((and_tail, or_tail)))))(input)?;

//         // This should only match on operations that are written as suffixes
//         let query = match ops {
//             Some(parts) => match parts {
//                 // Query::Not(_) => panic!(),
//                 PhraseQuery::And(mut parts) => {
//                     parts.insert(0, query);
//                     PhraseQuery::And(parts)
//                 },
//                 PhraseQuery::Or(mut parts) => {
//                     parts.insert(0, query);
//                     PhraseQuery::Or(parts)
//                 },
//                 PhraseQuery::Literal(_) => panic!(),
//                 PhraseQuery::MinOf(_, _) => panic!(),
//                 PhraseQuery::InsensitiveLiteral(_) => panic!(),
//             },
//             None => query,
//         };
//         return Ok((remain, query))
//     }

//     /// and_tail: ("&" parse_atom)+
//     fn and_tail(input: &str) -> IResult<&str, PhraseQuery> {
//         let (remain, query) = many1(preceded(ws(tag("&")), parse_atom))(input)?;
//         return Ok((remain, PhraseQuery::And(query)))
//     }

//     /// or_tail: ("|" parse_atom)+
//     fn or_tail(input: &str) -> IResult<&str, PhraseQuery> {
//         let (remain, query) = many1(preceded(ws(tag("|")), parse_atom))(input)?;
//         return Ok((remain, PhraseQuery::Or(query)))
//     }
// }


// #[cfg(test)]
// mod test {
//     use std::str::FromStr;

//     use super::PhraseQuery;

//     #[test]
//     fn parsing() {
//         assert_eq!(PhraseQuery::from_str(r#""xyz""#).unwrap(), PhraseQuery::Literal(b"xyz".to_vec()));

//         assert_eq!(PhraseQuery::from_str(r#""xyz" & "www""#).unwrap(), PhraseQuery::And(vec![
//             PhraseQuery::Literal(b"xyz".to_vec()),
//             PhraseQuery::Literal(b"www".to_vec()),
//         ]));

//         assert_eq!(PhraseQuery::from_str(r#"({112233} | "xxx") & "hmm""#).unwrap(), PhraseQuery::And(vec![
//             PhraseQuery::Or(vec![
//                 PhraseQuery::Literal(vec![17, 34, 51]),
//                 PhraseQuery::Literal(b"xxx".to_vec()),
//             ]),
//             PhraseQuery::Literal(b"hmm".to_vec()),
//         ]));
//     }

//     #[test]
//     fn parse_of() {
//         assert_eq!(PhraseQuery::from_str("(min 1 of ({737472696e67}))").unwrap(), PhraseQuery::MinOf(1, vec![PhraseQuery::Literal(vec![0x73, 0x74, 0x72, 0x69, 0x6e, 0x67])]));
//         assert_eq!(PhraseQuery::from_str("(min 1 of ({73747269}, {6e67}))").unwrap(), PhraseQuery::MinOf(1, vec![PhraseQuery::Literal(vec![0x73, 0x74, 0x72, 0x69]), PhraseQuery::Literal(vec![0x6e, 0x67])]));
//     }

//     #[test]
//     fn parsing_whitespace() {
//         let xyz = PhraseQuery::Literal(b"xyz".to_vec());
//         assert_eq!(PhraseQuery::from_str(r#""xyz""#).unwrap(), xyz);
//         assert_eq!(PhraseQuery::from_str(r#" "xyz""#).unwrap(), xyz);
//         assert_eq!(PhraseQuery::from_str(r#""xyz" "#).unwrap(), xyz);
//         assert_eq!(PhraseQuery::from_str(r#"  "xyz""#).unwrap(), xyz);
//         assert_eq!(PhraseQuery::from_str(r#""xyz"  "#).unwrap(), xyz);
//         assert_eq!(PhraseQuery::from_str(r#"   "xyz" "#).unwrap(), xyz);


//         let abc = PhraseQuery::Literal(b"abc".to_vec());
//         let compound = PhraseQuery::And(vec![xyz.clone(), abc.clone()]);
//         assert_eq!(PhraseQuery::from_str(r#""xyz"&"abc""#).unwrap(), compound);
//         assert_eq!(PhraseQuery::from_str(r#""xyz" &"abc""#).unwrap(), compound);
//         assert_eq!(PhraseQuery::from_str(r#""xyz"& "abc""#).unwrap(), compound);
//         assert_eq!(PhraseQuery::from_str(r#""xyz" & "abc" "#).unwrap(), compound);
//         assert_eq!(PhraseQuery::from_str(r#""xyz"  &  "abc" "#).unwrap(), compound);

//         let compound = PhraseQuery::And(vec![xyz.clone(), abc, xyz]);
//         assert_eq!(PhraseQuery::from_str(r#""xyz"&"abc"&"xyz""#).unwrap(), compound);
//         assert_eq!(PhraseQuery::from_str(r#""xyz" &"abc"&"xyz""#).unwrap(), compound);
//         assert_eq!(PhraseQuery::from_str(r#""xyz"& "abc"& "xyz""#).unwrap(), compound);
//         assert_eq!(PhraseQuery::from_str(r#""xyz" & "abc" &"xyz""#).unwrap(), compound);
//         assert_eq!(PhraseQuery::from_str(r#""xyz"  &  "abc" &   "xyz""#).unwrap(), compound);

//     }
// }