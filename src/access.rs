//!
//! Tool for handing data access control restrictions.
//!
use std::collections::HashSet;
use std::fmt::Display;
use std::str::FromStr;

use serde::{Serialize, Deserialize};
use anyhow::Result;

use crate::error::ErrorKinds;

/// An object describing who can access a given item
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug)]
pub enum AccessControl {
    /// Allow access when any of the sub-controls allow access
    Or(Vec<AccessControl>),
    /// Allow access only when all of the sub-controls allow access
    And(Vec<AccessControl>),
    /// Allow access when the user has this token in their permissions
    Token(String),
    /// Always allow access to the given resource
    Always,
}

/// An object describing what assets a user can access
pub type ViewControl = HashSet<String>;

impl Display for AccessControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccessControl::Or(items) => f.write_fmt(format_args!("Or({})",
                items.iter().map(AccessControl::to_string).collect::<Vec<String>>().join(", "))),
            AccessControl::And(items) => f.write_fmt(format_args!("And({})",
                items.iter().map(|x|x.to_string()).collect::<Vec<String>>().join(", "))),
            AccessControl::Token(value) => f.write_fmt(format_args!("\"{value}\"")),
            AccessControl::Always => f.write_str("Always"),
        }
    }
}

impl FromStr for AccessControl {
    type Err = ErrorKinds;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse::access(s)
    }
}

impl core::ops::BitAnd<AccessControl> for &AccessControl {
    type Output = AccessControl;

    fn bitand(self, rhs: AccessControl) -> Self::Output {
        self.and(&rhs)
    }
}

impl core::ops::BitAnd<&AccessControl> for &AccessControl {
    type Output = AccessControl;

    fn bitand(self, rhs: &AccessControl) -> Self::Output {
        self.and(rhs)
    }
}

impl core::ops::BitAnd<AccessControl> for AccessControl {
    type Output = AccessControl;

    fn bitand(self, rhs: AccessControl) -> Self::Output {
        self.and(&rhs)
    }
}

impl core::ops::BitAnd<&AccessControl> for AccessControl {
    type Output = AccessControl;

    fn bitand(self, rhs: &AccessControl) -> Self::Output {
        self.and(rhs)
    }
}

impl core::ops::BitOr<&AccessControl> for &AccessControl {
    type Output = AccessControl;

    fn bitor(self, rhs: &AccessControl) -> Self::Output {
        self.or(rhs)
    }
}

impl core::ops::BitOr<AccessControl> for &AccessControl {
    type Output = AccessControl;

    fn bitor(self, rhs: AccessControl) -> Self::Output {
        self.or(&rhs)
    }
}

impl core::ops::BitOr<AccessControl> for AccessControl {
    type Output = AccessControl;

    fn bitor(self, rhs: AccessControl) -> Self::Output {
        self.or(&rhs)
    }
}

impl core::ops::BitOr<&AccessControl> for AccessControl {
    type Output = AccessControl;

    fn bitor(self, rhs: &AccessControl) -> Self::Output {
        self.or(rhs)
    }
}

impl AccessControl {
    /// Combine this access controls with another, joined by an 'or' operation.
    pub fn or(&self, other: &AccessControl) -> AccessControl {
        let mut collected = vec![];
        match self {
            AccessControl::Or(values) => collected.extend(values.clone()),
            AccessControl::And(values) => collected.push(AccessControl::And(values.clone())),
            AccessControl::Token(token) => collected.push(AccessControl::Token(token.clone())),
            AccessControl::Always => return AccessControl::Always,
        }
        match other {
            AccessControl::Or(values) => collected.extend(values.clone()),
            AccessControl::And(values) => collected.push(AccessControl::And(values.clone())),
            AccessControl::Token(token) => collected.push(AccessControl::Token(token.clone())),
            AccessControl::Always => return AccessControl::Always,
        }

        collected.sort();
        collected.dedup();

        if collected.is_empty() {
            AccessControl::Always
        } else if collected.len() == 1 {
            collected.pop().unwrap()
        } else {
            AccessControl::Or(collected)
        }
    }

    /// Combine this access controls with another, joined by an 'and' operation.
    pub fn and(&self, other: &AccessControl) -> AccessControl {
        let mut collected = vec![];
        match self {
            AccessControl::Or(values) => collected.push(AccessControl::Or(values.clone())),
            AccessControl::And(values) => collected.extend(values.clone()),
            AccessControl::Token(token) => collected.push(AccessControl::Token(token.clone())),
            AccessControl::Always => {},
        }
        match other {
            AccessControl::Or(values) => collected.push(AccessControl::Or(values.clone())),
            AccessControl::And(values) => collected.extend(values.clone()),
            AccessControl::Token(token) => collected.push(AccessControl::Token(token.clone())),
            AccessControl::Always => {},
        }

        collected.sort();
        collected.dedup();

        if collected.len() == 1 {
            return collected.pop().unwrap();
        }
        return AccessControl::And(collected);
    }

    /// Combine a set of access controls under an 'and' operation
    fn set_into_and(items: HashSet<AccessControl>) -> AccessControl {
        return AccessControl::into_and(items.into_iter().collect())
    }

    /// Combine a list of access controls under an 'and' operation
    fn into_and(mut items: Vec<AccessControl>) -> AccessControl {
        assert!(!items.is_empty());
        items.sort();
        items.dedup();
        if items.len() == 1 {
            return items.pop().unwrap()
        }
        return AccessControl::And(items);
    }

    /// Combine a list of access controls under an 'or' operation
    pub fn into_or(mut items: Vec<AccessControl>) -> AccessControl {
        assert!(!items.is_empty());
        items.sort();
        items.dedup();
        if items.len() == 1 {
            return items.pop().unwrap()
        }
        return AccessControl::Or(items);
    }

    /// Simplify the set of access control objects by factoring out common terms.
    pub fn factor(items: Vec<HashSet<AccessControl>>) -> (HashSet<AccessControl>, Vec<Vec<AccessControl>>, bool) {
        let common = items.iter().cloned().reduce(|a, b| HashSet::from_iter(a.intersection(&b).cloned())).unwrap();

        let mut unfactored = vec![];
        let mut has_fallthrough = false;

        for op in items {
            let mut x = Vec::from_iter(op.difference(&common).cloned());
            x.sort();
            x.dedup();
            if !x.is_empty() {
                unfactored.push(x)
            } else {
                has_fallthrough = true;
            }
        }

        unfactored.sort();
        unfactored.dedup();

        return (common, unfactored, has_fallthrough)
    }

    /// Try to simplify the access control object through some simple and greedy transforms.
    pub fn simplify(self) -> AccessControl {
        match self {
            AccessControl::Or(items) => {
                let mut items: Vec<AccessControl> = items.into_iter().map(|x|x.simplify()).collect();
                if items.is_empty() {
                    return AccessControl::Always;
                }

                let mut operands: Vec<HashSet<AccessControl>> = vec![];
                while let Some(item) = items.pop() {
                    match item {
                        AccessControl::Or(parts) => items.extend(parts),
                        AccessControl::And(parts) => operands.push(HashSet::from_iter(parts)),
                        AccessControl::Token(value) => operands.push(HashSet::from([AccessControl::Token(value)])),
                        AccessControl::Always => return AccessControl::Always,
                    }
                }

                let (common, unfactored, fallthrough) = AccessControl::factor(operands);
                if fallthrough {
                    return AccessControl::set_into_and(common);
                }

                let mut unfactored: Vec<AccessControl> = unfactored.into_iter().map(AccessControl::into_and).collect();

                if common.is_empty() {
                    return AccessControl::into_or(unfactored);
                }

                let common = AccessControl::set_into_and(common);

                if unfactored.is_empty() {
                    return common;
                } else if unfactored.len() == 1 {
                    return unfactored.pop().unwrap() | common;
                } else {
                    let unfactored = AccessControl::into_or(unfactored);
                    let temp = unfactored.and(&common);
                    return temp;
                }
            },
            AccessControl::And(items) => {
                let mut items: Vec<AccessControl> = items.into_iter().map(|x|x.simplify()).collect();
                if items.is_empty() {
                    return AccessControl::Always;
                }

                let mut operands: Vec<HashSet<AccessControl>> = vec![];
                while let Some(item) = items.pop() {
                    match item {
                        AccessControl::And(parts) => items.extend(parts),
                        AccessControl::Or(parts) => operands.push(HashSet::from_iter(parts)),
                        AccessControl::Token(value) => operands.push(HashSet::from([AccessControl::Token(value)])),
                        AccessControl::Always => {},
                    }
                }

                let (common, unfactored, _) = AccessControl::factor(operands);

                let mut unfactored: Vec<AccessControl> = unfactored.into_iter().map(AccessControl::into_or).collect();

                if common.is_empty() {
                    return AccessControl::into_and(unfactored);
                }
                let common = AccessControl::into_or(common.into_iter().collect());

                if unfactored.is_empty() {
                    return common;
                } else if unfactored.len() == 1 {
                    return unfactored.pop().unwrap() & common;
                } else {
                    let unfactored = AccessControl::into_and(unfactored);
                    return unfactored & common;
                }
            },
            AccessControl::Token(token) => return AccessControl::Token(token),
            AccessControl::Always => return AccessControl::Always,
        };
    }

    /// Check whether a given viewer can access the controlled data
    pub fn can_access(&self, fields: &ViewControl) -> bool {
        match self {
            AccessControl::Or(sub) => {
                for ac in sub {
                    if ac.can_access(fields) {
                        return true;
                    }
                }
                false
            },
            AccessControl::And(sub) => {
                for ac in sub {
                    if !ac.can_access(fields) {
                        return false;
                    }
                }
                true
            },
            AccessControl::Token(token) => fields.contains(token),
            AccessControl::Always => true,
        }
    }
}

/// Parse access control information from a string
mod parse {

    use nom::branch::alt;
    use nom::bytes::complete::{tag_no_case, tag, is_not};
    use nom::character::complete::multispace0;
    use nom::multi::{separated_list1, many1};
    use nom::sequence::{delimited, tuple};
    use nom::{IResult};
    use crate::error::ErrorKinds;

    use super::AccessControl;

    /// Root parse function, consumes all input
    pub fn access(input: &str) -> Result<AccessControl, ErrorKinds> {
        let (remain, access) = match parse_access(input) {
            Ok(result) => result,
            Err(err) => return Err(ErrorKinds::CouldNotParseAccessString(input.to_owned(), err.to_string())),
        };
        if !remain.is_empty() {
            return Err(ErrorKinds::CouldNotParseAccessStringTrailing(input.to_owned(), remain.to_owned()))
        }
        return Ok(access)
    }

    /// parse_access: parse_literal | parse_and | parse_or | parse_always
    fn parse_access(input: &str) -> IResult<&str, AccessControl> {
        let (remain, access) = delimited(multispace0, alt((parse_literal, parse_and, parse_or, parse_always)), multispace0)(input)?;
        return Ok((remain, access))
    }

    /// parse_and: "and" "(" parse_access ("," parse_access)* ")"
    fn parse_and(input: &str) -> IResult<&str, AccessControl> {
        let (remain, (_, _, _, mut inner, _)) = tuple((tag_no_case("and"), multispace0, tag("("), separated_list1(tag(","), parse_access), tag(")")))(input)?;
        inner.sort();
        return Ok((remain, AccessControl::And(inner)))
    }

    /// parse_or: "or" "(" parse_access ("," parse_access)* ")"
    fn parse_or(input: &str) -> IResult<&str, AccessControl> {
        let (remain, (_, _, _, mut inner, _)) = tuple((tag_no_case("or"), multispace0, tag("("), separated_list1(tag(","), parse_access), tag(")")))(input)?;
        inner.sort();
        return Ok((remain, AccessControl::Or(inner)))
    }

    /// A quoted alphanumeric access token
    fn parse_literal(input: &str) -> IResult<&str, AccessControl> {
        let (remain, (_, value, _)) = tuple((tag("\""), many1(alt((tag("\\\\"), tag("\\\""), is_not("\"")))), tag("\"")))(input)?;
        let literal = value.join("");
        return Ok((remain, AccessControl::Token(literal)));
    }

    /// A keyword to always allow access (not quoted, won't be confused with parse_literal)
    fn parse_always(input: &str) -> IResult<&str, AccessControl> {
        let (remain, _) = tag_no_case("always")(input)?;
        return Ok((remain, AccessControl::Always))
    }
}

// S//T//REL:A
// TS//T//REL:A

// (S & T & REL:A) | (TS & T & REL:A)

// (S & T & REL:A) | (TS & T & REL:A)

// X = T & REL:A
// (S & X) | (TS & X)
// X & (S | TS)

// T & REL:A & (S | TS)

// (T & REL:A & (S | TS)) | (U & T & REL:A)

// T & REL:A & (S | TS | U)


#[cfg(test)]
mod test {
    use super::AccessControl;

    #[test]
    fn basic_and() {
        let a = &AccessControl::Token("A".to_owned());
        let b = &AccessControl::Token("B".to_owned());
        let c = &AccessControl::Token("C".to_owned());
        let d = &AccessControl::Token("D".to_owned());

        assert_eq!(a & b, AccessControl::And(vec![a.clone(), b.clone()]));
        assert_eq!(b & a, AccessControl::And(vec![a.clone(), b.clone()]));
        assert_eq!(a & a, *a);
        assert_eq!(a & (a & a), *a);
        assert_eq!(a & (b & a), AccessControl::And(vec![a.clone(), b.clone()]));
        assert_eq!((a & b & c) & (a & c & d), AccessControl::And(vec![a.clone(), b.clone(), c.clone(), d.clone()]));
        assert_eq!(a & (b & (c & d)), AccessControl::And(vec![a.clone(), b.clone(), c.clone(), d.clone()]));
    }

    #[test]
    fn basic_or() {
        let a = &AccessControl::Token("A".to_owned());
        let b = &AccessControl::Token("B".to_owned());
        let c = &AccessControl::Token("C".to_owned());
        let d = &AccessControl::Token("D".to_owned());

        assert_eq!(a | b, AccessControl::Or(vec![a.clone(), b.clone()]));
        assert_eq!(b | a, AccessControl::Or(vec![a.clone(), b.clone()]));
        assert_eq!(a | a, *a);
        assert_eq!(a | (a | a), *a);
        assert_eq!(a | (b | a), AccessControl::Or(vec![a.clone(), b.clone()]));
        assert_eq!((a | b | c) | (a | c | d), AccessControl::Or(vec![a.clone(), b.clone(), c.clone(), d.clone()]));
        assert_eq!(a | (b | (c | d)), AccessControl::Or(vec![a.clone(), b.clone(), c.clone(), d.clone()]));
    }

    #[test]
    fn simplify() {
        let a = &AccessControl::Token("A".to_owned());
        let b = &AccessControl::Token("B".to_owned());
        let c = &AccessControl::Token("C".to_owned());
        // let d = &AccessControl::Token("D".to_owned());

        assert_eq!(a, &a.clone().simplify());

        let a_or_b = a | b;
        assert_eq!(a_or_b, a_or_b.clone().simplify());

        let a_and_b = a & b;
        let a_and_c = a & c;
        assert_eq!(a_and_b, a_and_b.clone().simplify());

        assert_ne!(a, b);
        assert_ne!(a, &a_or_b);
        assert_ne!(a_and_b, a_or_b);

        let a_or_a = AccessControl::Or(vec![a.clone(), a.clone()]);
        assert_eq!(a, &a_or_a.simplify());

        let expr = AccessControl::And(vec![AccessControl::Or(vec![a.clone(), b.clone()]), a.clone()]);
        assert_eq!(expr.simplify(), a_and_b);

        let expr = AccessControl::Or(vec![AccessControl::And(vec![a.clone(), b.clone()]), a.clone()]);
        assert_eq!(&expr.simplify(), a);

        let expr = AccessControl::Or(vec![AccessControl::And(vec![a.clone(), b.clone(), c.clone()]), AccessControl::And(vec![a.clone(), c.clone()])]);
        assert_eq!(expr.simplify(), a_and_c);

        let expr = AccessControl::And(vec![a_or_b.clone(), AccessControl::Or(vec![b.clone(), a.clone()])]);
        assert_eq!(expr.simplify(), a_or_b);

        let expr = AccessControl::And(vec![
            AccessControl::Or(vec![a.clone(), AccessControl::Or(vec![b.clone(), a.clone()])]),
            (b | a)
        ]);
        assert_eq!(expr.simplify(), a_or_b);

        // let expr = (&(&a & &b) & &c) | (&a & &(&b & &d));
        // assert_eq!(expr.simplify(), AccessControl::And(vec![a.clone(), b.clone(), AccessControl::Or(vec![c.clone(), d.clone()])]));
    }

    use super::parse::access;

    #[test]
    fn parse() {
        let a = &AccessControl::Token("A".to_owned());
        let b = &AccessControl::Token("B".to_owned());
        let c = &AccessControl::Token("C".to_owned());
        let x = &AccessControl::Token("X".to_owned());
        let y = &AccessControl::Token("Y".to_owned());

        assert_eq!(access(r#"and("A","B", "C" , "X"   )   "#).unwrap(), a & b & c & x);
        assert_eq!(access(r#"and (  "A", "C" , "X"   )"#).unwrap(), a & c & x);
        assert_eq!(access(r#"and (  "A", "C" , OR("X", "Y")   )"#).unwrap(), a & c & (x | y));
    }
}