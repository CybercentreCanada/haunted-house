use std::collections::HashSet;

use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug)]
pub enum AccessControl {
    Or(Vec<AccessControl>),
    And(Vec<AccessControl>),
    Token(String),
    Always,
    // Never,
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
        self.or(&rhs)
    }
}

impl AccessControl {
    pub fn or(&self, other: &AccessControl) -> AccessControl {
        let mut collected = vec![];
        match self {
            AccessControl::Or(values) => collected.extend(values.clone()),
            AccessControl::And(values) => collected.push(AccessControl::And(values.clone())),
            AccessControl::Token(token) => collected.push(AccessControl::Token(token.clone())),
            AccessControl::Always => return AccessControl::Always,
            // AccessControl::Never => {},
        }
        match other {
            AccessControl::Or(values) => collected.extend(values.clone()),
            AccessControl::And(values) => collected.push(AccessControl::And(values.clone())),
            AccessControl::Token(token) => collected.push(AccessControl::Token(token.clone())),
            AccessControl::Always => return AccessControl::Always,
            // AccessControl::Never => {},
        }

        collected.sort();
        collected.dedup();

        if collected.len() == 0 {
            AccessControl::Always
        } else if collected.len() == 1 {
            collected.pop().unwrap()
        } else {
            AccessControl::Or(collected)
        }
    }

    pub fn and(&self, other: &AccessControl) -> AccessControl {
        let mut collected = vec![];
        match self {
            AccessControl::Or(values) => collected.push(AccessControl::Or(values.clone())),
            AccessControl::And(values) => collected.extend(values.clone()),
            AccessControl::Token(token) => collected.push(AccessControl::Token(token.clone())),
            AccessControl::Always => {},
            // AccessControl::Never => return AccessControl::Never,
        }
        match other {
            AccessControl::Or(values) => collected.push(AccessControl::Or(values.clone())),
            AccessControl::And(values) => collected.extend(values.clone()),
            AccessControl::Token(token) => collected.push(AccessControl::Token(token.clone())),
            AccessControl::Always => {},
            // AccessControl::Never => return AccessControl::Never,
        }

        collected.sort();
        collected.dedup();

        if collected.len() == 1 {
            return collected.pop().unwrap();
        }
        return AccessControl::And(collected);
    }

    fn set_into_and(items: HashSet<AccessControl>) -> AccessControl {
        return AccessControl::into_and(items.into_iter().collect())
    }

    fn into_and(mut items: Vec<AccessControl>) -> AccessControl {
        assert!(items.len() > 0);
        items.sort();
        items.dedup();
        if items.len() == 1 {
            return items.pop().unwrap()
        }
        return AccessControl::And(items);
    }

    pub fn into_or(mut items: Vec<AccessControl>) -> AccessControl {
        assert!(items.len() > 0);
        items.sort();
        items.dedup();
        if items.len() == 1 {
            return items.pop().unwrap()
        }
        return AccessControl::Or(items);
    }

    pub fn factor(items: Vec<HashSet<AccessControl>>) -> (HashSet<AccessControl>, Vec<Vec<AccessControl>>, bool) {
        let common = items.iter().cloned().reduce(|a, b| HashSet::from_iter(a.intersection(&b).cloned())).unwrap();

        let mut unfactored = vec![];
        let mut has_fallthrough = false;

        for op in items {
            let mut x = Vec::from_iter(op.difference(&common).cloned());
            x.sort();
            x.dedup();
            if x.len() > 0 {
                unfactored.push(x)
            } else {
                has_fallthrough = true;
            }
        }

        unfactored.sort();
        unfactored.dedup();

        return (common, unfactored, has_fallthrough)
    }

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
        
                if common.len() == 0 {
                    return AccessControl::into_or(unfactored);
                }

                let common = AccessControl::set_into_and(common);

                if unfactored.len() == 0 {
                    return common;
                } else if unfactored.len() == 1 {
                    return unfactored.pop().unwrap() | common
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

                if common.len() == 0 {
                    return AccessControl::into_and(unfactored);
                }
                let common = AccessControl::into_or(common.clone().into_iter().collect());

                if unfactored.len() == 0 {
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

    pub fn parse(data: &str, and_split: &str, or_split: &str) -> AccessControl {
        let mut anded = vec![];
        for segment in data.split(and_split) {
            let mut ored = vec![];
            for segment in segment.split(or_split) {
                let segment = segment.trim();
                if segment.len() > 0 {
                    ored.push(AccessControl::Token(segment.to_owned()));
                }
            }
            if ored.len() > 0 {
                anded.push(AccessControl::into_or(ored));
            }
        }
        if anded.len() > 0 {
            return AccessControl::into_and(anded);
        }
        return AccessControl::Always;
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

    #[test]
    fn parse() {
        let a = &AccessControl::Token("A".to_owned());
        let b = &AccessControl::Token("B".to_owned());
        let c = &AccessControl::Token("C".to_owned());
        let x = &AccessControl::Token("Rel:X".to_owned());
        let y = &AccessControl::Token("Rel:Y".to_owned());

        assert_eq!(AccessControl::parse("A//B//C/Rel:X", "/", ","), a & b & c & x);
        assert_eq!(AccessControl::parse("A//C/Rel:X", "/", ","), a & c & x);
        assert_eq!(AccessControl::parse("A//B/Rel:X,Rel:Y", "/", ","), a & b & (x | y));
    }
}