use std::collections::HashSet;

use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug)]
pub enum AccessControl {
    Or(Vec<AccessControl>),
    And(Vec<AccessControl>),
    Token(String)
}

impl AccessControl {
    pub fn or(&self, other: &AccessControl) -> AccessControl {
        let mut collected = vec![];
        match self {
            AccessControl::Or(values) => collected.extend(values.clone()),
            AccessControl::And(values) => collected.push(AccessControl::And(values.clone())),
            AccessControl::Token(token) => collected.push(AccessControl::Token(token.clone())),
        }
        match other {
            AccessControl::Or(values) => collected.extend(values.clone()),
            AccessControl::And(values) => collected.push(AccessControl::And(values.clone())),
            AccessControl::Token(token) => collected.push(AccessControl::Token(token.clone())),
        }

        return AccessControl::Or(collected).simplify()
    }

    pub fn and(&self, other: &AccessControl) -> AccessControl {
        let mut collected = vec![];
        match self {
            AccessControl::Or(values) => collected.push(AccessControl::Or(values.clone())),
            AccessControl::And(values) => collected.extend(values.clone()),
            AccessControl::Token(token) => collected.push(AccessControl::Token(token.clone())),
        }
        match other {
            AccessControl::Or(values) => collected.push(AccessControl::Or(values.clone())),
            AccessControl::And(values) => collected.extend(values.clone()),
            AccessControl::Token(token) => collected.push(AccessControl::Token(token.clone())),
        }

        return AccessControl::And(collected).simplify();
    }

    pub fn from_and(mut items: Vec<AccessControl>) -> AccessControl {
        assert!(items.len() > 0);
        items.sort();
        items.dedup();
        if items.len() == 1 {
            return items.pop().unwrap()
        }
        return AccessControl::And(items);
    }

    pub fn from_or(mut items: Vec<AccessControl>) -> AccessControl {
        assert!(items.len() > 0);
        items.sort();
        items.dedup();
        if items.len() == 1 {
            return items.pop().unwrap()
        }
        return AccessControl::Or(items);
    }

    pub fn simplify(self) -> AccessControl {
        match self {
            AccessControl::Or(items) => {
                let mut items: Vec<AccessControl> = items.into_iter().map(|x|x.simplify()).collect();

                let mut operands: Vec<HashSet<AccessControl>> = vec![];
                while let Some(item) = items.pop() {
                    match item {
                        AccessControl::Or(parts) => items.extend(parts),
                        AccessControl::And(parts) => operands.push(HashSet::from_iter(parts)),
                        AccessControl::Token(value) => operands.push(HashSet::from([AccessControl::Token(value)])),
                    }
                }

                assert!(!operands.is_empty());
                let common = operands.iter().cloned().reduce(|a, b| HashSet::from_iter(a.intersection(&b).cloned())).unwrap();
                if common.len() > 0 {
                    let mut unfactored = vec![];

                    for op in operands {
                        let mut x = Vec::from_iter(op.difference(&common).cloned());
                        x.sort();
                        x.dedup();
                        if x.len() > 0 {
                            unfactored.push(x.clone())
                        }
                    }

                    unfactored.sort();
                    unfactored.dedup();

                    let common = AccessControl::from_or(common.clone().into_iter().collect());
                    let mut unfactored: Vec<AccessControl> = unfactored.into_iter().map(|x|AccessControl::from_and(x)).collect();

                    if unfactored.len() == 0 {
                        return common;
                    } else if unfactored.len() == 1 {
                        return AccessControl::from_and(vec![unfactored.pop().unwrap(), common])
                    } else {
                        let unfactored = AccessControl::from_or(unfactored);
                        return AccessControl::from_and(vec![unfactored, common])
                    }
                } else {
                    return AccessControl::from_or(operands.into_iter().map(|x|AccessControl::from_and(x.into_iter().collect())).collect());
                }
            },
            AccessControl::And(items) => {
                let mut items: Vec<AccessControl> = items.into_iter().map(|x|x.simplify()).collect();

                let mut operands: Vec<HashSet<AccessControl>> = vec![];
                while let Some(item) = items.pop() {
                    match item {
                        AccessControl::And(parts) => items.extend(parts),
                        AccessControl::Or(parts) => operands.push(HashSet::from_iter(parts)),
                        AccessControl::Token(value) => operands.push(HashSet::from([AccessControl::Token(value)])),
                    }
                }

                assert!(!operands.is_empty());
                let common = operands.iter().cloned().reduce(|a, b| HashSet::from_iter(a.intersection(&b).cloned())).unwrap();
                if common.len() > 0 {
                    let mut unfactored = vec![];

                    for op in operands {
                        let mut x = Vec::from_iter(op.difference(&common).cloned());
                        x.sort();
                        x.dedup();
                        if x.len() > 0 {
                            unfactored.push(x.clone())
                        }
                    }

                    unfactored.sort();
                    unfactored.dedup();

                    let common = AccessControl::from_and(common.clone().into_iter().collect());
                    let mut unfactored: Vec<AccessControl> = unfactored.into_iter().map(|x|AccessControl::from_or(x)).collect();

                    if unfactored.len() == 0 {
                        return common;
                    } else if unfactored.len() == 1 {
                        return AccessControl::from_or(vec![unfactored.pop().unwrap(), common])
                    } else {
                        let unfactored = AccessControl::from_and(unfactored);
                        return AccessControl::from_or(vec![unfactored, common])
                    }
                } else {
                    return AccessControl::from_and(operands.into_iter().map(|x|AccessControl::from_and(x.into_iter().collect())).collect());
                }
            },
            AccessControl::Token(token) => return AccessControl::Token(token),
        };
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
    fn simplify() {
        let a = AccessControl::Token("A".to_owned());
        assert_eq!(a, a.clone().simplify());

        let b = AccessControl::Token("B".to_owned());
        let a_or_b = AccessControl::Or(vec![a.clone(), b.clone()]);
        assert_eq!(a_or_b, a_or_b.clone().simplify());

        let a_and_b = AccessControl::And(vec![a.clone(), b.clone()]);
        assert_eq!(a_and_b, a_and_b.clone().simplify());

        assert_ne!(a, b);
        assert_ne!(a, a_or_b);
        assert_ne!(a_and_b, a_or_b);

        let a_or_a = AccessControl::Or(vec![a.clone(), a.clone()]);
        assert_eq!(a, a_or_a.simplify());

    }
}