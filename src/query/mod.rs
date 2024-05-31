pub mod phrases;
mod yara;
pub mod broadcast;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use self::phrases::PhraseQuery;

pub fn parse_yara_signature(signature: &str) -> Result<(TrigramQuery, Vec<String>)> {
    let (query, warnings) = self::yara::yara_signature_to_phrase_query(signature)?;
    Ok((TrigramQuery::build(query), warnings))
}


#[derive(Serialize, Deserialize)]
#[derive(PartialEq, PartialOrd, Eq, Ord, Hash, Clone, Copy)]
pub enum Reference {
    Trigram(u32),
    Expression(usize),
}

impl Reference {
    pub fn from_array(data: [u8; 3]) -> Self {
        Self::Trigram(((data[0] as u32) << 16) | ((data[1] as u32) << 8) | (data[2] as u32))
    }

    pub fn from_values(a: u8, b: u8, c: u8) -> Self {
        Self::Trigram(((a as u32) << 16) | ((b as u32) << 8) | (c as u32))
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum TrigramQueryExpression {
    Or(Vec<Reference>),
    And(Vec<Reference>),
    MinOf(i32, Vec<Reference>, Vec<usize>),
}

impl TrigramQueryExpression {
    pub fn references(&self) -> &Vec<Reference> {
        match self {
            TrigramQueryExpression::Or(refs) => refs,
            TrigramQueryExpression::And(refs) => refs,
            TrigramQueryExpression::MinOf(_, refs, _) => refs,
        }
    }
}

struct TrigramQueryBuilder {
    counter: usize,
    expressions: HashMap<usize, TrigramQueryExpression>,
}

impl TrigramQueryBuilder {
    fn new() -> Self {
        Self {
            counter: 0,
            expressions: Default::default(),
        }
    }

    fn next_id(&mut self) -> usize {
        self.counter += 1;
        self.counter - 1
    }

    fn or(&self, mut values: Vec<Reference>) -> TrigramQueryExpression {
        values.sort_unstable();
        values.dedup();
        TrigramQueryExpression::Or(values)
    }

    fn and(&self, mut values: Vec<Reference>) -> TrigramQueryExpression {
        values.sort_unstable();
        values.dedup();
        TrigramQueryExpression::And(values)
    }

    fn min_of(&self, expected: i32, values: Vec<Reference>) -> TrigramQueryExpression {
        let mut counted = HashMap::new();
        for input in values {
            *counted.entry(input).or_default() += 1;
        }

        let mut values = vec![];
        let mut counts = vec![];

        for (val, count) in counted {
            values.push(val);
            counts.push(count);
        }

        TrigramQueryExpression::MinOf(expected, values, counts)
    }

    fn insert(&mut self, input: PhraseQuery) -> Reference {
        match input {
            PhraseQuery::Or(parts) => {
                let mut expressions = vec![];
                for part in parts {
                    expressions.push(self.insert(part));
                }
                let new_id = self.next_id();
                self.expressions.insert(new_id, self.or(expressions));
                Reference::Expression(new_id)
            }

            PhraseQuery::And(parts) => {
                let mut expressions = vec![];
                for part in parts {
                    expressions.push(self.insert(part));
                }
                let new_id = self.next_id();
                self.expressions.insert(new_id, self.and(expressions));
                Reference::Expression(new_id)
            }

            PhraseQuery::MinOf(expected, parts) => {
                let mut expressions = vec![];
                for part in parts {
                    expressions.push(self.insert(part));
                }
                let new_id = self.next_id();
                self.expressions.insert(new_id, self.min_of(expected, expressions));

                Reference::Expression(new_id)
            }

            PhraseQuery::Literal(literal) => {
                let mut expressions = vec![];
                let mut trigram = ((literal[0] as u32) << 8) | literal[1] as u32;
                for byte in literal.into_iter().skip(2) {
                    trigram = ((trigram << 8) | byte as u32) & 0xFFFFFF;
                    expressions.push(Reference::Trigram(trigram));
                }
                let new_id = self.next_id();
                self.expressions.insert(new_id, self.and(expressions));
                Reference::Expression(new_id)
            }

            PhraseQuery::InsensitiveLiteral(literal) => {
                let mut expressions = vec![];
                for window in literal.windows(3) {
                    expressions.push(self.insert_case_insensitive(window.try_into().unwrap()));
                }
                let new_id = self.next_id();
                self.expressions.insert(new_id, self.and(expressions));
                Reference::Expression(new_id)
            }
        }
    }

    fn insert_case_insensitive(&mut self, trigram: [u8; 3]) -> Reference {
        let lower = [
            trigram[0].to_ascii_lowercase(),
            trigram[1].to_ascii_lowercase(),
            trigram[2].to_ascii_lowercase(),
        ];

        let upper = [
            trigram[0].to_ascii_uppercase(),
            trigram[1].to_ascii_uppercase(),
            trigram[2].to_ascii_uppercase(),
        ];

        if lower == upper {
            return Reference::from_array(lower);
        }

        let mut entries = vec![
            Reference::from_values(lower[0], lower[1], lower[2]),
            Reference::from_values(lower[0], lower[1], upper[2]),
            Reference::from_values(lower[0], upper[1], lower[2]),
            Reference::from_values(lower[0], upper[1], upper[2]),
            Reference::from_values(upper[0], lower[1], lower[2]),
            Reference::from_values(upper[0], lower[1], upper[2]),
            Reference::from_values(upper[0], upper[1], lower[2]),
            Reference::from_values(upper[0], upper[1], upper[2]),
        ];

        entries.sort_unstable();
        entries.dedup();

        let new_id = self.next_id();

        self.expressions.insert(new_id, self.or(entries));

        Reference::Expression(new_id)
    }

}

#[derive(Serialize, Deserialize)]
pub struct TrigramQuery {
    pub root: Reference,
    pub expressions: HashMap<usize, TrigramQueryExpression>,
}

impl TrigramQuery {
    pub fn build(input: PhraseQuery) -> Self {
        let mut builder = TrigramQueryBuilder::new();

        let root = builder.insert(input);

        // There is room to do some query optimization here, eg. common component removal.
        // but I suspect it will have little improvement in total time which is probably IO bound
        // not limited by the little bit of extra cpu used to do redundant ops in the relations

        // let reassignments = builder.optimize();

        // if let Reference::Expression(root_expression) = root {
        //     root = Reference::Expression(*reassignments.get(&root_expression).unwrap_or(&root_expression));
        // }

        Self {
            root,
            expressions: builder.expressions,
        }
    }
}
