use std::ops::BitXor;

use anyhow::Result;
use bitvec::vec::BitVec;

use crate::database::Database;
use crate::{SimpleFilter, Filter};

// Try to get a prototype quality partition function for batches of filters


enum Node {
    Leaf {
        cover: SimpleFilter,
        children: Vec<SimpleFilter>
    },
    Stem {
        cover: SimpleFilter,
        children: Vec<Node>
    }
}



// struct FilterTree {
//     root: LayerNode
// }

// impl FilterTree {

// }

fn match_size(a: BitVec, b: BitVec) -> (BitVec, BitVec) {
    todo!();
}


// struct LayerNode {
//     cover: SimpleFilter,
//     children: Vec<Item>
// }

// enum Item {
//     Leaf (SimpleFilter),
//     Stem (Box<LayerNode>)
// }

// #[warn(unused_must_use)]

const NODE_SIZE_LIMIT: usize = 16;

impl Node {

    fn cover(&self) -> &SimpleFilter {
        match self {
            Node::Leaf{cover, ..} => cover,
            Node::Stem{cover, ..} => cover,
        }
    }

    fn fit_cost(&self, item: &SimpleFilter) -> i64 {
        let cover = self.cover().data();
        let new = item.data();

        let (cover, new) = match_size(cover.clone(), new.clone());
        let changes = cover.clone().bitxor(new);

        let changes = changes.count_ones() as i64;

        return changes;
    }

    fn insert(&mut self, filter: SimpleFilter) {
        match self {
            Node::Leaf { cover, children } => {
                cover.expand(&filter);
                children.push(filter);
            },
            Node::Stem { cover, children } => {
                // Choose a child to insert this
                let (selected, _score) = children.iter()
                    .map(|child| child.fit_cost(&filter))
                    .enumerate()
                    .reduce(|a, b| {
                        if a.1 <= b.1 {
                            a
                        } else {
                            b
                        }
                    }).unwrap();

                // Split the child as needed
                if children[selected].len() + 1 >= NODE_SIZE_LIMIT {
                    // Split off the child then retry insert
                    let other = children[selected].split();
                    children.push(other);
                    return self.insert(filter);
                }

                // Perform the insert
                cover.expand(&filter);
                children[selected].insert(filter);
            },
        }
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn split(&mut self) -> Node {
        todo!();
    }
}


async fn build_tree(db: Database) -> Result<()> {

    let hashes = db.list_hashes().await?;

    let tree = Root::new();

    for hash in hashes {
        for size in 6..=25 {
            let label = SimpleFilter::label_as(1 << size);
            let simple = if let Some(filter) = db.get_simple(&hash, &label).await? {
                filter
            } else {
                break;
            };

            let density = simple.data().count_ones() as f64/((1 << size) as f64);
            found = Some((filter, size));
            if density <= target_density {
                break;
            }
        }
    }

    todo!();
}