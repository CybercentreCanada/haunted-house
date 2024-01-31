//!
//! Roles for tracking which interfaces can be accessed by which api token.
//!

use std::collections::{HashMap, HashSet};
use std::fmt::Display;

use anyhow::Result;

use serde::{Serialize, Deserialize};

/// A set of roles indicating which actions a token can take.
#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Role {
    /// This role allows access to searching interfaces
    Search,
}


impl Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Role::Search => "Search",
        })
    }
}

/// A wrapper handing the logic of assigning roles to api tokens
pub enum Authenticator {
    /// Role assignment done by a static assignment
    Static(HashMap<String, HashSet<Role>>),
}

impl Authenticator {
    /// Crate an authenication manager from a static assignment of roles
    pub fn new_static(assignments: HashMap<String, HashSet<Role>>) -> Result<Self> {
        Ok(Authenticator::Static(assignments))
    }

    /// Crate an authenication manager from config by extracting a static assignment of roles
    pub fn from_config(config: crate::config::Authentication) -> Result<Self> {
        let mut assignment: HashMap<String, HashSet<Role>> = Default::default();
        for key in config.static_keys {
            assignment.insert(key.key, key.roles.into_iter().collect());
        }
        Self::new_static(assignment)
    }

    /// Given an api token return the set of assigned roles
    pub fn get_roles(&self, token: &str) -> Result<HashSet<Role>> {
        match self {
            Authenticator::Static(data) => Ok(match data.get(token) {
                Some(roles) => roles.clone(),
                None => Default::default()
            }),
        }
    }
}
