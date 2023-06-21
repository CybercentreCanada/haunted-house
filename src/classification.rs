//!
//! Parse assemblyline classification configuration files in order to
//! turn classification strings into access control structs.
//!

use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassificationConfig {

}