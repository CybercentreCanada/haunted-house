use anyhow::Result;
use kube::core::CustomResourceExt;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};


#[derive(CustomResource, Debug, Serialize, Deserialize, Default, Clone, JsonSchema)]
#[kube(group = "cccs.gc.ca", version = "beta1", kind = "HauntedHouse", plural = "hauntedhouse", namespaced)]
pub struct HauntedHouseSpec {
    registry: String,
    image: String,
    tag: String,
}


fn main() -> Result<()> {

    println!("{}", serde_yaml::to_string(&HauntedHouse::crd())?);

    return Ok(())
}