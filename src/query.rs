use serde::{Serialize, Deserialize};



#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug)]
pub enum Query {
    Or(Vec<Query>),
    And(Vec<Query>),
    String(String),
    Literal(Vec<u8>),
}


#[cfg(test)]
mod test {
    use super::Query;


    #[test]
    fn to_json() {
        let query = Query::Or(vec![
            Query::And(vec![
                Query::String("pending_timers".to_string()), 
                Query::String("pending_batch".to_string())
            ]), 
            Query::String("get_bucket_range".to_string())
        ]);

        assert_eq!(serde_json::to_string(&query).unwrap(), r#"{"Or":[{"And":[{"String":"pending_timers"},{"String":"pending_batch"}]},{"String":"get_bucket_range"}]}"#)
    }

}

