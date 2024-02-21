pub mod pager;
pub mod query;

#[cfg(test)]
pub mod test {
    use std::{collections::HashMap, fs};

    use crate::{pager::Pager, query::Query};


    #[test]
    pub fn insert_then_query() {
        fs::remove_file("./test.db");
        let mut pager = Pager::new("./test.db").unwrap();

        let mut a = pager.doc();
        a.insert("name", "flora".to_string()).unwrap();
        a.insert("age", 19).unwrap();
        a.insert("likes", "cats".to_string()).unwrap();

        let mut b = pager.doc();
        b.insert("name",  "sarah".to_string()).unwrap();
        b.insert("age",  21).unwrap();
        b.insert("likes",  "dogs".to_string()).unwrap();

        let mut c = pager.doc();
        c.insert("name",  "jane".to_string()).unwrap();
        c.insert("age",  20).unwrap();
        c.insert("likes",  "cats".to_string()).unwrap();

        pager.write_document(a).unwrap();
        pager.write_document(b).unwrap();
        pager.write_document(c).unwrap();

        let mut query = Query(HashMap::new());
        query.0.insert("likes".to_string(), crate::query::Isolator::Eq("cats".to_string()));

        let results = pager.query(query).unwrap();
        println!("{:?}", results);

    }

}