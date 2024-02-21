use std::collections::HashMap;

pub type FilterFn = Box<dyn Fn(&Vec<u8>) -> bool>;

pub struct Query(pub HashMap<String, Isolator>);
pub enum Isolator {
    Eq(String)
}