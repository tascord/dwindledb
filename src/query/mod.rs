pub struct Query {
    collection: String,
    filter: Option<Filter>,
    projection: Option<Projection>,
    sort: Option<Sort>,
    limit: Option<usize>,
    skip: Option<usize>,
}

pub struct Filter {
    field: String,
    operator: Operator,
    value: Value,
}

pub struct Projection {
    fields: Vec<String>,
}

pub struct Sort {
    field: String,
    order: Order,
}

pub enum Operator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

pub enum Order {
    Ascending,
    Descending,
}

pub enum Value {
    String(String),
    Integer(i32),
    Float(f64),
    Boolean(bool),
    Null,
}

impl Query {
    pub fn new(collection: String) -> Self {
        Query {
            collection,
            filter: None,
            projection: None,
            sort: None,
            limit: None,
            skip: None,
        }
    }

    pub fn filter(&mut self, filter: Filter) -> &mut Self {
        self.filter = Some(filter);
        self
    }

    pub fn project(&mut self, projection: Projection) -> &mut Self {
        self.projection = Some(projection);
        self
    }

    pub fn sort(&mut self, sort: Sort) -> &mut Self {
        self.sort = Some(sort);
        self
    }

    pub fn limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub fn skip(&mut self, skip: usize) -> &mut Self {
        self.skip = Some(skip);
        self
    }

    pub fn execute(&self) {
        // Implement your query execution logic here
    }
}
