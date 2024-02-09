use std::collections::{BTreeMap, HashMap};

use bincode::{config, Decode, Encode};

#[derive(Debug, Encode, Decode, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Span {
    Needs_Allocation { size: usize },
    Allocated { page: usize, size: usize },
}

#[derive(Debug, Encode, Decode)]
pub struct Document {
    pub content: BTreeMap<String, String>,
    pub metadata: Metadata,
}

#[derive(Debug, Encode, Decode)]
pub struct Metadata {
    pub id: usize,
    pub spans: Vec<Span>,
}

impl Document {
    pub fn new(id: usize) -> Self {
        Document {
            content: BTreeMap::new(),
            metadata: Metadata {
                id,
                spans: Vec::new(),
            },
        }
    }

    pub fn id(&self) -> usize {
        self.metadata.id
    }

    pub fn serialize(&self, page_size: usize) -> HashMap<Span, Vec<u8>> {
        let mut spans = HashMap::<Span, Vec<u8>>::new();
        let buffer = bincode::encode_to_vec(&self.content, config::standard()).unwrap();

        let index = 0;
        for chunk in buffer.chunks(page_size) {
            let data = chunk.to_vec();

            let span = {
                if self.metadata.spans.get(index).is_some() {
                    match self.metadata.spans.get(index).unwrap() {
                        Span::Allocated { page, size } => Span::Allocated {
                            page: *page,
                            size: *size,
                        },
                        _ => unimplemented!("Span::Needs_Allocation"),
                    }
                } else {
                    Span::Needs_Allocation { size: data.len() }
                }
            };

            spans.insert(span, data.to_vec());
        }

        spans
    }
}
