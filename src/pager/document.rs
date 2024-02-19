use std::{any::Any, collections::{BTreeMap, HashMap}};

use bincode::{config, Decode, Encode};

pub struct Data<T>(pub T)
where
    T: Encode + Decode;
impl<T> Data<T>
where
    T: Encode + Decode,
{
    pub fn dec(s: &[u8]) -> Result<Self, String> {
        Ok(Self(
            bincode::decode_from_slice(s, config::standard())
                .map(|i| i.0)
                .map_err(|_| "Unable to decode value".to_string())?,
        ))
    }

    pub fn enc(&self) -> Result<&[u8], String> {
        bincode::encode_to_vec(self.0, config::standard())
            .map(|a| a.as_slice())
            .map_err(|_| "Unable to encode value".to_string())
    }
}

#[derive(Debug, Encode, Decode, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Span {
    NeedsAllocation { size: usize },
    Allocated { page: usize, size: usize },
}

#[derive(Debug, Encode, Decode)]
pub struct Document {
    pub content: BTreeMap<String, Vec<u8>>,
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
                    Span::NeedsAllocation { size: data.len() }
                }
            };

            spans.insert(span, data.to_vec());
        }

        spans
    }

    pub fn insert<T>(&mut self, key: &str, value: T) -> Result<(), String>
    where
        T: Decode + Encode,
    {
        self.content.insert(key.to_string(), Data(value).enc()?.to_vec());
        Ok(())
    }

    pub fn get<T>(&self, key: &str) -> Result<Option<Data<T>>, String>
    where
        T: Decode + Encode,
    {
        match self.content.get(key) {
            None => Ok(None),
            Some(v) => Ok(Some(Data::dec(v)?)),
        }
    }
}
