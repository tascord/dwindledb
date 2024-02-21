use bincode::{config, Decode, Encode};
use log::warn;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::sync::Mutex;

// TODO: WRITE INDICIES PAGE
// AND READ 
// YAY :D

use crate::pager::document::Span;
use crate::query::{FilterFn, Isolator, Query};

use self::document::{Data, Document};

pub mod document;

const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = 300;
const MAGIC_BYTES: [u8; 4] = [0xDE, 0xAD, 0xBE, 0xEF];

#[derive(Debug, Encode, Decode)]
struct DatabaseHeader {
    page_size: usize,
    free_pages: Vec<usize>,
    last_used_page: usize,
}

impl Default for DatabaseHeader {
    fn default() -> Self {
        DatabaseHeader {
            page_size: PAGE_SIZE,
            free_pages: Vec::new(),
            last_used_page: 0,
        }
    }
}

pub struct Pager {
    entry: File,
    free_pages: Mutex<Vec<usize>>,
    last_used_page: Mutex<usize>,
    indices_page: usize,

    /// {[key: string]: {[value: Vec<u8>]: Vec<page: number>}}
    indices: Mutex<BTreeMap<String, BTreeMap<Vec<u8>, Vec<usize>>>>,
}

impl Pager {
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let file_entry = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(path);

        let mut pager = Pager {
            entry: file_entry?,
            free_pages: Mutex::new(Vec::new()),
            last_used_page: Mutex::new(0),
            indices: Mutex::new(BTreeMap::new()),
            indices_page: 0,
        };

        pager.initialize_header()?;
        pager.initialize_indices()?;

        Ok(pager)
    }

    fn initialize_header(&mut self) -> anyhow::Result<()> {
        let metadata = self.entry.metadata()?;
        let length = metadata.len() as usize;

        if length != 0 {
            // Check magic bytes
            let mut magic = [0; 4];
            self.entry.read_exact(&mut magic)?;
            if magic != MAGIC_BYTES {
                return Err(anyhow::anyhow!("Invalid database file"));
            }

            // Read header
            let mut buffer = [0; HEADER_SIZE];
            self.entry.read_exact(&mut buffer)?;
            let (header, _): (DatabaseHeader, _) =
                bincode::decode_from_slice(&buffer, config::standard())?;

            // Read free pages
            let mut free_pages = self.free_pages.lock().unwrap();
            for _ in 0..header.free_pages.len() {
                let mut page = [0; 4];
                self.entry.read_exact(&mut page)?;
                let page = u32::from_le_bytes(page) as usize;
                free_pages.push(page);
            }

            let mut last_used_page = self.last_used_page.lock().unwrap();
            *last_used_page = header.last_used_page;
        } else {
            self.update_header()?;
        }

        Ok(())
    }

    fn update_header(&mut self) -> anyhow::Result<()> {
        let free_pages = self.free_pages.lock().unwrap().to_vec().clone();
        let last_used_page = *self.last_used_page.lock().unwrap();

        let header = DatabaseHeader {
            page_size: PAGE_SIZE,
            free_pages,
            last_used_page,
        };

        let mut buffer = vec![0; PAGE_SIZE - MAGIC_BYTES.len()];
        let size = bincode::encode_into_slice(header, &mut buffer, config::standard())?;

        assert!(size <= HEADER_SIZE);
        buffer.resize(HEADER_SIZE - MAGIC_BYTES.len(), 0);

        self.entry.seek(std::io::SeekFrom::Start(0))?;
        self.entry.write_all(&MAGIC_BYTES)?;
        self.entry.write_all(&buffer)?;
        Ok(())
    }

    fn initialize_indices(&mut self) -> anyhow::Result<()> {
        if self.entry.metadata()?.len() < PAGE_SIZE as u64 * 2 {
            let mut doc = self.doc();
            self.indices_page = doc.metadata.id;
            for (key, val) in self.indices.lock().unwrap().clone() {
                doc.insert(
                    &key,
                    BTreeMap::from_iter(val.into_iter().map(|(k, v)| (k, v))),
                )
                .map_err(|_| anyhow::anyhow!("Unable to insert index"))?;
            }
            self.write_document(doc)?;
        } else {
            let doc = self.read_document(self.indices_page)?;
            let mut lock = self.indices.lock().unwrap();
            for (key, val) in doc.content {
                lock.insert(
                    key,
                    Data::dec(val.as_slice()).map_err(|e| anyhow::anyhow!(e))?.0,
                );
            }
        }

        Ok(())
    }

    fn update_indices(&mut self) -> anyhow::Result<()> {
        let mut doc = self.read_document(self.indices_page)?;
        doc.content.clear();
        for (key, val) in self.indices.lock().unwrap().clone() {
            doc.insert(
                &key,
                BTreeMap::from_iter(val.into_iter().map(|(k, v)| (k, v))),
            )
            .map_err(|_| anyhow::anyhow!("Unable to update index"))?;
        }
        self.write_document(doc)?;
        Ok(())
    }

    fn get_free_page(&mut self) -> anyhow::Result<usize> {
        let mut out_page = 0;
        let mut free_pages = self.free_pages.lock().unwrap();
        if let Some(page) = free_pages.pop() {
            println!("Reusing page {}", page);
            out_page = page;
        }

        drop(free_pages);

        if out_page != 0 {
            self.update_header().unwrap();
            return Ok(out_page);
        }

        let page = self.last_used_page.lock().unwrap().clone() + 1;
        *self.last_used_page.lock().unwrap() = page;

        println!("Allocating new page {}", page);
        self.update_header().unwrap();
        Ok(page)
    }

    fn free(&mut self, page: usize) {
        let mut free_pages = self.free_pages.lock().unwrap();
        free_pages.push(page);
    }

    fn remove_index(&mut self, key: &str, value: Vec<u8>, page: usize) {
        let mut lock = self.indices.lock().unwrap();
        match lock.get_mut(key) {
            Some(bt) => {
                let vec_at_value = bt.get_mut(&value).unwrap();
                if vec_at_value.len() == 1 {
                    bt.remove(&value);
                } else {
                    *vec_at_value = vec_at_value
                        .clone()
                        .into_iter()
                        .filter(|&x| x != page)
                        .collect();
                }
            }
            None => warn!("Tried to remove non-existent index"),
        }
    }

    fn insert_index(&mut self, key: &str, value: Vec<u8>, page: usize) {
        let mut lock = self.indices.lock().unwrap();
        match lock.get_mut(key) {
            Some(bt) => match bt.get_mut(&value) {
                Some(vec_at_value) => {
                    vec_at_value.push(page);
                }
                None => {
                    bt.insert(value, vec![page]);
                }
            },
            None => {
                lock.insert(
                    key.to_string(),
                    BTreeMap::from_iter(vec![(value, vec![page])]),
                );
            }
        }
    }

    fn replace_index(&mut self, key: &str, old_value: Vec<u8>, new_value: Vec<u8>, page: usize) {
        let mut lock = self.indices.lock().unwrap();
        match lock.get_mut(key) {
            Some(bt) => {
                let vec_at_value = bt.get_mut(&old_value).unwrap();
                if vec_at_value.len() == 1 {
                    bt.remove(&old_value);
                } else {
                    *vec_at_value = vec_at_value
                        .clone()
                        .into_iter()
                        .filter(|&x| x != page)
                        .collect();
                }

                match bt.get_mut(&new_value) {
                    Some(vec_at_value) => {
                        vec_at_value.push(page);
                    }
                    None => {
                        bt.insert(new_value, vec![page]);
                    }
                }
            }
            None => warn!("Tried to update non-existent index"),
        }
    }

    pub fn write_document(&mut self, mut document: Document) -> anyhow::Result<()> {
        // Skip indexing the indices
        if document.id() != 1 {
            let previous_indices = match self.read_document(document.id()) {
                Ok(doc) => doc.content,
                Err(_) => BTreeMap::new(),
            };

            let calculated_indices = document.content.iter();

            // Calculate removed, added, and updated indices
            let removed = previous_indices
                .iter()
                .filter(|(k, _)| !calculated_indices.clone().any(|(key, _)| key == *k))
                .map(|(k, _)| k.to_string())
                .collect::<Vec<String>>();

            let added = calculated_indices
                .clone()
                .filter(|(k, _)| !previous_indices.iter().any(|(key, _)| key == *k))
                .map(|(k, _)| k.to_string())
                .collect::<Vec<String>>();

            let updated = calculated_indices
                .clone()
                .filter(|(k, v)| {
                    previous_indices
                        .iter()
                        .any(|(key, val)| key == *k && &val != v)
                })
                .map(|(k, _)| k.to_string())
                .collect::<Vec<String>>();

            for key in removed {
                let value = previous_indices.get(&key).unwrap();
                self.remove_index(&key, value.clone(), document.id());
            }

            for key in added {
                let value = document.content.get(&key).unwrap();
                self.insert_index(&key, value.clone(), document.id());
            }

            for key in updated {
                let old_value = previous_indices.get(&key).unwrap();
                let new_value = document.content.get(&key).unwrap();
                self.replace_index(&key, old_value.clone(), new_value.clone(), document.id());
            }

            self.update_indices()?;
        }

        let spans = document.serialize(PAGE_SIZE);

        // Truncate unused spans
        for span in spans.len()..document.metadata.spans.len() {
            self.free(span);
            document.metadata.spans.pop();
        }

        for (span, data) in spans {
            let page = match span {
                Span::NeedsAllocation { size } => {
                    let page = self.get_free_page().unwrap();
                    document.metadata.spans.push(Span::Allocated { page, size });
                    page
                }
                Span::Allocated { page, size: _ } => page,
            };

            let offset = page * PAGE_SIZE;
            self.entry.seek(std::io::SeekFrom::Start(offset as u64))?;
            self.entry.write_all(&data)?;

            println!("Wrote {}b to page {} ({offset})", data.len(), page);
        }

        // All spans are allocated in the previous step, so we can safely remove the Needs_Allocation spans
        document.metadata.spans = document
            .metadata
            .spans
            .into_iter()
            .filter(|span| match span {
                Span::NeedsAllocation { size: _ } => false,
                Span::Allocated { page: _, size: _ } => true,
            })
            .collect();

        println!("New spans: {:?}", document.metadata.spans);

        let offset = document.metadata.id * PAGE_SIZE;
        let mut buffer = vec![0; PAGE_SIZE];
        bincode::encode_into_slice(&document.metadata, &mut buffer, config::standard())?;
        self.entry.seek(std::io::SeekFrom::Start(offset as u64))?;
        self.entry.write_all(&buffer)?;

        Ok(())
    }

    pub fn read_document(&mut self, id: usize) -> anyhow::Result<Document> {
        let offset = id * PAGE_SIZE;

        self.entry.seek(std::io::SeekFrom::Start(offset as u64))?;
        let meta: document::Metadata =
            bincode::decode_from_std_read(&mut self.entry, config::standard())?;

        println!("Read spans: {:?}", meta.spans);

        let mut content = Vec::new();
        for span in &meta.spans {
            let (page, size) = match span {
                Span::NeedsAllocation { size: _ } => unimplemented!("Span::Needs_Allocation"),
                Span::Allocated { page, size } => (*page, *size),
            };

            let offset = page * PAGE_SIZE;
            self.entry.seek(std::io::SeekFrom::Start(offset as u64))?;

            let mut chunk = vec![0; size];
            self.entry.read_exact(&mut chunk)?;

            println!("Read {}b from page {} ({offset})", chunk.len(), page);
            content.extend(chunk);
        }

        Ok(Document {
            content: bincode::decode_from_slice(&content, config::standard())
                .map_err(|e| anyhow::anyhow!(e))?
                .0,
            metadata: meta,
        })
    }

    pub fn query(&mut self, query: Query) -> anyhow::Result<Vec<Document>> {
        let mut filters = HashMap::<String, FilterFn>::new();

        // Construct filters from isolators
        for (key, isolator) in query.0 {
            match isolator {
                Isolator::Eq(value) => {
                    let filter = move |val: &Vec<u8>| {
                        val == &bincode::encode_to_vec(value.clone(), config::standard()).unwrap()
                    };

                    filters.insert(key, Box::new(filter));
                }
            }
        }

        // Iterate over all indexed values
        let indices = self.indices.lock().unwrap();
        let mut pages = Vec::<usize>::new();
        for (key, filter) in filters {
            if let Some(index) = indices.get(&key) {
                for (value, page) in index {
                    if filter(value) {
                        pages.extend(page);
                    }
                }
            }
        }

        drop(indices);

        Ok(pages
            .into_iter()
            .map(|page| self.read_document(page).unwrap())
            .collect())
    }

    pub fn doc(&mut self) -> Document {
        Document::new(self.get_free_page().unwrap())
    }
}
