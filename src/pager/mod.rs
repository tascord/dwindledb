use bincode::{config, Decode, Encode};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::sync::Mutex;

use crate::pager::document::Span;
use crate::query::Query;

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
    indices: Mutex<BTreeMap<String, BTreeMap<String, usize>>>,
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
            last_used_page: Mutex::new(1),
            indices: Mutex::new(BTreeMap::new()),
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
            assert!(doc.metadata.id == 1);
            for (key, val) in self.indices.lock().unwrap().clone() {
                doc.insert(
                    &key,
                    BTreeMap::from_iter(val.into_iter().map(|(k, v)| (k, v))),
                );
            }
            self.write_document(doc)?;
        } else {
            let doc = self.read_document(1)?;
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
        let mut doc = self.read_document(1)?;
        doc.content.clear();
        for (key, val) in self.indices.lock().unwrap().clone() {
            doc.insert(
                &key,
                BTreeMap::from_iter(val.into_iter().map(|(k, v)| (k, v))),
            );
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

    pub fn write_document(&mut self, mut document: Document) -> anyhow::Result<()> {
        // Skip indexing the indices
        if document.id() != 1 {
            let previous_indices = self
                .read_document(document.id())
                .ok()
                .map(|d| d.content)
                .unwrap_or_default()
                .iter();
            let calculated_indices = document.content.iter();

            let removed = previous_indices
                .filter(|(k, _)| !calculated_indices.any(|(k2, _)| k.to_string() == k2.to_string()))
                .collect::<Vec<(&String, &Value)>>();
            let added = calculated_indices
                .filter(|(k, _)| !previous_indices.any(|(k2, _)| k.to_string() == k2.to_string()))
                .collect::<Vec<(&String, &Value)>>();
            let updated = calculated_indices
                .filter(|(k, v)| {
                    previous_indices.any(|(k2, v2)| k.to_string() == k2.to_string() && **v != *v2)
                })
                .map(|(k, v)| {
                    let (pk, pv) = previous_indices
                        .find(|(k2, _)| k.to_string() == k2.to_string())
                        .unwrap();
                    (pk, pv, k, v)
                })
                .collect::<Vec<(&String, &Value, &String, &Value)>>();

            let lock = self.indices.lock().unwrap();
            for (index, value) in removed {
                let l = lock.get(index).unwrap().get(&value.to_string()).unwrap();
            }

            for (index, value) in added {
                let l = lock.get(index).get(value).insert(document.id);
            }

            for (previous_index, previous_value, index, value) in updated {
                let l = lock
                    .get(previous_index)
                    .get(previous_value)
                    .remove(document.id);
                let l = lock.get(index).get(value).insert(document.id);
            }

            drop(lock);
            update_indices(self, removed, added, updated);
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
                .unwrap()
                .0,
            metadata: meta,
        })
    }

    pub fn query(&mut self, query: Query) -> anyhow::Result<Vec<Document>> {
        // let mut documents = Vec::new();

        // for span in &query::execute(&query) {
        //     let document = self.read_document(span)?;
        //     documents.push(document);
        // }

        // Ok(documents)

        Ok(vec![])
    }

    pub fn doc(&mut self) -> Document {
        Document::new(self.get_free_page().unwrap())
    }
}
