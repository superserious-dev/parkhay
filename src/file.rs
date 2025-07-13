use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Seek, SeekFrom},
    panic,
};

use anyhow::{Context, Error, Result};
use parquet::{file::reader::ChunkReader, thrift::TSerializable};
use thrift::protocol::TCompactInputProtocol;

pub type ByteInterval = (u64, u64);
pub type Field = parquet::schema::types::TypePtr;
pub type SectionMap = BTreeMap<ByteInterval, ParkhayDataSection>;
pub type SectionIndex = u64;

#[derive(Debug)]
pub struct ParkhayFile {
    pub path: String,
    pub start_magic: [u8; 4],
    pub footer_length: u32,
    pub data: ParkhayDataSection,
    pub footer: ParkhayFooter,
    pub end_magic: [u8; 4],
}

impl ParkhayFile {
    const FOOTER_LENGTH_LENGTH: u8 = 4;
    const MAGIC_LENGTH: u8 = 4;

    pub fn new(path: &str) -> Result<Self> {
        let mut file =
            File::open(path).context(format!("Couldn't open parquet file at `{path}`"))?;

        // Read start magic
        let mut start_magic = [0u8; Self::MAGIC_LENGTH as usize];
        file.read_exact(&mut start_magic)
            .context("Failed to read start magic bytes")?;

        // Read footer length + end magic
        file.seek(SeekFrom::End(
            -(Self::FOOTER_LENGTH_LENGTH as i64 + Self::MAGIC_LENGTH as i64),
        ))
        .context("Failed to seek to footer length offset")?;

        let footer_length = {
            let mut bytes = [0u8; 4];
            file.read_exact(&mut bytes).unwrap();
            u32::from_le_bytes(bytes)
        };

        let mut end_magic = [0u8; 4];
        file.read_exact(&mut end_magic)
            .context("Failed to read end magic bytes")?;

        // Read footer content
        file.seek(SeekFrom::End(
            -(footer_length as i64 + Self::FOOTER_LENGTH_LENGTH as i64 + Self::MAGIC_LENGTH as i64),
        ))
        .context("Failed to seek to footer content offset")?;

        let mut blob = TCompactInputProtocol::new(&file);
        let file_metadata =
            parquet::format::FileMetaData::read_from_in_protocol(&mut blob).unwrap();

        let footer = ParkhayFooter::try_from(file_metadata)?;
        Ok(Self {
            path: path.to_string(),
            start_magic,
            end_magic,
            footer_length,
            data: ParkhayDataSection::new(&footer.row_groups, file)?,
            footer,
        })
    }
}

// TODO BloomFilter, ColumnIndex, OffsetIndex, CustomIndex
#[derive(Debug)]
pub enum ParkhayDataSection {
    ColumnChunk(SectionIndex, SectionMap),
    Page(SectionIndex, Box<parquet::format::PageHeader>),
    Root(SectionMap),
    RowGroup(SectionIndex, SectionMap),
}

impl ParkhayDataSection {
    // If the row group file offset is missing, default to this.
    // TODO Verify if this is the correct behavior
    const ROW_GROUP_MISSING_FILE_OFFSET_DEFAULT: i64 = 4;

    fn insert(&mut self, byte_interval: ByteInterval, section: Self) {
        let sections = match self {
            ParkhayDataSection::Root(sections)
            | ParkhayDataSection::ColumnChunk(_, sections)
            | ParkhayDataSection::RowGroup(_, sections) => sections,
            _ => panic!("Cannot insert section into a non-container section"),
        };

        // If the interval is in a subsection, recursively insert into the subsection
        for ((l, r), subsection) in sections.iter_mut() {
            let sr = *l..=*r;
            if sr.contains(&byte_interval.0) && sr.contains(&byte_interval.1) {
                subsection.insert(byte_interval, section);
                return;
            }
        }

        // If the interval was not in any of the subsections, insert into the section at the current level
        sections.insert(byte_interval, section);
    }

    fn new(rg_metadata: &[parquet::format::RowGroup], file: File) -> Result<Self> {
        let mut root_section = Self::Root(SectionMap::new());

        for (rg_idx, rg) in rg_metadata.iter().enumerate() {
            let mut rg_section = Self::RowGroup(rg_idx as SectionIndex, SectionMap::new());

            let rg_start =
                rg.file_offset
                    .unwrap_or(Self::ROW_GROUP_MISSING_FILE_OFFSET_DEFAULT) as u64;
            let mut rg_end = rg_start;

            // Keep track of the optional column and index offsets in each column chunk.
            // Since the indexes are generally written near the end of the file, reading them
            //  after reading all the column chunk pages should minimize the cost of seeking
            //  within the file.
            let mut column_and_offset_index_ranges = BTreeSet::new();

            for (cc_idx, cc) in rg.columns.iter().enumerate() {
                // Store optional Column Index byte range
                if let (Some(start), Some(length)) =
                    (cc.column_index_offset, cc.column_index_length)
                {
                    let end = start
                        .checked_add(length as i64 - 1)
                        .context("Column Index end exceeds bounds")?;

                    column_and_offset_index_ranges.insert((start, end));
                }

                // Store optional Offset Index byte range
                if let (Some(start), Some(length)) =
                    (cc.offset_index_offset, cc.offset_index_length)
                {
                    let end = start
                        .checked_add(length as i64 - 1)
                        .context("Offset Index end exceeds bounds")?;

                    column_and_offset_index_ranges.insert((start, end));
                }

                if let Some(ref cc_metadata) = cc.meta_data {
                    let mut cc_section =
                        Self::ColumnChunk(cc_idx as SectionIndex, SectionMap::new());

                    // If the column chunk has a dictionary page, read it before the first data page
                    let cc_start = cc_metadata
                        .dictionary_page_offset
                        .unwrap_or(cc_metadata.data_page_offset)
                        as u64;

                    // Decode page headers while still in the column chunk region
                    let mut page_idx = 0 as SectionIndex;
                    let mut page_header_reader = file
                        .get_read(cc_start)
                        .context("Could not create page header reader")?;
                    while page_header_reader.stream_position().unwrap()
                        != cc_start + cc_metadata.total_compressed_size as u64
                    {
                        let page_header_start = page_header_reader.stream_position().unwrap();
                        let mut blob = TCompactInputProtocol::new(&mut page_header_reader);
                        let page_header =
                            parquet::format::PageHeader::read_from_in_protocol(&mut blob)
                                .context("Could not decode page header")?;
                        let page_start = page_header_reader.stream_position().unwrap();

                        // Move reader forward by the compressed page size
                        let page_end = if page_header.compressed_page_size > 0 {
                            page_header_reader
                                .seek_relative(page_header.compressed_page_size as i64)
                                .unwrap();
                            page_start
                                .checked_add(page_header.compressed_page_size as u64 - 1)
                                .unwrap()
                        } else {
                            // If the page size is 0, then the end of the page byte range is the final byte of the page header
                            page_start - 1
                        };

                        let page = Self::Page(page_idx, Box::new(page_header));
                        page_idx += 1;

                        cc_section.insert((page_header_start, page_end), page); // NOTE The byte range includes the page header
                    }

                    let cc_end = cc_start
                        .checked_add(cc_metadata.total_compressed_size as u64 - 1)
                        .context("Column Chunk end exceeds bounds")?;
                    rg_section.insert((cc_start, cc_end), cc_section);

                    // Extend the row group endpoint to the column chunk endpoint
                    rg_end = cc_end;
                }
            }

            // TODO read offset index and column index sections from file

            root_section.insert((rg_start, rg_end), rg_section);
        }

        Ok(root_section)
    }
}

#[derive(Debug)]
pub struct ParkhayFooter {
    pub version: i32,
    pub num_rows: i64,
    pub created_by: Option<String>,
    pub key_value_metadata: Option<Vec<parquet::format::KeyValue>>,
    pub schema_root: Field,
    pub column_orders: Option<Vec<parquet::format::ColumnOrder>>,
    pub row_groups: Vec<parquet::format::RowGroup>,
}

impl ParkhayFooter {
    pub fn schema_message_string(&self) -> String {
        let mut out = vec![];
        parquet::schema::printer::print_schema(&mut out, &self.schema_root);
        String::from_utf8_lossy(&out).to_string()
    }
}

impl TryFrom<parquet::format::FileMetaData> for ParkhayFooter {
    type Error = Error;
    fn try_from(file_metadata: parquet::format::FileMetaData) -> Result<Self> {
        Ok(Self {
            version: file_metadata.version,
            num_rows: file_metadata.num_rows,
            created_by: file_metadata.created_by,
            key_value_metadata: file_metadata.key_value_metadata,
            schema_root: parquet::schema::types::from_thrift(&file_metadata.schema).unwrap(),
            column_orders: file_metadata.column_orders,
            row_groups: file_metadata.row_groups,
        })
    }
}
