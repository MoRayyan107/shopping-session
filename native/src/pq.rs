//! Parquet output through the `parquet` crate's low-level column writer (no arrow, no compression).
//! `day` is written as INT32 annotated DATE (days since 1970-01-01), strings as BYTE_ARRAY annotated STRING.
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{BoolType, ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

/// One column's values, in schema order.
pub enum Col {
    Str(Vec<Vec<u8>>),
    OptStr(Vec<Option<Vec<u8>>>),
    Date(Vec<i32>),
    I64(Vec<i64>),
    OptI64(Vec<Option<i64>>),
    Bool(Vec<bool>),
    OptDate(Vec<Option<i32>>),
    OptBool(Vec<Option<bool>>),
}

impl Col {
    fn len(&self) -> usize {
        match self {
            Col::Str(v) => v.len(),
            Col::OptStr(v) => v.len(),
            Col::Date(v) => v.len(),
            Col::I64(v) => v.len(),
            Col::OptI64(v) => v.len(),
            Col::Bool(v) => v.len(),
            Col::OptDate(v) => v.len(),
            Col::OptBool(v) => v.len(),
        }
    }
}

const ROW_GROUP: usize = 1 << 20;

/// Write `cols` (matching `message_type`, e.g. "message t { REQUIRED INT32 day (DATE); }") to `path`.
pub fn write_table(path: &Path, message_type: &str, cols: &[Col]) -> Result<()> {
    let schema = Arc::new(parse_message_type(message_type)?);
    let props = Arc::new(WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).build());
    let mut w = SerializedFileWriter::new(File::create(path)?, schema, props)?;
    let rows = cols.first().map_or(0, Col::len);
    assert!(cols.iter().all(|c| c.len() == rows), "columns of different lengths");
    let mut start = 0;
    loop {
        let end = (start + ROW_GROUP).min(rows);
        let mut rg = w.next_row_group()?;
        for c in cols {
            let mut col = rg.next_column()?.expect("schema has fewer columns than data");
            match c {
                Col::Str(v) => {
                    let vals: Vec<ByteArray> = v[start..end].iter().map(|s| ByteArray::from(s.clone())).collect();
                    col.typed::<ByteArrayType>().write_batch(&vals, None, None)?;
                }
                Col::OptStr(v) => {
                    let part = &v[start..end];
                    let defs: Vec<i16> = part.iter().map(|s| s.is_some() as i16).collect();
                    let vals: Vec<ByteArray> = part.iter().flatten().map(|s| ByteArray::from(s.clone())).collect();
                    col.typed::<ByteArrayType>().write_batch(&vals, Some(&defs), None)?;
                }
                Col::Date(v) => {
                    col.typed::<Int32Type>().write_batch(&v[start..end], None, None)?;
                }
                Col::I64(v) => {
                    col.typed::<Int64Type>().write_batch(&v[start..end], None, None)?;
                }
                Col::OptI64(v) => {
                    let part = &v[start..end];
                    let defs: Vec<i16> = part.iter().map(|s| s.is_some() as i16).collect();
                    let vals: Vec<i64> = part.iter().flatten().copied().collect();
                    col.typed::<Int64Type>().write_batch(&vals, Some(&defs), None)?;
                }
                Col::Bool(v) => {
                    col.typed::<BoolType>().write_batch(&v[start..end], None, None)?;
                }
                Col::OptDate(v) => {
                    let part = &v[start..end];
                    let defs: Vec<i16> = part.iter().map(|s| s.is_some() as i16).collect();
                    let vals: Vec<i32> = part.iter().flatten().copied().collect();
                    col.typed::<Int32Type>().write_batch(&vals, Some(&defs), None)?;
                }
                Col::OptBool(v) => {
                    let part = &v[start..end];
                    let defs: Vec<i16> = part.iter().map(|s| s.is_some() as i16).collect();
                    let vals: Vec<bool> = part.iter().flatten().copied().collect();
                    col.typed::<BoolType>().write_batch(&vals, Some(&defs), None)?;
                }
            }
            col.close()?;
        }
        rg.close()?;
        start = end;
        if start >= rows {
            break;
        }
    }
    w.close()?;
    Ok(())
}

pub const USER_DAILY: &str = "message user_daily {
    REQUIRED BYTE_ARRAY user_id (STRING);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 events;
    REQUIRED INT64 revenue_cents;
    OPTIONAL BYTE_ARRAY top_category (STRING);
}";

pub const CAMPAIGN_DAILY: &str = "message campaign_daily {
    REQUIRED BYTE_ARRAY campaign_id (STRING);
    OPTIONAL BYTE_ARRAY channel (STRING);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 purchasing_sessions;
    REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions;
}";
