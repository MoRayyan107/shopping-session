//! Parquet output through the `parquet` crate's low-level column writer (no arrow, no compression).
//! `day` is written as INT32 annotated DATE (days since 1970-01-01), strings as BYTE_ARRAY annotated STRING.
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{BoolType, ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::errors::Result;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
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
    /// internal: string columns converted for the writer
    Bytes(Vec<ByteArray>),
    OptBytes(Vec<ByteArray>, Vec<i16>),
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
            Col::Bytes(v) => v.len(),
            Col::OptBytes(_, d) => d.len(),
        }
    }

    fn into_bytes(self) -> Col {
        match self {
            Col::Str(v) => Col::Bytes(v.into_iter().map(ByteArray::from).collect()),
            Col::OptStr(v) => {
                let defs = v.iter().map(|s| s.is_some() as i16).collect();
                Col::OptBytes(v.into_iter().flatten().map(ByteArray::from).collect(), defs)
            }
            c => c,
        }
    }
}

const ROW_GROUP: usize = 1 << 20;

/// Write `cols` (matching `message_type`, e.g. "message t { REQUIRED INT32 day (DATE); }") to `path`.
pub fn write_table(path: &Path, message_type: &str, cols: Vec<Col>) -> Result<()> {
    let schema = Arc::new(parse_message_type(message_type)?);
    // plain encoding, no dictionary, no statistics: the fastest to write, and readers need neither
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .build(),
    );
    let mut w = SerializedFileWriter::new(File::create(path)?, schema, props)?;
    let rows = cols.first().map_or(0, Col::len);
    let cols: Vec<Col> = cols.into_iter().map(Col::into_bytes).collect();
    assert!(cols.iter().all(|c| c.len() == rows), "columns of different lengths");
    let mut start = 0;
    loop {
        let end = (start + ROW_GROUP).min(rows);
        let mut rg = w.next_row_group()?;
        for c in &cols {
            let mut col = rg.next_column()?.expect("schema has fewer columns than data");
            match c {
                Col::Str(_) | Col::OptStr(_) => unreachable!("converted by into_bytes"),
                Col::Bytes(v) => {
                    col.typed::<ByteArrayType>().write_batch(&v[start..end], None, None)?;
                }
                Col::OptBytes(v, defs) => {
                    // v holds only the present values, in order; find the slice of v for rows start..end
                    let d = &defs[start..end];
                    let before = defs[..start].iter().filter(|&&x| x == 1).count();
                    let n = d.iter().filter(|&&x| x == 1).count();
                    col.typed::<ByteArrayType>().write_batch(&v[before..before + n], Some(d), None)?;
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
