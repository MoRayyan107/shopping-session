//! Stage dumps for diffing against the DuckDB oracle's PIPELINE_DUMP_DIR tables (same names and columns).
use std::path::PathBuf;

use crate::pq::{write_table, Col};
use crate::raw::Row;

pub struct Dumper {
    dir: Option<PathBuf>,
    /// NATIVE_DUMP_TABLES: comma-separated names to dump (all when unset).
    only: Option<Vec<String>>,
}

impl Dumper {
    pub fn from_env() -> Dumper {
        let dir = std::env::var_os("NATIVE_DUMP_DIR").map(PathBuf::from);
        if let Some(d) = &dir {
            std::fs::create_dir_all(d).expect("create NATIVE_DUMP_DIR");
        }
        let only = std::env::var("NATIVE_DUMP_TABLES").ok().map(|s| s.split(',').map(str::to_string).collect());
        Dumper { dir, only }
    }
    /// True when dumping is on at all (cheap check before building dump columns).
    pub fn on(&self) -> bool {
        self.dir.is_some()
    }
    /// True when this table should be dumped.
    pub fn wants(&self, name: &str) -> bool {
        self.dir.is_some() && self.only.as_ref().is_none_or(|o| o.iter().any(|t| t == name))
    }
    fn path(&self, name: &str) -> PathBuf {
        self.dir.as_ref().unwrap().join(format!("{name}.parquet"))
    }

    /// A table of nullable string columns (the raw_* tables).
    pub fn raw<const N: usize>(&self, name: &str, columns: [&str; N], rows: &[Row<'_, N>]) {
        if !self.wants(name) {
            return;
        }
        let schema = format!(
            "message {name} {{ {} }}",
            columns.iter().map(|c| format!("OPTIONAL BYTE_ARRAY {c} (STRING);")).collect::<Vec<_>>().join(" ")
        );
        let cols: Vec<Col> =
            (0..N).map(|i| Col::OptStr(rows.iter().map(|r| r[i].map(<[u8]>::to_vec)).collect())).collect();
        write_table(&self.path(name), &schema, cols).expect("write dump");
    }

    /// Any table: `schema` is a Parquet message type, `cols` in its order.
    pub fn table(&self, name: &str, schema: &str, cols: impl FnOnce() -> Vec<Col>) {
        if self.wants(name) {
            let cols = cols();
            write_table(&self.path(name), schema, cols).expect("write dump");
        }
    }
}
