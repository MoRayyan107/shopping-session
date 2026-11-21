//! Writes small user_daily / campaign_daily files with awkward values, to check the Parquet output
//! with polars and DuckDB:  parquet_probe OUT_DIR
use std::path::PathBuf;

use shopping_sessions_native::pq::{write_table, Col, CAMPAIGN_DAILY, USER_DAILY};
use shopping_sessions_native::ts::days_from_civil;

fn main() {
    let out = PathBuf::from(std::env::args().nth(1).expect("OUT_DIR"));
    let d = |y, m, dd| days_from_civil(y, m, dd) as i32;
    std::fs::create_dir_all(out.join("user_daily")).unwrap();
    std::fs::create_dir_all(out.join("campaign_daily")).unwrap();
    std::fs::create_dir_all(out.join("empty")).unwrap();
    write_table(
        &out.join("user_daily/part-0.parquet"),
        USER_DAILY,
        vec![
            Col::Str(vec![b"A\tB".to_vec(), "é€😀".as_bytes().to_vec(), b"u1\r".to_vec()]),
            Col::Date(vec![d(2028, 2, 29), d(1970, 1, 1), d(2026, 10, 3)]),
            Col::I64(vec![1, 2, 3]),
            Col::I64(vec![2, 5, 9]),
            Col::I64(vec![0, 1000, 10_000_000_000]),
            Col::OptStr(vec![None, Some(b"shoes".to_vec()), Some(b"unknown".to_vec())]),
        ],
    )
    .unwrap();
    write_table(
        &out.join("campaign_daily/part-0.parquet"),
        CAMPAIGN_DAILY,
        vec![
            Col::Str(vec![b"organic".to_vec(), b"c_1".to_vec()]),
            Col::Str(vec![b"organic".to_vec(), b"search".to_vec()]),
            Col::Date(vec![d(2028, 2, 29), d(2028, 3, 1)]),
            Col::I64(vec![5, 0]),
            Col::I64(vec![1, 0]),
            Col::I64(vec![1000, 0]),
            Col::I64(vec![0, 7]),
        ],
    )
    .unwrap();
    write_table(
        &out.join("empty/part-0.parquet"),
        USER_DAILY,
        vec![Col::Str(vec![]), Col::Date(vec![]), Col::I64(vec![]), Col::I64(vec![]), Col::I64(vec![]), Col::OptStr(vec![])],
    )
    .unwrap();
    eprintln!("wrote probe files to {}", out.display());
}
