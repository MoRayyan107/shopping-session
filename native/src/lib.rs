//! Native Shopping Sessions pipeline. The DuckDB pipeline in `pipeline/` is the correctness oracle:
//! every function here mirrors a rule implemented and tested there.
pub mod csv;
pub mod ts;
