//! Native Shopping Sessions pipeline. The DuckDB pipeline in `pipeline/` is the correctness oracle:
//! every function here mirrors a rule implemented and tested there.
pub mod csv;
pub mod daily;
pub mod dump;
pub mod events;
pub mod links;
pub mod util;
pub mod pq;
pub mod raw;
pub mod sessions;
pub mod ts;
