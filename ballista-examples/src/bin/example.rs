
use ballista::prelude::*;
use datafusion::logical_plan::{Partitioning, col};

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results, using the DataFrame trait
#[tokio::main]
async fn main() -> Result<()> {
    let config = BallistaConfig::builder()
        .set("ballista.shuffle.partitions", "4")
        .build()?;
    let ctx = BallistaContext::remote("localhost", 50050, &config);

    let filename = "/tmp/data.parquet";

    // define the query using the DataFrame trait
    let df = ctx
        .read_parquet(filename)
        .await?;
        
    let partitioning_columns = vec![
        col("nlm_dimension_load_date"), 
        col("src_fuel_type")];
        
    let partitioned_df = df
        .repartition(Partitioning::Columns(
            partitioning_columns))?; // 72

    // print the results
    partitioned_df.show().await?;

    Ok(())
}