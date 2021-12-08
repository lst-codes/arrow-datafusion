
use ballista::prelude::*;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results, using the DataFrame trait
#[tokio::main]
async fn main() -> Result<()> {
    let config = BallistaConfig::builder()
        .set("ballista.shuffle.partitions", "4")
        .build()?;
    let ctx = BallistaContext::remote("localhost", 50050, &config);

    let filename = "/home/lexi/tmp/testing-parquet/";

    // define the query using the DataFrame trait
    let df = ctx
        .read_parquet(filename)
        .await?
        .select_columns(&["id", "data"])?;

    // print the results
    df.show().await?;

    Ok(())
}
