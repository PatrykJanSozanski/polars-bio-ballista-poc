use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

const E1_LEFT_RELATIVE: &str = "fixtures/e1/left.parquet";
const E1_RIGHT_RELATIVE: &str = "fixtures/e1/right.parquet";

pub fn interval_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("contig", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
    ]))
}

pub async fn ensure_e1_toy_parquet_fixtures() -> datafusion::error::Result<(String, String)> {
    let root = std::env::current_dir().map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "cannot resolve current directory for e1 fixtures: {e}"
        ))
    })?;
    let left_path = root.join(E1_LEFT_RELATIVE);
    let right_path = root.join(E1_RIGHT_RELATIVE);

    if left_path.exists() && right_path.exists() {
        return Ok((
            left_path.to_string_lossy().to_string(),
            right_path.to_string_lossy().to_string(),
        ));
    }

    let fixtures_dir: PathBuf = root.join("fixtures/e1");
    fs::create_dir_all(&fixtures_dir).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "cannot create e1 fixtures directory {}: {e}",
            fixtures_dir.display()
        ))
    })?;

    let fixture_ctx = SessionContext::new();

    fixture_ctx
        .sql(
            "SELECT * FROM (VALUES \
             ('L1', 'chr1', 10, 20), \
             ('L2', 'chr1', 30, 40), \
             ('L3', 'chr2', 50, 60)\
             ) AS t(id, contig, start, end)",
        )
        .await?
        .write_parquet(
            &left_path.to_string_lossy(),
            DataFrameWriteOptions::new(),
            None,
        )
        .await?;

    fixture_ctx
        .sql(
            "SELECT * FROM (VALUES \
             ('R1', 'chr1', 15, 25), \
             ('R2', 'chr1', 31, 39), \
             ('R3', 'chr1', 100, 120), \
             ('R4', 'chr2', 55, 65)\
             ) AS t(id, contig, start, end)",
        )
        .await?
        .write_parquet(
            &right_path.to_string_lossy(),
            DataFrameWriteOptions::new(),
            None,
        )
        .await?;

    Ok((
        left_path.to_string_lossy().to_string(),
        right_path.to_string_lossy().to_string(),
    ))
}
