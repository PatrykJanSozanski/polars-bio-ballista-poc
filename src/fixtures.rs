use anyhow::{Context, Result};
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const LEFT_FILE_NAME: &str = "left.parquet";
pub const RIGHT_FILE_NAME: &str = "right.parquet";

pub fn interval_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("contig", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
    ]))
}

pub fn prepare_fixtures(fixtures_dir: &Path) -> Result<(PathBuf, PathBuf)> {
    fs::create_dir_all(fixtures_dir)
        .with_context(|| format!("creating fixtures directory {}", fixtures_dir.display()))?;

    let left_path = fixtures_dir.join(LEFT_FILE_NAME);
    let right_path = fixtures_dir.join(RIGHT_FILE_NAME);

    write_fixture(
        &left_path,
        &["L1", "L2", "L3"],
        &["chr1", "chr1", "chr2"],
        &[10, 30, 5],
        &[20, 40, 8],
    )?;
    write_fixture(
        &right_path,
        &["R1", "R2", "R3", "R4"],
        &["chr1", "chr1", "chr1", "chr2"],
        &[15, 35, 50, 1],
        &[25, 38, 60, 6],
    )?;

    Ok((
        left_path.canonicalize().with_context(|| {
            format!("canonicalizing left fixture {}", left_path.display())
        })?,
        right_path.canonicalize().with_context(|| {
            format!("canonicalizing right fixture {}", right_path.display())
        })?,
    ))
}

fn write_fixture(
    path: &Path,
    ids: &[&str],
    contigs: &[&str],
    starts: &[i64],
    ends: &[i64],
) -> Result<()> {
    let schema = interval_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(contigs.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(starts.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(ends.to_vec())) as ArrayRef,
        ],
    )
    .context("building fixture record batch")?;

    let file = File::create(path)
        .with_context(|| format!("creating parquet fixture {}", path.display()))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)
        .with_context(|| format!("opening parquet writer for {}", path.display()))?;
    writer
        .write(&batch)
        .with_context(|| format!("writing parquet fixture {}", path.display()))?;
    writer
        .close()
        .with_context(|| format!("closing parquet fixture {}", path.display()))?;

    Ok(())
}
