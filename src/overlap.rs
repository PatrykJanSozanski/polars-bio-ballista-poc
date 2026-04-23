use crate::fixtures::interval_schema;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::{ViewTable, provider_as_source};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionContext, col};
use std::sync::Arc;

#[derive(Debug)]
pub struct OverlapParquetFunction;

pub fn register_overlap_udtf(ctx: &SessionContext) {
    ctx.register_udtf("overlap_demo", Arc::new(OverlapParquetFunction));
}

impl TableFunctionImpl for OverlapParquetFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 2 {
            return plan_err!("overlap_demo expects exactly two parquet path arguments");
        }

        let left_path = literal_string(&args[0])?;
        let right_path = literal_string(&args[1])?;
        let definition = Some(format!(
            "overlap_demo('{}', '{}')",
            escape_sql_literal(&left_path),
            escape_sql_literal(&right_path)
        ));

        let plan = build_overlap_plan(&left_path, &right_path)?;
        Ok(Arc::new(ViewTable::new(plan, definition)))
    }
}

fn build_overlap_plan(left_path: &str, right_path: &str) -> Result<LogicalPlan> {
    let left_scan = build_scan("left", left_path)?;
    let right_scan = build_scan("right", right_path)?;

    LogicalPlanBuilder::from(left_scan)
        .join_on(
            right_scan,
            JoinType::Inner,
            vec![
                col("left.contig").eq(col("right.contig")),
                col("left.start").lt_eq(col("right.end")),
                col("left.end").gt_eq(col("right.start")),
            ],
        )?
        .project(vec![
            col("left.id").alias("left_id"),
            col("right.id").alias("right_id"),
            col("left.contig").alias("contig"),
            col("left.start").alias("left_start"),
            col("left.end").alias("left_end"),
            col("right.start").alias("right_start"),
            col("right.end").alias("right_end"),
        ])?
        .build()
}

fn build_scan(alias: &str, path: &str) -> Result<LogicalPlan> {
    let table_url = ListingTableUrl::parse(path)?;
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .with_schema(interval_schema());
    let provider = Arc::new(ListingTable::try_new(config)?);

    LogicalPlanBuilder::scan(alias, provider_as_source(provider), None)?.build()
}

fn literal_string(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _) => Ok(value.clone()),
        Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _) => Ok(value.clone()),
        _ => plan_err!("overlap_demo expects string literal paths"),
    }
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}
