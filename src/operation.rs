use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::TableReference;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, ParquetReadOptions, SessionContext};
use datafusion_bio_function_ranges::{FilterOp as RangesFilterOp, OverlapProvider};

static OVERLAP_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

const LEFT_TABLE: &str = "left_intervals";
const RIGHT_TABLE: &str = "right_intervals";
const INTERVAL_COLUMNS: [&str; 3] = ["contig", "pos_start", "pos_end"];

pub async fn register_polars_bio_parquet_inputs(
    ctx: &SessionContext,
    left_path: &str,
    right_path: &str,
) -> Result<()> {
    ctx.register_parquet(LEFT_TABLE, left_path, ParquetReadOptions::new())
        .await?;
    ctx.register_parquet(RIGHT_TABLE, right_path, ParquetReadOptions::new())
        .await?;
    Ok(())
}

pub async fn do_polars_bio_style_overlap(
    ctx: &SessionContext,
    left_path: &str,
    right_path: &str,
) -> Result<DataFrame> {
    do_overlap(
        ctx,
        LEFT_TABLE.to_string(),
        RIGHT_TABLE.to_string(),
        Some(left_path.to_string()),
        Some(right_path.to_string()),
        default_interval_columns(),
        default_interval_columns(),
        ("_1".to_string(), "_3".to_string()),
        ProviderVariant::Serialized,
    )
    .await
}

pub async fn do_approach_a_overlap(
    ctx: &SessionContext,
    left_path: &str,
    right_path: &str,
) -> Result<DataFrame> {
    do_overlap(
        ctx,
        LEFT_TABLE.to_string(),
        RIGHT_TABLE.to_string(),
        Some(left_path.to_string()),
        Some(right_path.to_string()),
        default_interval_columns(),
        default_interval_columns(),
        ("_1".to_string(), "_3".to_string()),
        ProviderVariant::ApproachA,
    )
    .await
}

pub async fn do_approach_b_overlap(
    ctx: &SessionContext,
    left_path: &str,
    right_path: &str,
) -> Result<DataFrame> {
    do_overlap(
        ctx,
        LEFT_TABLE.to_string(),
        RIGHT_TABLE.to_string(),
        Some(left_path.to_string()),
        Some(right_path.to_string()),
        default_interval_columns(),
        default_interval_columns(),
        ("_1".to_string(), "_3".to_string()),
        ProviderVariant::ApproachB,
    )
    .await
}

pub async fn do_polars_bio_direct_overlap(ctx: &SessionContext) -> Result<DataFrame> {
    let left_table = LEFT_TABLE.to_string();
    let right_table = RIGHT_TABLE.to_string();
    let columns_1 = default_interval_columns();
    let columns_2 = default_interval_columns();
    let suffixes = ("_1".to_string(), "_3".to_string());
    let session = ctx.clone();

    let left_table_ref = TableReference::from(left_table.clone());
    let left_schema = ctx.table(left_table_ref).await?.schema().as_arrow().clone();
    let right_table_ref = TableReference::from(right_table.clone());
    let right_schema = ctx
        .table(right_table_ref)
        .await?
        .schema()
        .as_arrow()
        .clone();

    let overlap_provider = OverlapProvider::new(
        Arc::new(session),
        left_table,
        right_table,
        left_schema.clone(),
        right_schema.clone(),
        columns_1,
        columns_2,
        RangesFilterOp::Weak,
    );

    let table_name = format!(
        "overlap_result_{}",
        OVERLAP_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    ctx.register_table(table_name.as_str(), Arc::new(overlap_provider))?;

    let mut select_parts = Vec::new();
    for field in left_schema.fields() {
        select_parts.push(format!(
            "`left_{}` AS `{}{}`",
            field.name(),
            field.name(),
            suffixes.0
        ));
    }
    for field in right_schema.fields() {
        select_parts.push(format!(
            "`right_{}` AS `{}{}`",
            field.name(),
            field.name(),
            suffixes.1
        ));
    }

    let query = format!("SELECT {} FROM {}", select_parts.join(", "), table_name);
    ctx.sql(&query).await
}

async fn do_overlap(
    ctx: &SessionContext,
    left_table: String,
    right_table: String,
    left_path: Option<String>,
    right_path: Option<String>,
    columns_1: Vec<String>,
    columns_2: Vec<String>,
    suffixes: (String, String),
    variant: ProviderVariant,
) -> Result<DataFrame> {
    let left_table_ref = TableReference::from(left_table.clone());
    let left_schema = ctx.table(left_table_ref).await?.schema().as_arrow().clone();
    let right_table_ref = TableReference::from(right_table.clone());
    let right_schema = ctx
        .table(right_table_ref)
        .await?
        .schema()
        .as_arrow()
        .clone();

    let provider: Arc<dyn TableProvider> = match variant {
        ProviderVariant::Serialized => Arc::new(SerializableOverlapProvider::new(
            left_table,
            right_table,
            left_path,
            right_path,
            left_schema.clone(),
            right_schema.clone(),
            columns_1,
            columns_2,
            false,
        )),
        ProviderVariant::ApproachA => Arc::new(ApproachAOverlapProvider::new(
            left_table,
            right_table,
            left_path,
            right_path,
            left_schema.clone(),
            right_schema.clone(),
            columns_1,
            columns_2,
            false,
        )),
        ProviderVariant::ApproachB => Arc::new(ApproachBOverlapProvider::new(
            left_table,
            right_table,
            left_path,
            right_path,
            left_schema.clone(),
            right_schema.clone(),
            columns_1,
            columns_2,
            false,
            1,
        )),
    };

    let table_name = format!(
        "overlap_result_{}",
        OVERLAP_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    ctx.register_table(table_name.as_str(), provider)?;

    let mut select_parts = Vec::new();
    for field in left_schema.fields() {
        select_parts.push(format!(
            "`left_{}` AS `{}{}`",
            field.name(),
            field.name(),
            suffixes.0
        ));
    }
    for field in right_schema.fields() {
        select_parts.push(format!(
            "`right_{}` AS `{}{}`",
            field.name(),
            field.name(),
            suffixes.1
        ));
    }

    let query = format!("SELECT {} FROM {}", select_parts.join(", "), table_name);
    ctx.sql(&query).await
}

fn default_interval_columns() -> Vec<String> {
    INTERVAL_COLUMNS
        .iter()
        .map(|column| column.to_string())
        .collect()
}

#[derive(Clone, Copy)]
enum ProviderVariant {
    Serialized,
    ApproachA,
    ApproachB,
}

#[derive(Clone)]
pub struct SerializableOverlapProvider {
    left_table: String,
    right_table: String,
    left_path: Option<String>,
    right_path: Option<String>,
    columns_1: (String, String, String),
    columns_2: (String, String, String),
    strict: bool,
    schema: SchemaRef,
}

impl SerializableOverlapProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left_table: String,
        right_table: String,
        left_path: Option<String>,
        right_path: Option<String>,
        left_schema: Schema,
        right_schema: Schema,
        columns_1: Vec<String>,
        columns_2: Vec<String>,
        strict: bool,
    ) -> Self {
        Self {
            left_table,
            right_table,
            left_path,
            right_path,
            columns_1: (
                columns_1[0].clone(),
                columns_1[1].clone(),
                columns_1[2].clone(),
            ),
            columns_2: (
                columns_2[0].clone(),
                columns_2[1].clone(),
                columns_2[2].clone(),
            ),
            strict,
            schema: overlap_schema(&left_schema, &right_schema),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_serialized(
        left_table: String,
        right_table: String,
        left_path: Option<String>,
        right_path: Option<String>,
        columns_1: (String, String, String),
        columns_2: (String, String, String),
        strict: bool,
        schema: SchemaRef,
    ) -> Self {
        Self {
            left_table,
            right_table,
            left_path,
            right_path,
            columns_1,
            columns_2,
            strict,
            schema,
        }
    }

    pub fn left_table(&self) -> &str {
        &self.left_table
    }

    pub fn right_table(&self) -> &str {
        &self.right_table
    }

    pub fn left_path(&self) -> Option<&str> {
        self.left_path.as_deref()
    }

    pub fn right_path(&self) -> Option<&str> {
        self.right_path.as_deref()
    }

    pub fn columns_1(&self) -> &(String, String, String) {
        &self.columns_1
    }

    pub fn columns_2(&self) -> &(String, String, String) {
        &self.columns_2
    }

    pub fn strict(&self) -> bool {
        self.strict
    }
}

impl Debug for SerializableOverlapProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SerializableOverlapProvider {{ left: {}, right: {} }}",
            self.left_table, self.right_table
        )
    }
}

#[derive(Clone)]
pub struct ApproachAOverlapProvider {
    left_table: String,
    right_table: String,
    left_path: Option<String>,
    right_path: Option<String>,
    columns_1: (String, String, String),
    columns_2: (String, String, String),
    strict: bool,
    schema: SchemaRef,
}

impl ApproachAOverlapProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left_table: String,
        right_table: String,
        left_path: Option<String>,
        right_path: Option<String>,
        left_schema: Schema,
        right_schema: Schema,
        columns_1: Vec<String>,
        columns_2: Vec<String>,
        strict: bool,
    ) -> Self {
        Self {
            left_table,
            right_table,
            left_path,
            right_path,
            columns_1: (
                columns_1[0].clone(),
                columns_1[1].clone(),
                columns_1[2].clone(),
            ),
            columns_2: (
                columns_2[0].clone(),
                columns_2[1].clone(),
                columns_2[2].clone(),
            ),
            strict,
            schema: overlap_schema(&left_schema, &right_schema),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_serialized(
        left_table: String,
        right_table: String,
        left_path: Option<String>,
        right_path: Option<String>,
        columns_1: (String, String, String),
        columns_2: (String, String, String),
        strict: bool,
        schema: SchemaRef,
    ) -> Self {
        Self {
            left_table,
            right_table,
            left_path,
            right_path,
            columns_1,
            columns_2,
            strict,
            schema,
        }
    }

    pub fn left_table(&self) -> &str {
        &self.left_table
    }

    pub fn right_table(&self) -> &str {
        &self.right_table
    }

    pub fn left_path(&self) -> Option<&str> {
        self.left_path.as_deref()
    }

    pub fn right_path(&self) -> Option<&str> {
        self.right_path.as_deref()
    }

    pub fn columns_1(&self) -> &(String, String, String) {
        &self.columns_1
    }

    pub fn columns_2(&self) -> &(String, String, String) {
        &self.columns_2
    }

    pub fn strict(&self) -> bool {
        self.strict
    }
}

impl Debug for ApproachAOverlapProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ApproachAOverlapProvider {{ left: {}, right: {} }}",
            self.left_table, self.right_table
        )
    }
}

#[async_trait]
impl TableProvider for ApproachAOverlapProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        build_overlap_plan_from_paths(
            &self.left_table,
            &self.right_table,
            self.left_path.as_deref(),
            self.right_path.as_deref(),
            &self.columns_1,
            &self.columns_2,
            self.strict,
            limit,
        )
        .await
    }
}

#[derive(Clone)]
pub struct ApproachBOverlapProvider {
    left_table: String,
    right_table: String,
    left_path: Option<String>,
    right_path: Option<String>,
    columns_1: (String, String, String),
    columns_2: (String, String, String),
    strict: bool,
    schema: SchemaRef,
    codec_version: u8,
}

impl ApproachBOverlapProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left_table: String,
        right_table: String,
        left_path: Option<String>,
        right_path: Option<String>,
        left_schema: Schema,
        right_schema: Schema,
        columns_1: Vec<String>,
        columns_2: Vec<String>,
        strict: bool,
        codec_version: u8,
    ) -> Self {
        Self {
            left_table,
            right_table,
            left_path,
            right_path,
            columns_1: (
                columns_1[0].clone(),
                columns_1[1].clone(),
                columns_1[2].clone(),
            ),
            columns_2: (
                columns_2[0].clone(),
                columns_2[1].clone(),
                columns_2[2].clone(),
            ),
            strict,
            schema: overlap_schema(&left_schema, &right_schema),
            codec_version,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_serialized(
        left_table: String,
        right_table: String,
        left_path: Option<String>,
        right_path: Option<String>,
        columns_1: (String, String, String),
        columns_2: (String, String, String),
        strict: bool,
        schema: SchemaRef,
        codec_version: u8,
    ) -> Self {
        Self {
            left_table,
            right_table,
            left_path,
            right_path,
            columns_1,
            columns_2,
            strict,
            schema,
            codec_version,
        }
    }

    pub fn left_table(&self) -> &str {
        &self.left_table
    }

    pub fn right_table(&self) -> &str {
        &self.right_table
    }

    pub fn left_path(&self) -> Option<&str> {
        self.left_path.as_deref()
    }

    pub fn right_path(&self) -> Option<&str> {
        self.right_path.as_deref()
    }

    pub fn columns_1(&self) -> &(String, String, String) {
        &self.columns_1
    }

    pub fn columns_2(&self) -> &(String, String, String) {
        &self.columns_2
    }

    pub fn strict(&self) -> bool {
        self.strict
    }

    pub fn codec_version(&self) -> u8 {
        self.codec_version
    }
}

impl Debug for ApproachBOverlapProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ApproachBOverlapProvider {{ left: {}, right: {}, codec_v: {} }}",
            self.left_table, self.right_table, self.codec_version
        )
    }
}

#[async_trait]
impl TableProvider for ApproachBOverlapProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        build_overlap_plan_from_paths(
            &self.left_table,
            &self.right_table,
            self.left_path.as_deref(),
            self.right_path.as_deref(),
            &self.columns_1,
            &self.columns_2,
            self.strict,
            limit,
        )
        .await
    }
}

#[async_trait]
impl TableProvider for SerializableOverlapProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        build_overlap_plan_from_paths(
            &self.left_table,
            &self.right_table,
            self.left_path.as_deref(),
            self.right_path.as_deref(),
            &self.columns_1,
            &self.columns_2,
            self.strict,
            limit,
        )
        .await
    }
}

async fn build_overlap_plan_from_paths(
    left_table: &str,
    right_table: &str,
    left_path: Option<&str>,
    right_path: Option<&str>,
    columns_1: &(String, String, String),
    columns_2: &(String, String, String),
    strict: bool,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let ctx = SessionContext::new();
    let left_path = left_path.ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(
            "overlap provider requires a left parquet path".to_string(),
        )
    })?;
    let right_path = right_path.ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(
            "overlap provider requires a right parquet path".to_string(),
        )
    })?;

    ctx.register_parquet(left_table, left_path, ParquetReadOptions::new())
        .await?;
    ctx.register_parquet(right_table, right_path, ParquetReadOptions::new())
        .await?;

    let left_schema = ctx
        .table(TableReference::from(left_table.to_string()))
        .await?
        .schema()
        .as_arrow()
        .clone();
    let right_schema = ctx
        .table(TableReference::from(right_table.to_string()))
        .await?
        .schema()
        .as_arrow()
        .clone();

    let select_left = left_schema
        .fields()
        .iter()
        .map(|f| format!("a.`{}` AS `left_{}`", f.name(), f.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let select_right = right_schema
        .fields()
        .iter()
        .map(|f| format!("b.`{}` AS `right_{}`", f.name(), f.name()))
        .collect::<Vec<_>>()
        .join(", ");

    let sign = if strict { "" } else { "=" };
    let (c1, s1, e1) = (&columns_1.0, &columns_1.1, &columns_1.2);
    let (c2, s2, e2) = (&columns_2.0, &columns_2.1, &columns_2.2);

    let mut query = format!(
        "SELECT {select_left}, {select_right} \
         FROM `{}` AS b, `{}` AS a \
         WHERE a.`{c1}` = b.`{c2}` \
         AND CAST(a.`{e1}` AS INTEGER) >{sign} CAST(b.`{s2}` AS INTEGER) \
         AND CAST(a.`{s1}` AS INTEGER) <{sign} CAST(b.`{e2}` AS INTEGER)",
        right_table, left_table,
    );
    if let Some(limit) = limit {
        query.push_str(&format!(" LIMIT {limit}"));
    }

    ctx.sql(&query).await?.create_physical_plan().await
}

fn overlap_schema(left_schema: &Schema, right_schema: &Schema) -> SchemaRef {
    let mut fields = left_schema
        .fields()
        .iter()
        .map(|f| {
            Arc::new(Field::new(
                format!("left_{}", f.name()),
                f.data_type().clone(),
                f.is_nullable(),
            ))
        })
        .collect::<Vec<_>>();
    fields.extend(right_schema.fields().iter().map(|f| {
        Arc::new(Field::new(
            format!("right_{}", f.name()),
            f.data_type().clone(),
            f.is_nullable(),
        ))
    }));
    Arc::new(Schema::new(fields))
}
