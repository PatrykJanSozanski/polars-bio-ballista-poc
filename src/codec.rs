use std::sync::Arc;

use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result, TableReference};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_proto::logical_plan::LogicalExtensionCodec;

use crate::operation::{
    ApproachAOverlapProvider, ApproachBOverlapProvider, SerializableOverlapProvider,
};

const OVERLAP_PROVIDER_MAGIC: &[u8] = b"POLARS_BIO_OVERLAP_PROVIDER_V1";
const OVERLAP_PROVIDER_A_MAGIC: &[u8] = b"POLARS_BIO_OVERLAP_PROVIDER_A_V1";
const OVERLAP_PROVIDER_B_MAGIC: &[u8] = b"POLARS_BIO_OVERLAP_PROVIDER_B_V1";

#[derive(Debug)]
pub struct PolarsBioBallistaLogicalCodec {
    inner: BallistaLogicalExtensionCodec,
}

impl Default for PolarsBioBallistaLogicalCodec {
    fn default() -> Self {
        Self {
            inner: BallistaLogicalExtensionCodec::default(),
        }
    }
}

impl LogicalExtensionCodec for PolarsBioBallistaLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        if buf.starts_with(OVERLAP_PROVIDER_MAGIC) {
            let mut cursor = OVERLAP_PROVIDER_MAGIC.len();
            let left_table = read_string(buf, &mut cursor)?;
            let right_table = read_string(buf, &mut cursor)?;
            let left_path = read_optional_string(buf, &mut cursor)?;
            let right_path = read_optional_string(buf, &mut cursor)?;
            let columns_1 = (
                read_string(buf, &mut cursor)?,
                read_string(buf, &mut cursor)?,
                read_string(buf, &mut cursor)?,
            );
            let columns_2 = (
                read_string(buf, &mut cursor)?,
                read_string(buf, &mut cursor)?,
                read_string(buf, &mut cursor)?,
            );
            let strict = read_bool(buf, &mut cursor)?;

            return Ok(Arc::new(SerializableOverlapProvider::from_serialized(
                left_table,
                right_table,
                left_path,
                right_path,
                columns_1,
                columns_2,
                strict,
                schema,
            )));
        }

        if buf.starts_with(OVERLAP_PROVIDER_A_MAGIC) {
            let mut cursor = OVERLAP_PROVIDER_A_MAGIC.len();
            let left_table = read_string(buf, &mut cursor)?;
            let right_table = read_string(buf, &mut cursor)?;
            let left_path = read_optional_string(buf, &mut cursor)?;
            let right_path = read_optional_string(buf, &mut cursor)?;
            let columns_1 = (
                read_string(buf, &mut cursor)?,
                read_string(buf, &mut cursor)?,
                read_string(buf, &mut cursor)?,
            );
            let columns_2 = (
                read_string(buf, &mut cursor)?,
                read_string(buf, &mut cursor)?,
                read_string(buf, &mut cursor)?,
            );
            let strict = read_bool(buf, &mut cursor)?;

            return Ok(Arc::new(ApproachAOverlapProvider::from_serialized(
                left_table,
                right_table,
                left_path,
                right_path,
                columns_1,
                columns_2,
                strict,
                schema,
            )));
        }

        if buf.starts_with(OVERLAP_PROVIDER_B_MAGIC) {
            let mut cursor = OVERLAP_PROVIDER_B_MAGIC.len();
            let codec_version = read_u8(buf, &mut cursor)?;
            let left_table = read_string(buf, &mut cursor)?;
            let right_table = read_string(buf, &mut cursor)?;
            let left_path = read_optional_string(buf, &mut cursor)?;
            let right_path = read_optional_string(buf, &mut cursor)?;
            let columns_1 = (
                read_string(buf, &mut cursor)?,
                read_string(buf, &mut cursor)?,
                read_string(buf, &mut cursor)?,
            );
            let columns_2 = (
                read_string(buf, &mut cursor)?,
                read_string(buf, &mut cursor)?,
                read_string(buf, &mut cursor)?,
            );
            let strict = read_bool(buf, &mut cursor)?;

            return Ok(Arc::new(ApproachBOverlapProvider::from_serialized(
                left_table,
                right_table,
                left_path,
                right_path,
                columns_1,
                columns_2,
                strict,
                schema,
                codec_version,
            )));
        }

        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if let Some(provider) = node.as_any().downcast_ref::<SerializableOverlapProvider>() {
            buf.extend_from_slice(OVERLAP_PROVIDER_MAGIC);
            write_string(buf, provider.left_table())?;
            write_string(buf, provider.right_table())?;
            write_optional_string(buf, provider.left_path())?;
            write_optional_string(buf, provider.right_path())?;
            let (left_contig, left_start, left_end) = provider.columns_1();
            write_string(buf, left_contig)?;
            write_string(buf, left_start)?;
            write_string(buf, left_end)?;
            let (right_contig, right_start, right_end) = provider.columns_2();
            write_string(buf, right_contig)?;
            write_string(buf, right_start)?;
            write_string(buf, right_end)?;
            write_bool(buf, provider.strict());
            return Ok(());
        }

        if let Some(provider) = node.as_any().downcast_ref::<ApproachAOverlapProvider>() {
            buf.extend_from_slice(OVERLAP_PROVIDER_A_MAGIC);
            write_string(buf, provider.left_table())?;
            write_string(buf, provider.right_table())?;
            write_optional_string(buf, provider.left_path())?;
            write_optional_string(buf, provider.right_path())?;
            let (left_contig, left_start, left_end) = provider.columns_1();
            write_string(buf, left_contig)?;
            write_string(buf, left_start)?;
            write_string(buf, left_end)?;
            let (right_contig, right_start, right_end) = provider.columns_2();
            write_string(buf, right_contig)?;
            write_string(buf, right_start)?;
            write_string(buf, right_end)?;
            write_bool(buf, provider.strict());
            return Ok(());
        }

        if let Some(provider) = node.as_any().downcast_ref::<ApproachBOverlapProvider>() {
            buf.extend_from_slice(OVERLAP_PROVIDER_B_MAGIC);
            write_u8(buf, provider.codec_version());
            write_string(buf, provider.left_table())?;
            write_string(buf, provider.right_table())?;
            write_optional_string(buf, provider.left_path())?;
            write_optional_string(buf, provider.right_path())?;
            let (left_contig, left_start, left_end) = provider.columns_1();
            write_string(buf, left_contig)?;
            write_string(buf, left_start)?;
            write_string(buf, left_end)?;
            let (right_contig, right_start, right_end) = provider.columns_2();
            write_string(buf, right_contig)?;
            write_string(buf, right_start)?;
            write_string(buf, right_end)?;
            write_bool(buf, provider.strict());
            return Ok(());
        }

        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn FileFormatFactory>> {
        self.inner.try_decode_file_format(buf, ctx)
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> Result<()> {
        self.inner.try_encode_file_format(buf, node)
    }
}

fn read_string(buf: &[u8], cursor: &mut usize) -> Result<String> {
    let len = read_u32(buf, cursor)? as usize;
    if buf.len() < *cursor + len {
        return Err(DataFusionError::Internal(
            "invalid overlap provider payload: string exceeds buffer".to_string(),
        ));
    }
    let value = std::str::from_utf8(&buf[*cursor..*cursor + len])
        .map_err(|e| DataFusionError::Internal(format!("invalid utf8 payload: {e}")))?
        .to_string();
    *cursor += len;
    Ok(value)
}

fn read_optional_string(buf: &[u8], cursor: &mut usize) -> Result<Option<String>> {
    match read_bool(buf, cursor)? {
        true => Ok(Some(read_string(buf, cursor)?)),
        false => Ok(None),
    }
}

fn read_bool(buf: &[u8], cursor: &mut usize) -> Result<bool> {
    if buf.len() <= *cursor {
        return Err(DataFusionError::Internal(
            "invalid overlap provider payload: missing bool".to_string(),
        ));
    }
    let value = buf[*cursor] != 0;
    *cursor += 1;
    Ok(value)
}

fn read_u32(buf: &[u8], cursor: &mut usize) -> Result<u32> {
    if buf.len() < *cursor + 4 {
        return Err(DataFusionError::Internal(
            "invalid overlap provider payload: missing length".to_string(),
        ));
    }
    let mut len = [0; 4];
    len.copy_from_slice(&buf[*cursor..*cursor + 4]);
    *cursor += 4;
    Ok(u32::from_le_bytes(len))
}

fn read_u8(buf: &[u8], cursor: &mut usize) -> Result<u8> {
    if buf.len() <= *cursor {
        return Err(DataFusionError::Internal(
            "invalid overlap provider payload: missing u8".to_string(),
        ));
    }
    let value = buf[*cursor];
    *cursor += 1;
    Ok(value)
}

fn write_string(buf: &mut Vec<u8>, value: &str) -> Result<()> {
    let len = u32::try_from(value.len()).map_err(|_| {
        DataFusionError::Internal("overlap provider payload string is too large".to_string())
    })?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_optional_string(buf: &mut Vec<u8>, value: Option<&str>) -> Result<()> {
    match value {
        Some(value) => {
            write_bool(buf, true);
            write_string(buf, value)
        }
        None => {
            write_bool(buf, false);
            Ok(())
        }
    }
}

fn write_bool(buf: &mut Vec<u8>, value: bool) {
    buf.push(u8::from(value));
}

fn write_u8(buf: &mut Vec<u8>, value: u8) {
    buf.push(value);
}
