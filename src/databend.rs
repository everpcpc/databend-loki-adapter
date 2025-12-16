use std::collections::BTreeMap;

use databend_driver::{Client, Row, Value};

use crate::error::AppError;

pub async fn execute_query(client: &Client, sql: &str) -> Result<Vec<Row>, AppError> {
    let conn = client.get_conn().await?;
    let rows = conn.query_all(sql).await?;
    Ok(rows)
}

#[derive(Clone)]
pub struct LogEntry {
    pub timestamp_ns: i128,
    pub labels: BTreeMap<String, String>,
    pub line: String,
}

pub fn log_entry_from_row(row: &Row) -> Result<LogEntry, AppError> {
    let values = row.values();
    if values.len() < 3 {
        return Err(AppError::Internal(
            "query must return timestamp, labels, line".into(),
        ));
    }

    let timestamp_ns = match &values[0] {
        Value::Timestamp(zoned) | Value::TimestampTz(zoned) => zoned.timestamp().as_nanosecond(),
        _ => {
            return Err(AppError::Internal(
                "timestamp column has unexpected type".into(),
            ));
        }
    };

    let labels = parse_labels_value(&values[1])?;
    let line = match &values[2] {
        Value::String(text) | Value::Variant(text) => text.clone(),
        Value::Binary(bin) => String::from_utf8_lossy(bin).to_string(),
        _ => return Err(AppError::Internal("line column has unexpected type".into())),
    };

    Ok(LogEntry {
        timestamp_ns,
        labels,
        line,
    })
}

fn parse_labels_value(value: &Value) -> Result<BTreeMap<String, String>, AppError> {
    match value {
        Value::Variant(raw) | Value::String(raw) => parse_labels_json(raw),
        Value::Map(pairs) => {
            let mut map = BTreeMap::new();
            for (key, val) in pairs {
                let key = match key {
                    Value::String(text) => text.clone(),
                    _ => return Err(AppError::Internal("labels map key must be string".into())),
                };
                let value = match val {
                    Value::String(text) => text.clone(),
                    _ => return Err(AppError::Internal("labels map value must be string".into())),
                };
                map.insert(key, value);
            }
            Ok(map)
        }
        _ => Err(AppError::Internal(
            "labels column must be VARIANT or MAP".into(),
        )),
    }
}

fn parse_labels_json(raw: &str) -> Result<BTreeMap<String, String>, AppError> {
    let map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(raw)
        .map_err(|err| AppError::Internal(format!("labels column is not valid JSON: {err}")))?;
    let mut labels = BTreeMap::new();
    for (key, value) in map {
        let value = value
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| AppError::Internal("label values must be strings".into()))?;
        labels.insert(key, value);
    }
    Ok(labels)
}
