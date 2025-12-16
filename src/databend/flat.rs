// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use databend_driver::{Client, Row};
use log::info;

use crate::{
    error::AppError,
    logql::{LabelMatcher, LabelOp, LogqlExpr},
};

use super::core::{
    LabelQueryBounds, LogEntry, QueryBounds, SchemaConfig, TableColumn, TableRef, ensure_line_column,
    ensure_timestamp_column, escape, execute_query, is_line_candidate, line_filter_clause,
    matches_named_column, missing_required_column, quote_ident, timestamp_literal, value_to_string,
    value_to_timestamp,
};

#[derive(Clone)]
pub struct FlatSchema {
    pub(crate) timestamp_col: String,
    pub(crate) line_col: String,
    pub(crate) label_cols: Vec<String>,
}

impl FlatSchema {
    pub(crate) fn build_query(
        &self,
        table: &TableRef,
        expr: &LogqlExpr,
        bounds: &QueryBounds,
    ) -> Result<String, AppError> {
        let ts_col = quote_ident(&self.timestamp_col);
        let line_col = quote_ident(&self.line_col);

        let mut clauses = Vec::new();
        if let Some(start) = bounds.start_ns {
            clauses.push(format!("{} >= {}", ts_col, timestamp_literal(start)?));
        }
        if let Some(end) = bounds.end_ns {
            clauses.push(format!("{} <= {}", ts_col, timestamp_literal(end)?));
        }

        for matcher in &expr.selectors {
            clauses.push(label_clause_flat(matcher, &self.label_cols)?);
        }
        clauses.extend(
            expr.filters
                .iter()
                .map(|f| line_filter_clause(line_col.clone(), f)),
        );

        let where_clause = if clauses.is_empty() {
            "1=1".to_string()
        } else {
            clauses.join(" AND ")
        };

        let mut selected = Vec::with_capacity(self.label_cols.len() + 2);
        selected.push(format!("{} AS ts_col", ts_col));
        selected.push(format!("{} AS line_col", line_col));
        for col in &self.label_cols {
            selected.push(quote_ident(col));
        }

        Ok(format!(
            "SELECT {cols} FROM {table} WHERE {where} ORDER BY {ts} {order} LIMIT {limit}",
            cols = selected.join(", "),
            table = table.fq_name(),
            where = where_clause,
            ts = ts_col,
            order = bounds.order.sql(),
            limit = bounds.limit
        ))
    }

    pub(crate) fn parse_row(&self, row: &Row) -> Result<LogEntry, AppError> {
        let values = row.values();
        if values.len() < self.label_cols.len() + 2 {
            return Err(AppError::Internal(
                "flat schema query must return timestamp, line, labels".into(),
            ));
        }
        let timestamp_ns = value_to_timestamp(&values[0])?;
        let line = value_to_string(&values[1]);
        let mut labels = BTreeMap::new();
        for (idx, key) in self.label_cols.iter().enumerate() {
            let value = value_to_string(&values[idx + 2]);
            labels.insert(key.clone(), value);
        }
        Ok(LogEntry {
            timestamp_ns,
            labels,
            line,
        })
    }

    pub(crate) fn list_labels(&self) -> Vec<String> {
        let mut labels = self.label_cols.clone();
        labels.sort();
        labels
    }

    pub(crate) async fn list_label_values(
        &self,
        client: &Client,
        table: &TableRef,
        label: &str,
        bounds: &LabelQueryBounds,
    ) -> Result<Vec<String>, AppError> {
        let column = self
            .label_cols
            .iter()
            .find(|col| col.eq_ignore_ascii_case(label))
            .ok_or_else(|| {
                AppError::BadRequest(format!("label `{label}` is not available in flat schema"))
            })?;
        let mut clauses = Vec::new();
        let ts_col = quote_ident(&self.timestamp_col);
        if let Some(start) = bounds.start_ns {
            clauses.push(format!("{ts_col} >= {}", timestamp_literal(start)?));
        }
        if let Some(end) = bounds.end_ns {
            clauses.push(format!("{ts_col} <= {}", timestamp_literal(end)?));
        }
        let label_col = quote_ident(column);
        clauses.push(format!("{label_col} IS NOT NULL"));
        let where_clause = if clauses.is_empty() {
            "1=1".to_string()
        } else {
            clauses.join(" AND ")
        };
        let sql = format!(
            "SELECT DISTINCT {label_col} FROM {table} WHERE {where} ORDER BY {label_col}",
            label_col = label_col,
            table = table.fq_name(),
            where = where_clause
        );
        let rows = execute_query(client, &sql).await?;
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(value) = row.values().first() {
                let text = value_to_string(value);
                if !text.is_empty() {
                    values.push(text);
                }
            }
        }
        Ok(values)
    }

    pub(crate) fn from_columns(
        columns: Vec<TableColumn>,
        config: &SchemaConfig,
    ) -> Result<Self, AppError> {
        if columns.is_empty() {
            return Err(AppError::Config("flat schema table has no columns".into()));
        }

        let timestamp_override = config.timestamp_column.as_deref();
        let desired_timestamp = timestamp_override.map(|value| value.to_lowercase());
        let line_override = config.line_column.as_deref();
        let desired_line = line_override.map(|value| value.to_lowercase());

        let mut timestamp_col: Option<TableColumn> = None;
        let mut explicit_line: Option<TableColumn> = None;
        let mut label_cols: Vec<TableColumn> = Vec::new();

        for column in columns {
            let lower = column.name.to_lowercase();
            if timestamp_col.is_none()
                && matches_named_column(&desired_timestamp, &lower, "timestamp")
            {
                ensure_timestamp_column(&column)?;
                timestamp_col = Some(column);
                continue;
            }

            if desired_line
                .as_ref()
                .map_or(false, |target| lower == *target)
                && explicit_line.is_none()
            {
                ensure_line_column(&column)?;
                explicit_line = Some(column);
                continue;
            }

            label_cols.push(column);
        }

        let timestamp = timestamp_col.ok_or_else(|| {
            missing_required_column("timestamp", timestamp_override, "flat schema")
        })?;

        let line = if let Some(line) = explicit_line {
            line
        } else if let Some(name) = line_override {
            return Err(AppError::Config(format!(
                "line column `{name}` not found in table"
            )));
        } else {
            let idx = label_cols
                .iter()
                .position(|col| is_line_candidate(&col.name));
            let Some(idx) = idx else {
                return Err(AppError::Config(
                    "flat schema requires --line-column or a column named request/line/message/msg"
                        .into(),
                ));
            };
            let column = label_cols.remove(idx);
            ensure_line_column(&column)?;
            column
        };

        let label_cols: Vec<String> = label_cols
            .into_iter()
            .filter(|col| col.name != line.name)
            .map(|col| col.name.clone())
            .collect();

        info!(
            "flat schema resolved: timestamp=`{}`, line=`{}`, labels={:?}",
            timestamp.name, line.name, label_cols
        );

        Ok(Self {
            timestamp_col: timestamp.name,
            line_col: line.name,
            label_cols,
        })
    }
}

fn label_clause_flat(matcher: &LabelMatcher, columns: &[String]) -> Result<String, AppError> {
    let column = columns
        .iter()
        .find(|col| col.eq_ignore_ascii_case(&matcher.key))
        .ok_or_else(|| {
            AppError::BadRequest(format!(
                "label `{}` is not a valid column in flat schema",
                matcher.key
            ))
        })?;
    let column = quote_ident(column);
    let value = escape(&matcher.value);
    Ok(match matcher.op {
        LabelOp::Eq => format!("{column} = '{value}'"),
        LabelOp::NotEq => format!("{column} != '{value}'"),
        LabelOp::RegexEq => format!("match({column}, '{value}')"),
        LabelOp::RegexNotEq => format!("NOT match({column}, '{value}')"),
    })
}
