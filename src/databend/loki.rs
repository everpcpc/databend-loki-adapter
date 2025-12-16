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

use databend_driver::Row;

use crate::{
    error::AppError,
    logql::{LabelMatcher, LabelOp, LogqlExpr},
};

use super::core::{
    LogEntry, QueryBounds, SchemaConfig, TableColumn, TableRef, ensure_labels_column,
    ensure_line_column, ensure_timestamp_column, escape, line_filter_clause, matches_line_column,
    matches_named_column, missing_required_column, parse_labels_value, quote_ident,
    timestamp_literal, value_to_string, value_to_timestamp,
};

#[derive(Clone)]
pub struct LokiSchema {
    pub(crate) timestamp_col: String,
    pub(crate) labels_col: String,
    pub(crate) line_col: String,
}

impl LokiSchema {
    pub(crate) fn build_query(
        &self,
        table: &TableRef,
        expr: &LogqlExpr,
        bounds: &QueryBounds,
    ) -> Result<String, AppError> {
        let mut clauses = Vec::new();
        if let Some(start) = bounds.start_ns {
            clauses.push(format!(
                "{} >= {}",
                quote_ident(&self.timestamp_col),
                timestamp_literal(start)?
            ));
        }
        if let Some(end) = bounds.end_ns {
            clauses.push(format!(
                "{} <= {}",
                quote_ident(&self.timestamp_col),
                timestamp_literal(end)?
            ));
        }
        clauses.extend(
            expr.selectors
                .iter()
                .map(|m| label_clause_loki(m, quote_ident(&self.labels_col))),
        );
        clauses.extend(
            expr.filters
                .iter()
                .map(|f| line_filter_clause(quote_ident(&self.line_col), f)),
        );

        let where_clause = if clauses.is_empty() {
            "1=1".to_string()
        } else {
            clauses.join(" AND ")
        };

        Ok(format!(
            "SELECT {ts}, {labels}, {line} FROM {table} WHERE {where} ORDER BY {ts} {order} LIMIT {limit}",
            ts = quote_ident(&self.timestamp_col),
            labels = quote_ident(&self.labels_col),
            line = quote_ident(&self.line_col),
            table = table.fq_name(),
            where = where_clause,
            order = bounds.order.sql(),
            limit = bounds.limit
        ))
    }

    pub(crate) fn parse_row(&self, row: &Row) -> Result<LogEntry, AppError> {
        if row.values().len() < 3 {
            return Err(AppError::Internal(
                "Loki schema query must return timestamp, labels, line".into(),
            ));
        }
        let timestamp_ns = value_to_timestamp(&row.values()[0])?;
        let labels = parse_labels_value(&row.values()[1])?;
        let line = value_to_string(&row.values()[2]);
        Ok(LogEntry {
            timestamp_ns,
            labels,
            line,
        })
    }

    pub(crate) fn from_columns(
        columns: Vec<TableColumn>,
        config: &SchemaConfig,
    ) -> Result<Self, AppError> {
        if columns.is_empty() {
            return Err(AppError::Config("loki schema table has no columns".into()));
        }

        let timestamp_override = config.timestamp_column.as_deref();
        let desired_timestamp = timestamp_override.map(|value| value.to_lowercase());
        let labels_override = config.labels_column.as_deref();
        let desired_labels = labels_override.map(|value| value.to_lowercase());
        let line_override = config.line_column.as_deref();
        let desired_line = line_override.map(|value| value.to_lowercase());

        let mut timestamp = None;
        let mut labels = None;
        let mut line = None;
        for column in columns {
            let lower = column.name.to_lowercase();
            if timestamp.is_none() && matches_named_column(&desired_timestamp, &lower, "timestamp")
            {
                ensure_timestamp_column(&column)?;
                timestamp = Some(column.name.clone());
                continue;
            }
            if labels.is_none() && matches_named_column(&desired_labels, &lower, "labels") {
                ensure_labels_column(&column)?;
                labels = Some(column.name.clone());
                continue;
            }
            if line.is_none() && matches_line_column(&desired_line, &lower) {
                ensure_line_column(&column)?;
                line = Some(column.name.clone());
            }
        }

        let timestamp_col = timestamp.ok_or_else(|| {
            missing_required_column("timestamp", timestamp_override, "loki schema")
        })?;
        let labels_col = labels
            .ok_or_else(|| missing_required_column("labels", labels_override, "loki schema"))?;
        let line_col = line.ok_or_else(|| match line_override {
            Some(name) => AppError::Config(format!("line column `{name}` not found in table")),
            None => AppError::Config(
                "loki schema requires a `line` column (or use --line-column to override)".into(),
            ),
        })?;

        Ok(Self {
            timestamp_col,
            labels_col,
            line_col,
        })
    }
}

fn label_clause_loki(matcher: &LabelMatcher, column: String) -> String {
    let value = escape(&matcher.value);
    match matcher.op {
        LabelOp::Eq => format!("{column}['{key}'] = '{value}'", key = matcher.key),
        LabelOp::NotEq => format!("{column}['{key}'] != '{value}'", key = matcher.key),
        LabelOp::RegexEq => format!("match({column}['{key}'], '{value}')", key = matcher.key),
        LabelOp::RegexNotEq => {
            format!("NOT match({column}['{key}'], '{value}')", key = matcher.key)
        }
    }
}
