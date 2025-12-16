use chrono::{TimeZone, Utc};

use crate::{
    error::AppError,
    logql::{LabelMatcher, LabelOp, LineFilter, LineFilterOp, LogqlExpr},
};

pub struct QueryBounds {
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub limit: u64,
    pub order: SqlOrder,
}

#[derive(Clone, Copy)]
pub enum SqlOrder {
    Asc,
    Desc,
}

pub fn build_select(
    table: &str,
    expr: &LogqlExpr,
    bounds: &QueryBounds,
) -> Result<String, AppError> {
    let mut clauses = Vec::new();
    if let Some(start) = bounds.start_ns {
        clauses.push(format!("timestamp >= {}", timestamp_literal(start)?));
    }
    if let Some(end) = bounds.end_ns {
        clauses.push(format!("timestamp <= {}", timestamp_literal(end)?));
    }

    clauses.extend(expr.selectors.iter().map(label_clause));
    clauses.extend(expr.filters.iter().map(line_filter_clause));

    let sql = format!(
        "SELECT timestamp, labels, line FROM {} WHERE {} ORDER BY timestamp {} LIMIT {}",
        table,
        if clauses.is_empty() {
            "1=1".to_string()
        } else {
            clauses.join(" AND ")
        },
        match bounds.order {
            SqlOrder::Asc => "ASC",
            SqlOrder::Desc => "DESC",
        },
        bounds.limit
    );
    Ok(sql)
}

fn label_clause(matcher: &LabelMatcher) -> String {
    let key = &matcher.key;
    let value = escape(&matcher.value);
    match matcher.op {
        LabelOp::Eq => format!("labels['{key}'] = '{value}'"),
        LabelOp::NotEq => format!("labels['{key}'] != '{value}'"),
        LabelOp::RegexEq => format!("match(labels['{key}'], '{value}')"),
        LabelOp::RegexNotEq => format!("NOT match(labels['{key}'], '{value}')"),
    }
}

fn line_filter_clause(filter: &LineFilter) -> String {
    let value = escape(&filter.value);
    match filter.op {
        LineFilterOp::Contains => format!("position('{value}' in line) > 0"),
        LineFilterOp::NotContains => format!("position('{value}' in line) = 0"),
        LineFilterOp::Regex => format!("match(line, '{value}')"),
        LineFilterOp::NotRegex => format!("NOT match(line, '{value}')"),
    }
}

fn escape(input: &str) -> String {
    input.replace('\'', "''")
}

fn timestamp_literal(ns: i64) -> Result<String, AppError> {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    let datetime = Utc
        .timestamp_opt(secs, nanos)
        .single()
        .ok_or_else(|| AppError::BadRequest("timestamp is out of range".into()))?;
    Ok(format!(
        "TIMESTAMP '{}'",
        datetime.format("%Y-%m-%d %H:%M:%S%.f")
    ))
}
