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

use databend_driver::Row;
use serde::Serialize;
use serde_json;

use crate::{
    databend::SchemaAdapter,
    error::AppError,
};

pub(crate) fn rows_to_streams(
    schema: &SchemaAdapter,
    rows: Vec<Row>,
) -> Result<Vec<LokiStream>, AppError> {
    let mut buckets: BTreeMap<String, StreamBucket> = BTreeMap::new();
    for row in rows {
        let entry = schema.parse_row(&row)?;
        let key = serde_json::to_string(&entry.labels)
            .map_err(|err| AppError::Internal(format!("failed to encode labels: {err}")))?;
        let bucket = buckets
            .entry(key)
            .or_insert_with(|| StreamBucket::new(entry.labels.clone()));
        bucket.values.push((entry.timestamp_ns, entry.line.clone()));
    }

    let mut result = Vec::with_capacity(buckets.len());
    for bucket in buckets.into_values() {
        result.push(bucket.into_stream());
    }
    Ok(result)
}

struct StreamBucket {
    labels: BTreeMap<String, String>,
    values: Vec<(i128, String)>,
}

impl StreamBucket {
    fn new(labels: BTreeMap<String, String>) -> Self {
        Self {
            labels,
            values: Vec::new(),
        }
    }

    fn into_stream(mut self) -> LokiStream {
        self.values.sort_by_key(|(ts, _)| *ts);
        let values = self
            .values
            .into_iter()
            .map(|(ts, line)| [ts.to_string(), line])
            .collect();
        LokiStream {
            stream: self.labels,
            values,
        }
    }
}

#[derive(Serialize)]
pub(crate) struct LokiResponse {
    status: &'static str,
    data: LokiData,
}

impl LokiResponse {
    pub(crate) fn success(streams: Vec<LokiStream>) -> Self {
        Self {
            status: "success",
            data: LokiData {
                result_type: "streams",
                result: streams,
            },
        }
    }
}

#[derive(Serialize)]
struct LokiData {
    #[serde(rename = "resultType")]
    result_type: &'static str,
    result: Vec<LokiStream>,
}

#[derive(Serialize)]
pub(crate) struct LokiStream {
    stream: BTreeMap<String, String>,
    values: Vec<[String; 2]>,
}
