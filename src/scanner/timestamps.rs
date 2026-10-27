use chrono::{DateTime, NaiveDateTime};
use serde_json::Value;
use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom},
    path::Path,
    time::UNIX_EPOCH,
};
const CHUNK: usize = 256 * 1024;
const MAX_LINE: usize = 8 * 1024 * 1024;
const PRIMARY: &[&str] = &["timestamp", "created_at", "createdAt", "time", "date"];
const CONTAINERS: &[&str] = &["payload", "data", "message", "time"];
const NESTED: &[&str] = &[
    "timestamp",
    "created_at",
    "createdAt",
    "created",
    "time",
    "date",
    "completed",
];
pub(super) fn mtime(metadata: &std::fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
fn conversation(value: &Value) -> bool {
    fn allowed(s: &str) -> bool {
        matches!(
            s.trim().to_ascii_lowercase().replace('_', "-").as_str(),
            "user"
                | "assistant"
                | "message"
                | "custom-message"
                | "event-msg"
                | "response-item"
                | "turn-start"
                | "turn-end"
                | "turn-context"
                | "agent-message"
                | "user-message"
                | "tool-call"
                | "tool-result"
                | "tool-use"
                | "function-call"
                | "function-call-output"
                | "custom-tool-call"
                | "custom-tool-call-output"
                | "thinking"
                | "question-prompt"
                | "error"
        )
    }
    if let Some(s) = value
        .get("payload")
        .and_then(|p| p.get("type"))
        .and_then(Value::as_str)
    {
        return allowed(s);
    }
    ["type", "event_type", "event"]
        .iter()
        .any(|key| value.get(key).and_then(Value::as_str).is_some_and(allowed))
}
pub(super) fn rfc_timestamp(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.timestamp_millis())
        .filter(|t| *t > 0)
}
pub(super) fn header_timestamp(value: &Value) -> i64 {
    value
        .as_i64()
        .or_else(|| {
            value
                .as_f64()
                .filter(|n| n.is_finite() && *n > 0.0 && *n < i64::MAX as f64)
                .map(|n| n as i64)
        })
        .or_else(|| value.as_str().and_then(rfc_timestamp))
        .unwrap_or(0)
}
fn timestamp(value: &Value) -> Option<i64> {
    let number = if let Some(s) = value.as_str() {
        let s = s.trim();
        if let Some(t) = rfc_timestamp(s) {
            return Some(t);
        }
        for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"] {
            if let Ok(t) = NaiveDateTime::parse_from_str(s, format) {
                return Some(t.and_utc().timestamp_millis()).filter(|t| *t > 0);
            }
        }
        s.parse::<f64>().ok()?
    } else {
        value.as_f64()?
    };
    if !number.is_finite() || number < 1e9 {
        return None;
    }
    let millis = if number >= 1e17 {
        number / 1e6
    } else if number >= 1e14 {
        number / 1e3
    } else if number >= 1e11 {
        number
    } else {
        number * 1000.0
    };
    (millis < i64::MAX as f64).then_some(millis as i64)
}
fn extract(value: &Value, first: bool) -> Option<i64> {
    for key in PRIMARY {
        if let Some(t) = value.get(*key).and_then(timestamp) {
            return Some(t);
        }
    }
    for container in CONTAINERS {
        if let Some(value) = value.get(*container) {
            for key in NESTED {
                if let Some(t) = value.get(*key).and_then(timestamp) {
                    return Some(t);
                }
            }
        }
    }
    if first {
        for value in
            std::iter::once(value).chain(CONTAINERS.iter().filter_map(|key| value.get(*key)))
        {
            for key in ["started_at", "startedAt", "start"] {
                if let Some(t) = value.get(key).and_then(timestamp) {
                    return Some(t);
                }
            }
        }
    }
    None
}
/// Finds the final conversational timestamp with bounded reverse reads.
pub fn extract_last_active_timestamp(path: &Path, fallback_mtime: i64) -> io::Result<i64> {
    let mut file = File::open(path)?;
    let mut cursor = file.metadata()?.len();
    let mut remainder = Vec::new();
    let mut oversized = false;
    while cursor > 0 {
        let length = usize::try_from(cursor.min(CHUNK as u64)).unwrap_or(CHUNK);
        cursor -= length as u64;
        file.seek(SeekFrom::Start(cursor))?;
        let mut bytes = vec![0; length];
        file.read_exact(&mut bytes)?;
        let mut end = bytes.len();
        // Complete lines right-to-left; discard an oversized line until its actual boundary.
        while let Some(index) = bytes[..end].iter().rposition(|b| *b == b'\n') {
            let suffix = &bytes[index + 1..end];
            if !oversized && suffix.len() + remainder.len() <= MAX_LINE {
                let mut line = suffix.to_vec();
                line.extend_from_slice(&remainder);
                if let Ok(value) = serde_json::from_slice::<Value>(&line)
                    && conversation(&value)
                    && let Some(t) = extract(&value, false)
                {
                    return Ok(t);
                }
            }
            oversized = false;
            remainder.clear();
            end = index;
        }
        if !oversized {
            if end + remainder.len() > MAX_LINE {
                remainder.clear();
                oversized = true;
            } else {
                let mut prefix = bytes[..end].to_vec();
                prefix.extend_from_slice(&remainder);
                remainder = prefix;
            }
        }
    }
    if !oversized
        && let Ok(value) = serde_json::from_slice::<Value>(&remainder)
        && conversation(&value)
        && let Some(t) = extract(&value, false)
    {
        return Ok(t);
    }
    Ok(fallback_mtime)
}
/// Finds the first timestamped JSON record, including session metadata.
pub fn extract_first_timestamp(path: &Path) -> io::Result<i64> {
    let file = File::open(path)?;
    let fallback = mtime(&file.metadata()?);
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut oversized = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            break;
        }
        let take = available
            .iter()
            .position(|b| *b == b'\n')
            .map_or(available.len(), |i| i + 1);
        let complete = available[take - 1] == b'\n';
        if !oversized {
            if line.len() + take > MAX_LINE {
                line.clear();
                oversized = true;
            } else {
                line.extend_from_slice(&available[..take]);
            }
        }
        reader.consume(take);
        if complete {
            if !oversized
                && let Ok(v) = serde_json::from_slice::<Value>(&line)
                && let Some(t) = extract(&v, true)
            {
                return Ok(t);
            }
            line.clear();
            oversized = false;
        }
    }
    if !oversized
        && let Ok(v) = serde_json::from_slice::<Value>(&line)
        && let Some(t) = extract(&v, true)
    {
        return Ok(t);
    }
    Ok(fallback)
}
