//! A minimal JSON value, reader and writer for the crate's tooling.
//!
//! The perf tooling reads and writes `mandate-check.json`, so it needs a JSON
//! codec. The harness deliberately keeps its dependency graph lean, and the
//! one shape this codec has to reproduce faithfully is Python's `json` module
//! as Python's `json` module used it: an integer literal stays an integer
//! (so `126000` prints as `126000`, not `126000.0`), a float stays a float (so
//! `12.0` prints with its decimal point), duplicate object keys resolve last,
//! and a written object sorts its keys.

use std::collections::BTreeMap;
use std::fmt::Write as _;

/// One JSON value. Objects are [`BTreeMap`]s so a written object is
/// key-sorted, which is what `json.dumps(..., sort_keys=True)` produced.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Array(Vec<Json>),
    Object(BTreeMap<String, Json>),
}

impl Json {
    /// The object's members, or `None` for a non-object.
    pub fn as_object(&self) -> Option<&BTreeMap<String, Json>> {
        match self {
            Json::Object(map) => Some(map),
            _ => None,
        }
    }

    /// The array's items, or `None` for a non-array.
    pub fn as_array(&self) -> Option<&[Json]> {
        match self {
            Json::Array(items) => Some(items),
            _ => None,
        }
    }

    /// The string, or `None` for a non-string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(text) => Some(text),
            _ => None,
        }
    }

    /// A member of an object, or `None` for a non-object or a missing key.
    pub fn get(&self, key: &str) -> Option<&Json> {
        self.as_object().and_then(|map| map.get(key))
    }

    /// The mutable object, or `None` for a non-object.
    pub fn as_object_mut(&mut self) -> Option<&mut BTreeMap<String, Json>> {
        match self {
            Json::Object(map) => Some(map),
            _ => None,
        }
    }

    /// Whether the value is a JSON number (an integer or a float).
    pub fn is_number(&self) -> bool {
        matches!(self, Json::Int(_) | Json::Float(_))
    }

    /// The value as `f64`, or `None` for a non-number.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Json::Int(value) => Some(*value as f64),
            Json::Float(value) => Some(*value),
            _ => None,
        }
    }

    /// Python truthiness, which the report's own `bool(...)` reads use: an
    /// absent key, a null, a zero number, an empty string, array or object are
    /// false; everything else is true.
    pub fn truthy(&self) -> bool {
        match self {
            Json::Null => false,
            Json::Bool(value) => *value,
            Json::Int(value) => *value != 0,
            Json::Float(value) => *value != 0.0,
            Json::Str(text) => !text.is_empty(),
            Json::Array(items) => !items.is_empty(),
            Json::Object(map) => !map.is_empty(),
        }
    }
}

/// Python's `repr` of a float: shortest round-trip, always carrying a decimal
/// point (`90.0`, not `90`) so a reader can tell a float from an integer.
pub fn format_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    let text = format!("{value}");
    if text.contains('.') || text.contains('e') || text.contains('E') {
        text
    } else {
        format!("{text}.0")
    }
}

/// A string value, or JSON `null` for an absent one. The ported tools record
/// an optional name as `null` rather than omitting the key, so the report's
/// schema is the same shape whatever a run measured.
pub fn opt_str(value: Option<&str>) -> Json {
    match value {
        Some(text) => Json::Str(text.to_string()),
        None => Json::Null,
    }
}

/// A number value, or JSON `null` for an absent one.
pub fn opt_float(value: Option<f64>) -> Json {
    match value {
        Some(number) => Json::Float(number),
        None => Json::Null,
    }
}

/// An array of strings.
pub fn str_list(items: &[String]) -> Json {
    Json::Array(items.iter().cloned().map(Json::Str).collect())
}

/// An object value built from a borrowed map, cloning each member.
pub fn object(map: &BTreeMap<String, Json>) -> Json {
    Json::Object(map.clone())
}

/// An object value built from an owned member list.
pub fn object_map(map: BTreeMap<String, Json>) -> Json {
    Json::Object(map)
}

/// Python's `json.dumps(value)`: the compact form, with `", "` and `": "`
/// separators, which is what the runner hands the plotter's `--run-values`.
pub fn to_compact(value: &Json) -> String {
    let mut out = String::new();
    write_compact(&mut out, value);
    out
}

fn write_compact(out: &mut String, value: &Json) {
    match value {
        Json::Null => out.push_str("null"),
        Json::Bool(true) => out.push_str("true"),
        Json::Bool(false) => out.push_str("false"),
        Json::Int(number) => {
            let _ = write!(out, "{number}");
        }
        Json::Float(number) => out.push_str(&format_float(*number)),
        Json::Str(text) => write_string(out, text),
        Json::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                write_compact(out, item);
            }
            out.push(']');
        }
        Json::Object(map) => {
            out.push('{');
            for (index, (key, item)) in map.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                write_string(out, key);
                out.push_str(": ");
                write_compact(out, item);
            }
            out.push('}');
        }
    }
}

/// The compact form of an object, preserving the member order given.
pub fn object_compact(members: &[(String, Json)]) -> String {
    let mut out = String::from("{");
    for (index, (key, value)) in members.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        write_string(&mut out, key);
        out.push_str(": ");
        write_compact(&mut out, value);
    }
    out.push('}');
    out
}

/// Read and parse one JSON document, with the failure Python's `json.load`
/// raised as a sentence.
pub fn parse_document(path: &std::path::Path) -> Result<Json, String> {
    let text = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
    parse(&text)
}

/// Parse one JSON document. Trailing non-whitespace is an error, so a
/// truncated document cannot be read as a shorter one.
pub fn parse(text: &str) -> Result<Json, String> {
    let bytes = text.as_bytes();
    let mut parser = Parser { bytes, pos: 0 };
    let value = parser.value()?;
    parser.skip_whitespace();
    if parser.pos != bytes.len() {
        return Err(format!("trailing content at byte {}", parser.pos));
    }
    Ok(value)
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    fn skip_whitespace(&mut self) {
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn expect(&mut self, byte: u8) -> Result<(), String> {
        if self.peek() == Some(byte) {
            self.pos += 1;
            Ok(())
        } else {
            Err(format!(
                "expected {:?} at byte {}, found {:?}",
                byte as char,
                self.pos,
                self.peek().map(|b| b as char)
            ))
        }
    }

    fn value(&mut self) -> Result<Json, String> {
        self.skip_whitespace();
        match self.peek() {
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(b'"') => Ok(Json::Str(self.string()?)),
            Some(b't') => {
                self.literal("true")?;
                Ok(Json::Bool(true))
            }
            Some(b'f') => {
                self.literal("false")?;
                Ok(Json::Bool(false))
            }
            Some(b'n') => {
                self.literal("null")?;
                Ok(Json::Null)
            }
            Some(b'-') | Some(b'0'..=b'9') => self.number(),
            other => Err(format!(
                "unexpected {:?} at byte {}",
                other.map(|b| b as char),
                self.pos
            )),
        }
    }

    fn literal(&mut self, word: &str) -> Result<(), String> {
        if self.bytes[self.pos..].starts_with(word.as_bytes()) {
            self.pos += word.len();
            Ok(())
        } else {
            Err(format!("expected {word} at byte {}", self.pos))
        }
    }

    fn object(&mut self) -> Result<Json, String> {
        self.expect(b'{')?;
        let mut map = BTreeMap::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Json::Object(map));
        }
        loop {
            self.skip_whitespace();
            let key = self.string()?;
            self.skip_whitespace();
            self.expect(b':')?;
            let value = self.value()?;
            // Insert in document order so a duplicated key keeps the last
            // value, which is what Python's `json.loads` does.
            map.insert(key, value);
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(Json::Object(map));
                }
                other => {
                    return Err(format!(
                        "expected ',' or '}}' at byte {}, found {:?}",
                        self.pos,
                        other.map(|b| b as char)
                    ));
                }
            }
        }
    }

    fn array(&mut self) -> Result<Json, String> {
        self.expect(b'[')?;
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(Json::Array(items));
        }
        loop {
            items.push(self.value()?);
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    return Ok(Json::Array(items));
                }
                other => {
                    return Err(format!(
                        "expected ',' or ']' at byte {}, found {:?}",
                        self.pos,
                        other.map(|b| b as char)
                    ));
                }
            }
        }
    }

    fn string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let byte = self
                .peek()
                .ok_or_else(|| "unterminated string".to_string())?;
            match byte {
                b'"' => {
                    self.pos += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.pos += 1;
                    let escape = self
                        .peek()
                        .ok_or_else(|| "unterminated escape".to_string())?;
                    self.pos += 1;
                    match escape {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{0008}'),
                        b'f' => out.push('\u{000c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => out.push(self.unicode_escape()?),
                        other => {
                            return Err(format!("unknown escape \\{}", other as char));
                        }
                    }
                }
                _ => {
                    // A multi-byte UTF-8 sequence is copied a byte at a time,
                    // which is only valid because the source is already valid
                    // UTF-8 and no delimiter byte equals a continuation byte.
                    let start = self.pos;
                    let width = utf8_width(byte);
                    let end = (start + width).min(self.bytes.len());
                    out.push_str(
                        std::str::from_utf8(&self.bytes[start..end])
                            .map_err(|error| format!("invalid UTF-8: {error}"))?,
                    );
                    self.pos = end;
                }
            }
        }
    }

    fn unicode_escape(&mut self) -> Result<char, String> {
        let digits = self
            .bytes
            .get(self.pos..self.pos + 4)
            .ok_or_else(|| "truncated \\u escape".to_string())?;
        let text =
            std::str::from_utf8(digits).map_err(|error| format!("invalid \\u escape: {error}"))?;
        let code =
            u32::from_str_radix(text, 16).map_err(|_| format!("invalid \\u escape {text:?}"))?;
        self.pos += 4;
        // A surrogate pair is two escapes; the tool's inputs are ASCII, so a
        // lone surrogate is decoded as the replacement character.
        char::from_u32(code).ok_or_else(|| format!("invalid code point U+{code:04X}"))
    }

    fn number(&mut self) -> Result<Json, String> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        while self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.pos += 1;
        }
        let mut is_float = false;
        if self.peek() == Some(b'.') {
            is_float = true;
            self.pos += 1;
            while self.peek().is_some_and(|b| b.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        if self.peek().is_some_and(|b| b == b'e' || b == b'E') {
            is_float = true;
            self.pos += 1;
            if self.peek().is_some_and(|b| b == b'+' || b == b'-') {
                self.pos += 1;
            }
            while self.peek().is_some_and(|b| b.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        let text = std::str::from_utf8(&self.bytes[start..self.pos]).map_err(|e| e.to_string())?;
        if is_float {
            text.parse::<f64>()
                .map(Json::Float)
                .map_err(|error| format!("invalid number {text:?}: {error}"))
        } else {
            text.parse::<i64>()
                .map(Json::Int)
                .map_err(|error| format!("invalid number {text:?}: {error}"))
        }
    }
}

fn utf8_width(byte: u8) -> usize {
    if byte < 0x80 {
        1
    } else if byte >> 5 == 0b110 {
        2
    } else if byte >> 4 == 0b1110 {
        3
    } else {
        4
    }
}

/// Serialise one value the way `json.dumps(value, indent=2, sort_keys=True)`
/// did: key-sorted objects, two-space indentation, and no trailing newline.
pub fn to_string(value: &Json) -> String {
    let mut out = String::new();
    write_value(&mut out, value, 0);
    out
}

fn write_value(out: &mut String, value: &Json, indent: usize) {
    match value {
        Json::Null => out.push_str("null"),
        Json::Bool(true) => out.push_str("true"),
        Json::Bool(false) => out.push_str("false"),
        Json::Int(number) => {
            let _ = write!(out, "{number}");
        }
        Json::Float(number) => out.push_str(&format_float(*number)),
        Json::Str(text) => write_string(out, text),
        Json::Array(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push_str("[\n");
            for (index, item) in items.iter().enumerate() {
                out.push_str(&" ".repeat((indent + 1) * 2));
                write_value(out, item, indent + 1);
                if index + 1 < items.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&" ".repeat(indent * 2));
            out.push(']');
        }
        Json::Object(map) => {
            if map.is_empty() {
                out.push_str("{}");
                return;
            }
            out.push_str("{\n");
            for (index, (key, item)) in map.iter().enumerate() {
                out.push_str(&" ".repeat((indent + 1) * 2));
                write_string(out, key);
                out.push_str(": ");
                write_value(out, item, indent + 1);
                if index + 1 < map.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&" ".repeat(indent * 2));
            out.push('}');
        }
    }
}

fn write_string(out: &mut String, text: &str) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000c}' => out.push_str("\\f"),
            other if (other as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", other as u32);
            }
            other => out.push(other),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_stay_integers_and_floats_keep_their_point() {
        assert_eq!(to_string(&Json::Int(126000)), "126000");
        assert_eq!(format_float(12.0), "12.0");
        assert_eq!(format_float(0.98), "0.98");
        assert_eq!(format_float(-0.0), "-0.0");
    }

    #[test]
    fn a_duplicate_key_keeps_the_last_value() {
        let value = parse(r#"{"a": 1, "a": 2}"#).expect("parses");
        assert_eq!(value.get("a"), Some(&Json::Int(2)));
    }

    #[test]
    fn a_truncated_document_is_refused() {
        assert!(parse(r#"{"a": 1"#).is_err());
        assert!(parse(r#"[1, 2"#).is_err());
    }

    #[test]
    fn a_written_object_is_key_sorted() {
        let mut map = BTreeMap::new();
        map.insert("b".to_string(), Json::Int(2));
        map.insert("a".to_string(), Json::Array(vec![Json::Int(1)]));
        let text = to_string(&Json::Object(map));
        assert_eq!(text, "{\n  \"a\": [\n    1\n  ],\n  \"b\": 2\n}");
        assert_eq!(
            parse(&text).expect("round trips"),
            parse(&text).expect("round trips")
        );
    }

    #[test]
    fn a_string_round_trips_through_escapes() {
        let value = Json::Str("a\"b\\c\nd".to_string());
        let text = to_string(&value);
        assert_eq!(parse(&text).expect("parses"), value);
    }
}
