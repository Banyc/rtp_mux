//! The JSON and text-repr codec the ported plot tool needs.
//!
//! `netem-tools mandate-plot` (the port of `tools/mandate_plot.py`, now deleted)
//! reads two kinds of JSON document and writes a third,
//! and the three are not interchangeable:
//!
//! - its **inputs** (the panel declaration, and the runner's per-arm `MANDATE`
//!   measurements and censoring readings) are read with `json.loads`, which
//!   **preserves the document's key order** -- and the order is behaviour here:
//!   the run's own key order is the order the guards are named in on a bound's
//!   label, and the order its arms are enumerated in is the order their bars are
//!   drawn in;
//! - its **output** is written with `json.dumps(..., sort_keys=True)`, so the
//!   `<desc class="panel-summary">` an SVG carries is key-sorted whatever order
//!   the document was built in.
//!
//! [`crate::tools::json`]'s object is a `BTreeMap`, which is exactly right for
//! the sorted side and exactly wrong for the ordered one, so this module carries
//! its own value with an insertion-ordered object and sorts at write time.
//!
//! The float spelling is the other half. `json.dumps` writes a float with
//! `repr`, and Rust's `{}` is *nearly* that: both are shortest-round-trip, but
//! they disagree at the exponents (`repr(1e16)` is `1e+16`, `{}` is
//! `10000000000000000`). So the spelling comes from
//! [`crate::tools::pyformat::format_float`] with the empty presentation type,
//! which is the measured implementation of Python's own `repr`.

use std::fmt::Write as _;

use crate::tools::pyformat;

/// One JSON value, with objects in document order.
#[derive(Debug, Clone, PartialEq)]
pub enum J {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Arr(Vec<J>),
    Obj(Vec<(String, J)>),
}

impl J {
    /// A member of an object, or `None` for a non-object or a missing key.
    pub fn get(&self, key: &str) -> Option<&J> {
        match self {
            J::Obj(members) => members
                .iter()
                .rev()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value),
            _ => None,
        }
    }

    /// The array's items, or `None` for a non-array.
    pub fn as_arr(&self) -> Option<&[J]> {
        match self {
            J::Arr(items) => Some(items),
            _ => None,
        }
    }

    /// The object's members in document order, or `None` for a non-object.
    pub fn as_obj(&self) -> Option<&[(String, J)]> {
        match self {
            J::Obj(members) => Some(members),
            _ => None,
        }
    }

    /// The string, or `None` for a non-string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            J::Str(text) => Some(text),
            _ => None,
        }
    }

    /// The value as `f64`, or `None` for a non-number.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            J::Int(value) => Some(*value as f64),
            J::Float(value) => Some(*value),
            _ => None,
        }
    }

    /// The value as `i64`, or `None` for a non-integer number.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            J::Int(value) => Some(*value),
            _ => None,
        }
    }

    /// Python's `type(value).__name__`, which the refusals print.
    pub fn type_name(&self) -> &'static str {
        match self {
            J::Null => "NoneType",
            J::Bool(_) => "bool",
            J::Int(_) => "int",
            J::Float(_) => "float",
            J::Str(_) => "str",
            J::Arr(_) => "list",
            J::Obj(_) => "dict",
        }
    }

    /// Python truthiness.
    pub fn truthy(&self) -> bool {
        match self {
            J::Null => false,
            J::Bool(value) => *value,
            J::Int(value) => *value != 0,
            J::Float(value) => *value != 0.0,
            J::Str(text) => !text.is_empty(),
            J::Arr(items) => !items.is_empty(),
            J::Obj(members) => !members.is_empty(),
        }
    }

    /// Python's `repr()` of the value, in the spelling `repr` uses: a string
    /// quoted and escaped, a float shortest-round-trip, a bool `True`/`False`,
    /// a list in brackets, a dict in braces.
    pub fn repr(&self) -> String {
        match self {
            J::Null => "None".to_string(),
            J::Bool(true) => "True".to_string(),
            J::Bool(false) => "False".to_string(),
            J::Int(value) => value.to_string(),
            J::Float(value) => repr_float(*value),
            J::Str(text) => repr_str(text),
            J::Arr(items) => {
                let inner: Vec<String> = items.iter().map(J::repr).collect();
                format!("[{}]", inner.join(", "))
            }
            J::Obj(members) => {
                let inner: Vec<String> = members
                    .iter()
                    .map(|(key, value)| format!("{}: {}", repr_str(key), value.repr()))
                    .collect();
                format!("{{{}}}", inner.join(", "))
            }
        }
    }
}

/// Python's `repr()` of a string: quoted, escaped, and quoted with the other
/// mark when the string contains one quote and not the other.
pub fn repr_str(text: &str) -> String {
    let has_single = text.contains('\'');
    let has_double = text.contains('"');
    let quote = if has_single && !has_double { '"' } else { '\'' };
    let mut out = String::with_capacity(text.len() + 2);
    out.push(quote);
    for character in text.chars() {
        match character {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other if other == quote => {
                out.push('\\');
                out.push(other);
            }
            other if (other as u32) < 0x20 || other as u32 == 0x7f => {
                let _ = write!(out, "\\x{:02x}", other as u32);
            }
            other if !is_printable(other) => {
                let code = other as u32;
                if code <= 0xffff {
                    let _ = write!(out, "\\u{code:04x}");
                } else {
                    let _ = write!(out, "\\U{code:08x}");
                }
            }
            other => out.push(other),
        }
    }
    out.push(quote);
    out
}

/// Whether a character counts as printable for `repr`, in the sense that
/// matters here: the tooling's own labels are Latin text and a spacing or a
/// formatting character is the case the escape exists for.
fn is_printable(character: char) -> bool {
    if character.is_ascii_graphic() || character == ' ' {
        return true;
    }
    // A rough unicode "printable" test: letters, marks, numbers, punctuation,
    // symbols and spaces are printable; control, format, surrogate and
    // unassigned characters are not.
    !character.is_control()
}

/// Python's `repr()` of a list of strings: `['a', 'b']`. The ported refusals
/// print a collection this way (`{sorted(names)}`), so a Rust `{:?}` -- which
/// quotes with `"` -- would spell the same list differently.
pub fn py_list(items: &[String]) -> String {
    let listed: Vec<String> = items.iter().map(|item| repr_str(item)).collect();
    format!("[{}]", listed.join(", "))
}

/// Python's `repr()` of a list of floats: `[1.0, 2.0]`.
pub fn py_float_list(items: &[f64]) -> String {
    let listed: Vec<String> = items.iter().map(|item| repr_float(*item)).collect();
    format!("[{}]", listed.join(", "))
}

/// Python's `repr()` of a float, which is `str()` of one.
pub fn repr_float(value: f64) -> String {
    pyformat::format_float(value, "").unwrap_or_else(|_| format!("{value}"))
}

/// Python's `html.escape(text, quote=True)`.
pub fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            other => out.push(other),
        }
    }
    out
}

/// Python's `html.unescape(text)`, over the entities `escape` can produce (and
/// the numeric forms), which is the whole of what the tooling ever reads back.
pub fn unescape(text: &str) -> String {
    let bytes: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != '&' {
            out.push(bytes[index]);
            index += 1;
            continue;
        }
        let rest: String = bytes[index..].iter().take(12).collect();
        let mut matched = None;
        for (entity, replacement) in [
            ("&amp;", "&"),
            ("&lt;", "<"),
            ("&gt;", ">"),
            ("&quot;", "\""),
            ("&#x27;", "'"),
            ("&apos;", "'"),
            ("&nbsp;", "\u{a0}"),
        ] {
            if rest.starts_with(entity) {
                matched = Some((entity.len(), replacement.to_string()));
                break;
            }
        }
        if let Some((width, replacement)) = matched {
            out.push_str(&replacement);
            index += width;
            continue;
        }
        if let Some((width, code)) = numeric_entity(&rest) {
            out.push(char::from_u32(code).unwrap_or('\u{fffd}'));
            index += width;
            continue;
        }
        out.push('&');
        index += 1;
    }
    out
}

fn numeric_entity(rest: &str) -> Option<(usize, u32)> {
    let body = rest.strip_prefix("&#")?;
    let (digits, radix, prefix) = match body.strip_prefix(['x', 'X']) {
        Some(hex) => (hex, 16, 2),
        None => (body, 10, 1),
    };
    // `&#` is two characters, plus an optional `x`.
    let mut text = String::new();
    for character in digits.chars() {
        if character.is_digit(radix) {
            text.push(character);
        } else {
            break;
        }
    }
    if text.is_empty() {
        return None;
    }
    // A numeric reference without its semicolon is accepted by Python, so it is
    // accepted here too.
    let width = 2 + prefix + text.len() + usize::from(digits[text.len()..].starts_with(';'));
    let code = u32::from_str_radix(&text, radix).ok()?;
    Some((width, code))
}

/// Parse one JSON document, keeping object keys in document order.
pub fn parse(text: &str) -> Result<J, String> {
    let bytes = text.as_bytes();
    let mut parser = Parser { bytes, pos: 0 };
    let value = parser.value()?;
    parser.skip_whitespace();
    if parser.pos != bytes.len() {
        return Err(parser.fail_at("Extra data", parser.pos));
    }
    Ok(value)
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    /// Python's `json.JSONDecodeError` text: the scanner's own message, then
    /// the position as `line`/`column`/`char`. The message is what a caller
    /// reads, and a port that answered a *different* sentence for the same
    /// malformed document would not be the same tool.
    fn fail_at(&self, message: &str, pos: usize) -> String {
        let chars: Vec<char> = String::from_utf8_lossy(self.bytes).chars().collect();
        let pos = pos.min(chars.len());
        let line = 1 + chars[..pos].iter().filter(|c| **c == '\n').count();
        let column = match chars[..pos].iter().rposition(|c| *c == '\n') {
            Some(index) => pos - index,
            None => pos + 1,
        };
        format!("{message}: line {line} column {column} (char {pos})")
    }
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
            Err(self.fail_at(&format!("Expecting {:?} delimiter", byte as char), self.pos))
        }
    }

    fn value(&mut self) -> Result<J, String> {
        self.skip_whitespace();
        match self.peek() {
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(b'"') => Ok(J::Str(self.string()?)),
            Some(b't') => {
                self.literal("true")?;
                Ok(J::Bool(true))
            }
            Some(b'f') => {
                self.literal("false")?;
                Ok(J::Bool(false))
            }
            Some(b'n') => {
                self.literal("null")?;
                Ok(J::Null)
            }
            Some(b'-') | Some(b'0'..=b'9') => self.number(),
            _ => Err(self.fail_at("Expecting value", self.pos)),
        }
    }

    fn literal(&mut self, word: &str) -> Result<(), String> {
        if self.bytes[self.pos..].starts_with(word.as_bytes()) {
            self.pos += word.len();
            Ok(())
        } else {
            Err(self.fail_at("Expecting value", self.pos))
        }
    }

    fn object(&mut self) -> Result<J, String> {
        self.expect(b'{')?;
        let mut members: Vec<(String, J)> = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(J::Obj(members));
        }
        loop {
            self.skip_whitespace();
            if self.peek() != Some(b'"') {
                return Err(self.fail_at(
                    "Expecting property name enclosed in double quotes",
                    self.pos,
                ));
            }
            let key = self.string()?;
            self.skip_whitespace();
            self.expect(b':')?;
            let value = self.value()?;
            // A duplicated key keeps its *first* position and its last value,
            // which is what Python's dict does.
            if let Some(existing) = members.iter_mut().find(|(name, _)| *name == key) {
                existing.1 = value;
            } else {
                members.push((key, value));
            }
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(J::Obj(members));
                }
                _ => return Err(self.fail_at("Expecting ',' delimiter", self.pos)),
            }
        }
    }

    fn array(&mut self) -> Result<J, String> {
        self.expect(b'[')?;
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(J::Arr(items));
        }
        loop {
            items.push(self.value()?);
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    return Ok(J::Arr(items));
                }
                _ => return Err(self.fail_at("Expecting ',' delimiter", self.pos)),
            }
        }
    }

    fn string(&mut self) -> Result<String, String> {
        let start = self.pos;
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let Some(byte) = self.peek() else {
                return Err(self.fail_at("Unterminated string starting at", start));
            };
            if byte < 0x20 {
                return Err(
                    self.fail_at(&format!("Invalid control character {byte:?} at"), self.pos)
                );
            }
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
        char::from_u32(code).ok_or_else(|| format!("invalid code point U+{code:04X}"))
    }

    fn number(&mut self) -> Result<J, String> {
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
        if !is_float && let Ok(value) = text.parse::<i64>() {
            return Ok(J::Int(value));
        }
        text.parse::<f64>()
            .map(J::Float)
            .map_err(|error| format!("invalid number {text:?}: {error}"))
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

/// Serialise a value the way Python's `json.dumps` did: `sort_keys` sorts each
/// object's keys, and `indent` of `None` writes the compact form with `", "`
/// and `": "` separators while `Some(n)` writes Python's indented form.
pub fn dumps(value: &J, sort_keys: bool, indent: Option<usize>) -> String {
    let mut out = String::new();
    match indent {
        None => write_compact(&mut out, value, sort_keys),
        Some(step) => write_indented(&mut out, value, sort_keys, step, 0),
    }
    out
}

fn write_compact(out: &mut String, value: &J, sort_keys: bool) {
    match value {
        J::Null => out.push_str("null"),
        J::Bool(true) => out.push_str("true"),
        J::Bool(false) => out.push_str("false"),
        J::Int(number) => {
            let _ = write!(out, "{number}");
        }
        J::Float(number) => out.push_str(&json_float(*number)),
        J::Str(text) => write_string(out, text),
        J::Arr(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                write_compact(out, item, sort_keys);
            }
            out.push(']');
        }
        J::Obj(members) => {
            out.push('{');
            for (index, (key, item)) in ordered(members, sort_keys).iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                write_string(out, key);
                out.push_str(": ");
                write_compact(out, item, sort_keys);
            }
            out.push('}');
        }
    }
}

fn write_indented(out: &mut String, value: &J, sort_keys: bool, step: usize, depth: usize) {
    match value {
        J::Arr(items) if !items.is_empty() => {
            out.push_str("[\n");
            for (index, item) in items.iter().enumerate() {
                out.push_str(&" ".repeat((depth + 1) * step));
                write_indented(out, item, sort_keys, step, depth + 1);
                if index + 1 < items.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&" ".repeat(depth * step));
            out.push(']');
        }
        J::Obj(members) if !members.is_empty() => {
            out.push_str("{\n");
            let ordered = ordered(members, sort_keys);
            for (index, (key, item)) in ordered.iter().enumerate() {
                out.push_str(&" ".repeat((depth + 1) * step));
                write_string(out, key);
                out.push_str(": ");
                write_indented(out, item, sort_keys, step, depth + 1);
                if index + 1 < ordered.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&" ".repeat(depth * step));
            out.push('}');
        }
        other => write_compact(out, other, sort_keys),
    }
}

fn ordered(members: &[(String, J)], sort_keys: bool) -> Vec<(&String, &J)> {
    let mut entries: Vec<(&String, &J)> = members.iter().map(|(k, v)| (k, v)).collect();
    if sort_keys {
        entries.sort_by(|a, b| a.0.cmp(b.0));
    }
    entries
}

/// A float as `json.dumps` writes it: `repr` for a finite value, and the
/// `Infinity`/`NaN` tokens for the rest.
fn json_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }
    repr_float(value)
}

/// `json.dumps`'s default `ensure_ascii=True`: every character outside ASCII is
/// written as a `\uXXXX` escape, an astral one as its surrogate pair. The
/// panel summary carries a `\u00b1` in a band label and an em dash in its
/// reading, and a document written with the literal characters is *not* the
/// document Python writes -- so a summary read back and compared would differ
/// from the one a Python run produced on nothing but spelling.
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
            other if (other as u32) < 0x7f => out.push(other),
            other => {
                let code = other as u32;
                if code <= 0xffff {
                    let _ = write!(out, "\\u{code:04x}");
                } else {
                    let offset = code - 0x10000;
                    let high = 0xd800 + (offset >> 10);
                    let low = 0xdc00 + (offset & 0x3ff);
                    let _ = write!(out, "\\u{high:04x}\\u{low:04x}");
                }
            }
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn objects_keep_their_document_order_and_sort_on_request() {
        let value = parse(r#"{"b": 1, "a": 2}"#).expect("parses");
        assert_eq!(dumps(&value, false, None), r#"{"b": 1, "a": 2}"#);
        assert_eq!(dumps(&value, true, None), r#"{"a": 2, "b": 1}"#);
        // Vacuity: the ordered and sorted spellings differ on this input, so
        // the test measures the ordering rather than the writer's willingness
        // to produce a document.
        assert_ne!(dumps(&value, false, None), dumps(&value, true, None));
    }

    #[test]
    fn a_float_is_written_with_pythons_repr() {
        assert_eq!(json_float(250.0), "250.0");
        assert_eq!(json_float(0.995), "0.995");
        assert_eq!(json_float(1e16), "1e+16");
        assert_eq!(json_float(-0.0), "-0.0");
        assert_eq!(repr_float(1e-5), "1e-05");
        // The shortest round trip, not a fixed number of digits.
        assert_eq!(repr_float(0.1 + 0.2), "0.30000000000000004");
    }

    #[test]
    fn a_strings_repr_picks_the_quote_python_picks() {
        assert_eq!(repr_str("plain"), "'plain'");
        assert_eq!(repr_str("has 'one"), "\"has 'one\"");
        assert_eq!(repr_str("has 'and \" both"), "'has \\'and \" both'");
    }

    #[test]
    fn escaping_and_unescaping_round_trip_the_panels_own_markup() {
        let text = "a & b < c > d \" e ' f";
        let escaped = escape(text);
        assert_eq!(escaped, "a &amp; b &lt; c &gt; d &quot; e &#x27; f");
        assert_eq!(unescape(&escaped), text);
        assert_eq!(unescape("&#x2212;"), "\u{2212}");
    }

    #[test]
    fn a_list_is_spelled_the_way_python_spells_it() {
        assert_eq!(
            py_list(&["clean".to_string(), "hostile".to_string()]),
            "['clean', 'hostile']"
        );
        assert_eq!(py_float_list(&[1.0, 0.995]), "[1.0, 0.995]");
        // Vacuity: Rust's own debug spelling is a different string, which is
        // the difference this exists to close.
        assert_ne!(
            py_list(&["clean".to_string()]),
            format!("{:?}", vec!["clean".to_string()])
        );
    }

    #[test]
    fn a_non_ascii_character_is_escaped_the_way_json_dumps_escapes_it() {
        let value = J::Obj(vec![(
            "label".to_string(),
            J::Str("a \u{b1} b \u{2014} c".to_string()),
        )]);
        assert_eq!(
            dumps(&value, false, None),
            "{\"label\": \"a \\u00b1 b \\u2014 c\"}"
        );
        // Vacuity: the literal spelling is a different document, which is the
        // difference `ensure_ascii` is here to avoid.
        assert_ne!(
            dumps(&value, false, None),
            "{\"label\": \"a \u{b1} b \u{2014} c\"}"
        );
        assert_eq!(
            dumps(&J::Str("\u{1f600}".to_string()), false, None),
            "\"\\ud83d\\ude00\""
        );
    }

    #[test]
    fn the_indented_form_is_pythons_indent_two() {
        let value = parse(r#"{"b": {"c": 1}, "a": []}"#).expect("parses");
        assert_eq!(
            dumps(&value, true, Some(2)),
            "{\n  \"a\": [],\n  \"b\": {\n    \"c\": 1\n  }\n}"
        );
    }
}
