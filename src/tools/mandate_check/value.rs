//! The measurement values a producer's line carries, as Python read them.
//!
//! The runner's report embeds each parsed value verbatim, and the reader of
//! that report is a `mandate-check/10` consumer that expects the same JSON
//! types the Python runner wrote: an integer token is an integer, a finite
//! float token is a float, and a token that is neither is the token's own
//! text. So the coercion lives here rather than at each call site, and the
//! report's own spelling of a number is Python's.

use crate::tools::json::Json;
use crate::tools::pyformat;
use crate::tools::pyjson;

/// A unit suffix a producer may attach to a measurement (`12345B`, `12.3s`).
pub const ARM_UNIT_SUFFIXES: [char; 2] = ['B', 's'];

/// Python's `int(text)`: an optional sign, then digits, with `_` permitted
/// between them. Returns `None` when the token is not an integer literal.
///
/// A literal that does not fit an `i64` is not an integer this crate can
/// carry, so it is returned as a float instead of being refused outright: the
/// producers print byte counts and sample counts, which are far inside the
/// range, and a report that dropped such a value entirely would be worse than
/// one that lost the last bits of a value nothing prints.
fn py_int(text: &str) -> Option<i64> {
    strip_underscores(text)?.parse::<i64>().ok()
}

/// Python's `float(text)` for the spellings a producer's `format!` prints.
///
/// `inf`, `nan` and their long spellings are accepted the way Python accepts
/// them, and a literal with `_` separators has them removed. Rust's own parser
/// accepts the same exponent, sign and leading/trailing-dot forms, so the
/// underscores are the only pre-processing.
fn py_float(text: &str) -> Option<f64> {
    let body = strip_underscores(text)?;
    body.parse::<f64>().ok()
}

/// Remove Python's digit-group underscores, or `None` when they are misplaced.
fn strip_underscores(text: &str) -> Option<String> {
    if !text.contains('_') {
        return Some(text.to_string());
    }
    let chars: Vec<char> = text.chars().collect();
    for (index, character) in chars.iter().enumerate() {
        if *character != '_' {
            continue;
        }
        let before = index.checked_sub(1).and_then(|i| chars.get(i)).copied();
        let after = chars.get(index + 1).copied();
        let ok = matches!(before, Some(c) if c.is_alphanumeric() || c == '_')
            && matches!(after, Some(c) if c.is_alphanumeric() || c == '_');
        if !ok {
            return None;
        }
    }
    let cleaned: String = chars.into_iter().filter(|c| *c != '_').collect();
    if cleaned.contains('_') {
        return None;
    }
    Some(cleaned)
}

/// Whether the token is a finite number, the way Python's `_is_number` asked.
pub fn is_number(text: &str) -> bool {
    py_float(text).is_some_and(f64::is_finite)
}

/// Python's `splitlines()`: a line ends at any of the boundaries `str.splitlines`
/// recognises, and a trailing boundary does not produce an empty final line.
///
/// The producer's output is newline-separated, so this differs from Rust's
/// `lines()` only for the exotic boundaries — but a ported reader of a line
/// stream should not be the place that difference lands.
pub fn py_splitlines(text: &str) -> Vec<String> {
    let is_boundary = |character: char| {
        matches!(
            character,
            '\n' | '\r'
                | '\u{0b}'
                | '\u{0c}'
                | '\u{1c}'
                | '\u{1d}'
                | '\u{1e}'
                | '\u{85}'
                | '\u{2028}'
                | '\u{2029}'
        )
    };
    let mut out = Vec::new();
    let mut current = String::new();
    let mut chars = text.chars().peekable();
    while let Some(character) = chars.next() {
        if !is_boundary(character) {
            current.push(character);
            continue;
        }
        out.push(std::mem::take(&mut current));
        if character == '\r' && chars.peek() == Some(&'\n') {
            chars.next();
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// An ordered JSON object, because the verdict block prints a mandate's
/// measurements in the order its `MANDATE` line gave them. The report's JSON
/// sorts an object's keys on write, so the two faces of the same record are
/// served by the same value: the order is kept for the text, and dropped when
/// the document is written.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Ordered(pub Vec<(String, Json)>);

impl Ordered {
    pub fn get(&self, key: &str) -> Option<&Json> {
        self.0
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value)
    }

    /// Insert a member. A key already present keeps its first value, which is
    /// what Python's `continue` on a repeated token did.
    pub fn insert(&mut self, key: String, value: Json) {
        if !self.contains_key(&key) {
            self.0.push((key, value));
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.iter().any(|(name, _)| name == key)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, (String, Json)> {
        self.0.iter()
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.0.iter().map(|(name, _)| name)
    }

    /// The object as the report writes it: keys sorted.
    pub fn to_json(&self) -> Json {
        let mut map = std::collections::BTreeMap::new();
        for (key, value) in &self.0 {
            map.insert(key.clone(), value.clone());
        }
        Json::Object(map)
    }

    /// The object in its own member order, which the plotter's `--run-values`
    /// is handed.
    pub fn to_compact(&self) -> String {
        crate::tools::json::object_compact(&self.0)
    }
}

/// A finite number when the token is one, otherwise the token itself.
pub fn coerce_value(text: &str) -> Json {
    if let Some(value) = py_int(text) {
        return Json::Int(value);
    }
    match py_float(text) {
        Some(number) if number.is_finite() => Json::Float(number),
        Some(_) => Json::Str(text.to_string()),
        None => Json::Str(text.to_string()),
    }
}

/// A measurement token as `(value, unit)`: `12345B` -> `(12345, "B")`.
///
/// The producer's format strings pad a value (`recv=  800`) and suffix a unit
/// (`wire=   12345B`, `wall=12.3s`); the number is what a comparison reads, so
/// the unit is returned alongside rather than kept in the token.
pub fn coerce_measure(token: &str) -> (Json, Option<char>) {
    let chars: Vec<char> = token.chars().collect();
    if chars.len() > 1 {
        let last = chars[chars.len() - 1];
        if ARM_UNIT_SUFFIXES.contains(&last) {
            let candidate: String = chars[..chars.len() - 1].iter().collect();
            if is_number(&candidate) {
                return (coerce_value(&candidate), Some(last));
            }
        }
    }
    (coerce_value(token), None)
}

/// Python's `round(value, digits)`: correctly rounded half-to-even on the
/// binary value, printed at that many decimals and read back.
///
/// Asked through the shared formatter rather than reimplemented, because the
/// rule the report states is the rule the figure is *printed* in: a duration a
/// reader sees as `0.00` is what the report refuses, so the rounding that
/// produces that figure is the formatter's own.
pub fn py_round(value: f64, digits: usize) -> f64 {
    let spec = format!(".{digits}f");
    match pyformat::format_float(value, &spec) {
        Ok(text) => text.parse::<f64>().unwrap_or(value),
        Err(_) => value,
    }
}

/// Python's `str()` of a parsed value: what the verdict block prints.
pub fn py_str(value: &Json) -> String {
    match value {
        Json::Null => "None".to_string(),
        Json::Bool(flag) => (if *flag { "True" } else { "False" }).to_string(),
        Json::Int(number) => number.to_string(),
        Json::Float(number) => pyjson::repr_float(*number),
        Json::Str(text) => text.clone(),
        other => crate::tools::json::to_string(other),
    }
}

/// `math.isfinite` over a value that may not be a number at all.
pub fn finite(value: &Json) -> bool {
    value.as_f64().is_some_and(f64::is_finite)
}

/// A unit count, or `None`: a non-negative integer and not a bool.
pub fn delivery_count(value: &Json) -> Option<i64> {
    match value {
        Json::Bool(_) | Json::Str(_) | Json::Null | Json::Array(_) | Json::Object(_) => None,
        Json::Int(number) => (*number >= 0).then_some(*number),
        Json::Float(number) => {
            if !number.is_finite() || *number < 0.0 || number.fract() != 0.0 {
                None
            } else {
                Some(*number as i64)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_integer_token_stays_an_integer() {
        assert_eq!(coerce_value("0"), Json::Int(0));
        assert_eq!(coerce_value("800"), Json::Int(800));
        assert_eq!(coerce_value("-12"), Json::Int(-12));
        assert_eq!(coerce_value("+5"), Json::Int(5));
    }

    #[test]
    fn a_float_token_keeps_its_decimal_point() {
        assert_eq!(coerce_value("1.000"), Json::Float(1.0));
        assert_eq!(coerce_value("26.3"), Json::Float(26.3));
        assert_eq!(coerce_value("1e3"), Json::Float(1000.0));
    }

    #[test]
    fn a_non_finite_or_non_numeric_token_is_its_own_text() {
        assert_eq!(coerce_value("inf"), Json::Str("inf".to_string()));
        assert_eq!(coerce_value("nan"), Json::Str("nan".to_string()));
        assert_eq!(coerce_value("Clear"), Json::Str("Clear".to_string()));
        assert_eq!(coerce_value("0.995x"), Json::Str("0.995x".to_string()));
    }

    #[test]
    fn a_unit_suffix_is_stripped_only_when_the_number_precedes_it() {
        assert_eq!(coerce_measure("12345B"), (Json::Int(12345), Some('B')));
        assert_eq!(coerce_measure("12.3s"), (Json::Float(12.3), Some('s')));
        assert_eq!(coerce_measure("800"), (Json::Int(800), None));
        // A lone letter is not a number with a unit, so the token is itself.
        assert_eq!(coerce_measure("B"), (Json::Str("B".to_string()), None));
        assert_eq!(
            coerce_measure("mppsB"),
            (Json::Str("mppsB".to_string()), None)
        );
    }

    /// The rounding is the formatter's own, so `2.675` rounds to `2.67` (its
    /// binary value is below the midpoint) rather than to the decimal
    /// literal's `2.68`.
    #[test]
    fn rounding_is_pythons_own_on_the_binary_value() {
        assert_eq!(py_round(2.675, 2), 2.67);
        assert_eq!(py_round(0.5, 0), 0.0);
        assert_eq!(py_round(1.5, 0), 2.0);
        assert_eq!(py_round(0.002, 2), 0.0);
        assert_eq!(py_round(1.2345, 3), 1.234);
        // The naive multiply-and-round disagrees, so the assertion has teeth.
        assert_eq!(((2.675_f64 * 100.0).round()) / 100.0, 2.68);
    }

    #[test]
    fn a_delivery_count_refuses_a_bool_a_fraction_and_a_negative() {
        assert_eq!(delivery_count(&Json::Int(1200)), Some(1200));
        assert_eq!(delivery_count(&Json::Float(1200.0)), Some(1200));
        assert_eq!(delivery_count(&Json::Float(1200.5)), None);
        assert_eq!(delivery_count(&Json::Int(-1)), None);
        assert_eq!(delivery_count(&Json::Bool(true)), None);
        assert_eq!(delivery_count(&Json::Str("1200".to_string())), None);
        assert_eq!(delivery_count(&Json::Null), None);
    }
}
