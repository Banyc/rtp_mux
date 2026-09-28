//! Python's `format()` number/string spec, reimplemented on CPython's rules.
//!
//! The perf tooling is being ported from `tools/mandate_plot.py`, and the port
//! dies on number formatting: Rust has no `{:g}`, and Rust's `{:.4}` on an
//! `f64` means four *decimal places* where Python's `:.4g` means four
//! *significant digits*. The specs are also used inside the refusal strings
//! the plot tool prints, so they are part of any byte-differential a port must
//! pass.
//!
//! This module is the single authority for that behaviour in the crate. It
//! follows CPython's own two files rather than a restatement of the docs:
//!
//! - `Python/formatter_unicode.c` (`format_float_internal`,
//!   `format_long_internal`, `format_string_internal`): the spec parser and
//!   the field-assembly layout
//!   `[lpad][sign][prefix][spad][grouped_digits][.][frac][remainder][rpad]`.
//! - `Python/pystrtod.c` (`format_float_short`): the digit-string to text
//!   step, including the `g`/`r` decision of fixed versus exponent notation
//!   and the exponent's `%+.02d` shape.
//!
//! The one external engine it does *not* reimplement is the correctly-rounded
//! decimal conversion itself: `_Py_dg_dtoa` is replaced by Rust's own
//! correctly-rounded `f64` formatting (`{:e}`, `{:.{p}e}` and `{:.{n}}`), whose
//! digit strings are then stripped of trailing zeros exactly as Gay's `mode 2`
//! and `mode 3` strip them. That substitution is what the corpus differential
//! (`tools/test_netem_tools.py` and the `py-format` subcommand) exists to
//! verify: it compares this module's output against CPython's, value by value.
//!
//! Cost is deliberately irrelevant: this runs on the reporting path, a few
//! thousand calls per rendered panel, never per packet. Optimising the
//! allocations here at the risk of a rounding change would be a bad trade.

use std::fmt;

/// A spec that CPython would reject, or one outside this module's scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PyFormatError {
    /// `Unknown format code 'x' for object of type 'float'`.
    UnknownPresentationType(char),
    /// `Format specifier missing precision`.
    MissingPrecision,
    /// `Invalid format specifier '...'`.
    InvalidFormatSpecifier(String),
    /// `Cannot specify both ',' and '_'`.
    BothGroupingSeparators,
    /// `Cannot specify ',' with 'x'`.
    GroupingNotAllowed { separator: char, type_char: char },
    /// Sign, `#`, `z`, `=` alignment and similar are per-type restrictions.
    NotAllowedForType {
        what: &'static str,
        type_name: &'static str,
    },
    /// A feature this shim deliberately does not cover (locale-dependent `n`
    /// is approximated, fractional grouping is not implemented).
    Unsupported(&'static str),
}

impl fmt::Display for PyFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPresentationType(c) => {
                write!(f, "unknown presentation type {c:?}")
            }
            Self::MissingPrecision => write!(f, "format specifier missing precision"),
            Self::InvalidFormatSpecifier(spec) => write!(f, "invalid format specifier {spec:?}"),
            Self::BothGroupingSeparators => write!(f, "cannot specify both ',' and '_'"),
            Self::GroupingNotAllowed {
                separator,
                type_char,
            } => write!(f, "cannot specify {separator:?} with {type_char:?}"),
            Self::NotAllowedForType { what, type_name } => {
                write!(f, "{what} not allowed for object of type '{type_name}'")
            }
            Self::Unsupported(what) => write!(f, "unsupported: {what}"),
        }
    }
}

impl std::error::Error for PyFormatError {}

/// The parsed shape of a format spec, before any value is known.
///
/// Field order is CPython's documented grammar:
/// `[[fill]align][sign][z][#][0][width][grouping][.precision][type]`.
#[derive(Debug, Clone)]
struct Spec {
    fill: char,
    fill_specified: bool,
    align: char,
    align_specified: bool,
    sign: Option<char>,
    no_neg_0: bool,
    alternate: bool,
    width: Option<usize>,
    grouping: Option<char>,
    /// A `,`/`_` after the precision (Python 3.14 fractional grouping).
    frac_grouping: Option<char>,
    precision: Option<usize>,
    type_char: char,
}

fn is_align(c: char) -> bool {
    matches!(c, '<' | '>' | '=' | '^')
}

/// Parse `spec` the way `parse_internal_render_format_spec` does.
///
/// `default_type` is `'d'` for ints, `'\0'` for floats and `'s'` for strings;
/// `default_align` is `'>'` for numbers and `'<'` for strings.
fn parse_spec(
    spec: &str,
    default_type: char,
    default_align: char,
    type_name: &'static str,
) -> Result<Spec, PyFormatError> {
    let chars: Vec<char> = spec.chars().collect();
    let mut pos = 0usize;
    let end = chars.len();

    let mut out = Spec {
        fill: ' ',
        fill_specified: false,
        align: default_align,
        align_specified: false,
        sign: None,
        no_neg_0: false,
        alternate: false,
        width: None,
        grouping: None,
        frac_grouping: None,
        precision: None,
        type_char: default_type,
    };

    if end - pos >= 2 && is_align(chars[pos + 1]) {
        out.fill = chars[pos];
        out.align = chars[pos + 1];
        out.fill_specified = true;
        out.align_specified = true;
        pos += 2;
    } else if end - pos >= 1 && is_align(chars[pos]) {
        out.align = chars[pos];
        out.align_specified = true;
        pos += 1;
    }

    if end - pos >= 1 && matches!(chars[pos], ' ' | '+' | '-') {
        out.sign = Some(chars[pos]);
        pos += 1;
    }

    if end - pos >= 1 && chars[pos] == 'z' {
        out.no_neg_0 = true;
        pos += 1;
    }

    if end - pos >= 1 && chars[pos] == '#' {
        out.alternate = true;
        pos += 1;
    }

    // The backwards-compatible `0` fill: only when no fill char was given, and
    // a `0` in front of a width is the fill, not part of the number. `align`
    // only becomes `=` when the type's default alignment is right.
    if !out.fill_specified && end - pos >= 1 && chars[pos] == '0' {
        out.fill = '0';
        if !out.align_specified {
            out.align = '=';
        }
        pos += 1;
    }

    // Width.
    let start = pos;
    let mut value: usize = 0;
    let mut overflow = false;
    while pos < end && chars[pos].is_ascii_digit() {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(chars[pos] as usize - '0' as usize))
            .unwrap_or_else(|| {
                overflow = true;
                0
            });
        pos += 1;
    }
    if overflow {
        return Err(PyFormatError::Unsupported("width too large"));
    }
    out.width = if pos == start { None } else { Some(value) };

    // Grouping.
    if pos < end && chars[pos] == ',' {
        out.grouping = Some(',');
        pos += 1;
    }
    if pos < end && chars[pos] == '_' {
        if out.grouping.is_some() {
            return Err(PyFormatError::BothGroupingSeparators);
        }
        out.grouping = Some('_');
        pos += 1;
    }
    if pos < end && chars[pos] == ',' && out.grouping == Some('_') {
        return Err(PyFormatError::BothGroupingSeparators);
    }

    // Precision, with its own optional grouping.
    if pos < end && chars[pos] == '.' {
        pos += 1;
        let start = pos;
        let mut value: usize = 0;
        let mut overflow = false;
        while pos < end && chars[pos].is_ascii_digit() {
            value = value
                .checked_mul(10)
                .and_then(|v| v.checked_add(chars[pos] as usize - '0' as usize))
                .unwrap_or_else(|| {
                    overflow = true;
                    0
                });
            pos += 1;
        }
        if overflow {
            return Err(PyFormatError::Unsupported("precision too big"));
        }
        let mut consumed = pos - start;
        if pos < end && chars[pos] == ',' {
            out.frac_grouping = Some(',');
            pos += 1;
            consumed += 1;
        }
        if pos < end && chars[pos] == '_' {
            if out.frac_grouping.is_some() {
                return Err(PyFormatError::BothGroupingSeparators);
            }
            out.frac_grouping = Some('_');
            pos += 1;
            consumed += 1;
        }
        if consumed == 0 {
            return Err(PyFormatError::MissingPrecision);
        }
        if pos == start {
            // A bare `.` before the separator: precision stays unspecified.
            out.precision = None;
        } else {
            out.precision = Some(value);
        }
    }

    if end - pos > 1 {
        return Err(PyFormatError::InvalidFormatSpecifier(spec.to_string()));
    }
    if end - pos == 1 {
        out.type_char = chars[pos];
        pos += 1;
    }
    debug_assert_eq!(pos, end);

    // Locale-dependent `n` is approximated as `g`/`d` in the C locale, and
    // fractional grouping is out of scope. Both are explicit rather than
    // silently wrong; neither is used by the plot corpus.
    if out.type_char == 'n'
        && let Some(separator) = out.frac_grouping
    {
        return Err(PyFormatError::GroupingNotAllowed {
            separator,
            type_char: 'n',
        });
    }

    // Validate the grouping separator against the presentation type, exactly
    // as the parser's trailing switch does.
    if let Some(sep) = out.grouping {
        match out.type_char {
            'd' | 'e' | 'f' | 'g' | 'E' | 'G' | '%' | 'F' | '\0' => {}
            'n' => {
                return Err(PyFormatError::GroupingNotAllowed {
                    separator: sep,
                    type_char: 'n',
                });
            }
            'b' | 'o' | 'x' | 'X' if sep == '_' => {}
            other => {
                return Err(PyFormatError::GroupingNotAllowed {
                    separator: sep,
                    type_char: other,
                });
            }
        }
    }
    let _ = type_name;
    Ok(out)
}

/// The three `_Py_dg_dtoa` modes this formatter uses, plus the `decpt`/digit
/// split Gay returns: the value is `0.<digits> * 10^decpt`. Trailing zeros are
/// suppressed, which is what `mode 2` and `mode 3` guarantee and what the
/// field layout below relies on to strip and re-pad.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DtoaMode {
    Shortest,
    Significant(usize),
    Fixed(usize),
}

/// Digits and decimal-point position for a finite, non-negative `f64`.
///
/// The engine is Rust's own correctly-rounded `f64` formatting; the trailing
/// zeros it writes are removed here, so the result has the same shape as
/// `_Py_dg_dtoa`'s.
fn dtoa(abs: f64, mode: DtoaMode) -> (String, i64) {
    match mode {
        DtoaMode::Shortest => parse_scientific(&format!("{abs:e}")),
        DtoaMode::Significant(p) => {
            let p = p.max(1);
            parse_scientific(&format!("{:.*e}", p - 1, abs))
        }
        DtoaMode::Fixed(n) => parse_fixed(&format!("{:.*}", n, abs)),
    }
}

fn strip_trailing_zeros(digits: &mut String) {
    let trimmed = digits.trim_end_matches('0');
    if trimmed.is_empty() {
        digits.clear();
        digits.push('0');
    } else {
        digits.truncate(trimmed.len());
    }
}

/// `"1.234e-4"` -> `("1234", -3)`; `"0e0"` -> `("0", 1)`.
fn parse_scientific(text: &str) -> (String, i64) {
    let (mantissa, exponent) = text.split_once('e').expect("std writes an exponent");
    let exp: i64 = exponent.parse().expect("std writes a decimal exponent");
    let mut digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    strip_trailing_zeros(&mut digits);
    (digits, exp + 1)
}

/// `"00123.4500"` -> `("12345", 3)`; `"0.00"` -> `("0", 1)`.
fn parse_fixed(text: &str) -> (String, i64) {
    let (int_part, frac_part) = match text.split_once('.') {
        Some((i, f)) => (i, f),
        None => (text, ""),
    };
    let mut digits: String = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let mut decpt = int_part.len() as i64;
    let leading = digits.len() - digits.trim_start_matches('0').len();
    if leading > 0 {
        digits.drain(..leading);
        decpt -= leading as i64;
    }
    if digits.is_empty() {
        return ("0".to_string(), 1);
    }
    strip_trailing_zeros(&mut digits);
    (digits, decpt)
}

/// `"%+.02d"` for the exponent, including the sign and the two-digit floor.
fn format_exponent(exp: i64, upper: bool) -> String {
    let sign = if exp < 0 { '-' } else { '+' };
    let magnitude = exp.unsigned_abs();
    let letter = if upper { 'E' } else { 'e' };
    format!("{letter}{sign}{magnitude:02}")
}

/// What `format_float_short` needs to know beyond the value.
struct BodyOptions {
    type_char: char,
    upper: bool,
    alternate: bool,
    add_dot_0: bool,
    no_neg_0: bool,
    /// `precision` after `PyOS_double_to_string`'s per-type adjustment.
    precision: usize,
    mode: DtoaMode,
}

/// The numeric text of a float, sign included, before field assembly.
///
/// This is `PyOS_double_to_string` plus `format_float_short`: the type mapping,
/// the dtoa call, the fixed-versus-exponent decision, the zero padding that
/// reaches `vdigits_end`, and the exponent suffix.
fn float_body(value: f64, opts: &BodyOptions) -> String {
    let negative = value.is_sign_negative();
    if value.is_nan() {
        return if opts.upper { "NAN" } else { "nan" }.to_string();
    }
    if value.is_infinite() {
        let word = if opts.upper { "INF" } else { "inf" };
        return if negative {
            format!("-{word}")
        } else {
            word.to_string()
        };
    }

    let abs = value.abs();
    let (digits, mut decpt) = dtoa(abs, opts.mode);
    let mut sign = negative;

    // `z`: a negative zero (or a value that rounds to one) loses its sign.
    let all_zero = digits.len() == 1 && digits.starts_with('0');
    if opts.no_neg_0 && sign && all_zero {
        sign = false;
    }

    let digits_len = digits.len() as i64;
    let mut use_exp = false;
    let mut vdigits_end = digits_len;
    match opts.type_char {
        'e' => {
            use_exp = true;
            vdigits_end = opts.precision as i64;
        }
        'f' => {
            vdigits_end = decpt + opts.precision as i64;
        }
        'g' => {
            let threshold = if opts.add_dot_0 {
                opts.precision as i64 - 1
            } else {
                opts.precision as i64
            };
            use_exp = decpt <= -4 || decpt > threshold;
            if opts.alternate {
                vdigits_end = opts.precision as i64;
            }
        }
        'r' => {
            use_exp = decpt <= -4 || decpt > 16;
        }
        _ => unreachable!("type mapped before the body is built"),
    }

    let mut exp = 0i64;
    if use_exp {
        exp = decpt - 1;
        decpt = 1;
    }
    let vdigits_start = if decpt <= 0 { decpt - 1 } else { 0 };
    if !use_exp && opts.add_dot_0 {
        vdigits_end = vdigits_end.max(decpt + 1);
    } else {
        vdigits_end = vdigits_end.max(decpt);
    }

    let mut out = String::with_capacity(32);
    if sign {
        out.push('-');
    }

    // The layout is the `vdigits[vdigits_start : vdigits_end]` slice of
    // `digits` padded with infinite zeros on both sides; exactly one of the
    // three branches below writes the decimal point.
    if decpt <= 0 {
        for _ in 0..(decpt - vdigits_start) {
            out.push('0');
        }
        out.push('.');
        for _ in 0..(-decpt) {
            out.push('0');
        }
    } else {
        for _ in 0..(-vdigits_start).max(0) {
            out.push('0');
        }
    }

    if decpt > 0 && decpt <= digits_len {
        out.push_str(&digits[..decpt as usize]);
        out.push('.');
        out.push_str(&digits[decpt as usize..]);
    } else {
        out.push_str(&digits);
    }

    if digits_len < decpt {
        for _ in 0..(decpt - digits_len) {
            out.push('0');
        }
        out.push('.');
        for _ in 0..(vdigits_end - decpt) {
            out.push('0');
        }
    } else {
        for _ in 0..(vdigits_end - digits_len).max(0) {
            out.push('0');
        }
    }

    if out.ends_with('.') && !opts.alternate {
        out.pop();
    }

    if use_exp {
        out.push_str(&format_exponent(exp, opts.upper));
    }
    out
}

/// A body split the way CPython's `parse_number` splits it.
struct Split {
    int_digits: String,
    has_decimal: bool,
    frac_digits: String,
    remainder: String,
}

fn split_body(body: &str) -> Split {
    let bytes = body.as_bytes();
    let mut pos = 0usize;
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    let int_digits = body[..pos].to_string();
    let mut has_decimal = false;
    let mut frac_digits = String::new();
    if pos < bytes.len() && bytes[pos] == b'.' {
        has_decimal = true;
        pos += 1;
        let start = pos;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        frac_digits = body[start..pos].to_string();
    }
    let remainder = body[pos..].to_string();
    Split {
        int_digits,
        has_decimal,
        frac_digits,
        remainder,
    }
}

/// Insert `separator` every `every` digits, counting from the right.
fn group(digits: &str, every: usize, separator: char) -> String {
    if separator == '\0' || digits.len() <= every {
        return digits.to_string();
    }
    let mut out = String::with_capacity(digits.len() + digits.len() / every);
    let first = digits.len() % every;
    let first = if first == 0 { every } else { first };
    out.push_str(&digits[..first]);
    let mut pos = first;
    while pos < digits.len() {
        out.push(separator);
        out.push_str(&digits[pos..pos + every]);
        pos += every;
    }
    out
}

/// The sign character CPython's `calc_number_widths` chooses.
fn sign_char(negative: bool, requested: Option<char>) -> Option<char> {
    match requested {
        Some('+') => Some(if negative { '-' } else { '+' }),
        Some(' ') => Some(if negative { '-' } else { ' ' }),
        _ => {
            if negative {
                Some('-')
            } else {
                None
            }
        }
    }
}

/// The final field assembly shared by every value type.
///
/// Mirrors `calc_number_widths` + `fill_number`:
/// `[lpad][sign][prefix][spad][grouped_digits][.][grouped_frac][remainder][rpad]`.
#[allow(clippy::too_many_arguments)]
fn assemble(
    spec: &Spec,
    sign: Option<char>,
    prefix: &str,
    split: &Split,
    every: usize,
    separator: char,
    frac_separator: char,
) -> String {
    let n_sign = usize::from(sign.is_some());
    let n_prefix = prefix.len();
    let n_digits = split.int_digits.len();
    let n_frac = split.frac_digits.len();
    let n_remainder = split.remainder.len();
    let n_non_digit_non_padding =
        n_sign + n_prefix + usize::from(split.has_decimal) + n_frac + n_remainder;

    // Zero padding inserts its zeros *inside* the digit field, so grouping has
    // to count them; `n_min_width` is that count.
    let min_width_digits = if spec.fill == '0' && spec.align == '=' {
        spec.width
            .map(|w| w.saturating_sub(n_non_digit_non_padding))
            .unwrap_or(0)
    } else {
        0
    };

    let padded_int = if n_digits < min_width_digits {
        let mut s = String::with_capacity(min_width_digits);
        for _ in 0..(min_width_digits - n_digits) {
            s.push('0');
        }
        s.push_str(&split.int_digits);
        s
    } else {
        split.int_digits.clone()
    };
    let grouped_digits = group(&padded_int, every, separator);
    // The fraction is grouped only when the spec says so (Python 3.14's
    // `.,`/`._`), which this shim rejects before reaching here; without it
    // the separator is the empty string, exactly as `frac_thousands_sep` is.
    let grouped_frac = group(&split.frac_digits, 3, frac_separator);

    let content = n_non_digit_non_padding + grouped_digits.len() + grouped_frac.len() - n_frac;
    let width = spec.width.unwrap_or(0);
    let padding = width.saturating_sub(content);
    let (lpad, spad, rpad) = match spec.align {
        '<' => (0, 0, padding),
        '^' => (padding / 2, 0, padding - padding / 2),
        '=' => (0, padding, 0),
        _ => (padding, 0, 0),
    };

    let mut out = String::with_capacity(width.max(content));
    for _ in 0..lpad {
        out.push(spec.fill);
    }
    if let Some(c) = sign {
        out.push(c);
    }
    out.push_str(prefix);
    for _ in 0..spad {
        out.push(spec.fill);
    }
    out.push_str(&grouped_digits);
    if split.has_decimal {
        out.push('.');
    }
    out.push_str(&grouped_frac);
    out.push_str(&split.remainder);
    for _ in 0..rpad {
        out.push(spec.fill);
    }
    out
}

/// `format(value, spec)` for a Python `float`.
pub fn format_float(value: f64, spec: &str) -> Result<String, PyFormatError> {
    if spec.is_empty() {
        return Ok(float_body(
            value,
            &BodyOptions {
                type_char: 'r',
                upper: false,
                alternate: false,
                add_dot_0: true,
                no_neg_0: false,
                precision: 0,
                mode: DtoaMode::Shortest,
            },
        ));
    }
    let parsed = parse_spec(spec, '\0', '>', "float")?;
    format_float_with_spec(parsed, value)
}

/// The float path with the spec already parsed.
///
/// `_PyLong_FormatAdvancedWriter` routes an int carrying a float presentation
/// type (`e E f F g G %`) to exactly this path after `PyNumber_Float`, so the
/// int and the float spellings cannot disagree.
fn format_float_with_spec(parsed: Spec, value: f64) -> Result<String, PyFormatError> {
    if parsed.frac_grouping.is_some() {
        return Err(PyFormatError::Unsupported("fractional grouping"));
    }

    // `format_float_internal`'s type mapping.
    let mut type_char = parsed.type_char;
    let mut value = value;
    let mut add_pct = false;
    let mut add_dot_0 = false;
    let mut default_precision: usize = 6;
    if type_char == '\0' {
        add_dot_0 = true;
        type_char = 'r';
        default_precision = 0;
    }
    if type_char == 'n' {
        type_char = 'g';
    }
    if type_char == '%' {
        type_char = 'f';
        value *= 100.0;
        add_pct = true;
    }

    let specified = parsed.precision;
    let effective_type = if specified.is_none() {
        type_char
    } else if type_char == 'r' {
        'g'
    } else {
        type_char
    };
    let precision = match specified {
        None => default_precision,
        Some(p) => p,
    };

    let upper = matches!(effective_type, 'E' | 'F' | 'G');
    let lower_type = effective_type.to_ascii_lowercase();
    let (mode, body_precision) = match lower_type {
        // `PyOS_double_to_string` increments the precision for 'e': `%e`'s
        // precision counts digits after the point, dtoa's counts significant
        // digits, and the field layout pads to the significant count.
        'e' => (DtoaMode::Significant(precision + 1), precision + 1),
        'f' => (DtoaMode::Fixed(precision), precision),
        'g' => {
            let p = if precision == 0 { 1 } else { precision };
            (DtoaMode::Significant(p), p)
        }
        'r' => (DtoaMode::Shortest, 0),
        _ => return Err(PyFormatError::UnknownPresentationType(effective_type)),
    };

    let opts = BodyOptions {
        type_char: lower_type,
        upper,
        alternate: parsed.alternate,
        add_dot_0,
        no_neg_0: parsed.no_neg_0,
        precision: body_precision,
        mode,
    };
    let mut body = float_body(value, &opts);
    if add_pct {
        body.push('%');
    }

    let negative = body.starts_with('-');
    let body = if negative { &body[1..] } else { body.as_str() };
    let split = split_body(body);
    let sign = sign_char(negative, parsed.sign);
    let separator = parsed.grouping.unwrap_or('\0');
    Ok(assemble(&parsed, sign, "", &split, 3, separator, '\0'))
}

/// `format(value, spec)` for a Python `int`.
pub fn format_int(value: i64, spec: &str) -> Result<String, PyFormatError> {
    if spec.is_empty() {
        return Ok(value.to_string());
    }
    let parsed = parse_spec(spec, 'd', '>', "int")?;
    if matches!(parsed.type_char, 'e' | 'E' | 'f' | 'F' | 'g' | 'G' | '%') {
        // `_PyLong_FormatAdvancedWriter` converts the int to a float for these
        // and calls the float path, so precision and `z` are legal here.
        return format_float_with_spec(parsed, value as f64);
    }
    if parsed.precision.is_some() {
        return Err(PyFormatError::NotAllowedForType {
            what: "precision",
            type_name: "int",
        });
    }
    if parsed.no_neg_0 {
        return Err(PyFormatError::NotAllowedForType {
            what: "negative zero coercion (z)",
            type_name: "int",
        });
    }
    if parsed.frac_grouping.is_some() {
        return Err(PyFormatError::Unsupported("fractional grouping"));
    }

    if parsed.type_char == 'c' {
        if parsed.sign.is_some() {
            return Err(PyFormatError::NotAllowedForType {
                what: "sign",
                type_name: "int",
            });
        }
        if parsed.alternate {
            return Err(PyFormatError::NotAllowedForType {
                what: "alternate form (#)",
                type_name: "int",
            });
        }
        let code = u32::try_from(value).map_err(|_| PyFormatError::NotAllowedForType {
            what: "%c arg not in range(0x110000)",
            type_name: "int",
        })?;
        let ch = char::from_u32(code).ok_or(PyFormatError::NotAllowedForType {
            what: "%c arg not in range(0x110000)",
            type_name: "int",
        })?;
        let split = Split {
            int_digits: String::new(),
            has_decimal: false,
            frac_digits: String::new(),
            remainder: ch.to_string(),
        };
        return Ok(assemble(&parsed, None, "", &split, 3, '\0', '\0'));
    }

    let negative = value < 0;
    // `i64::MIN` has no positive counterpart in i64.
    let magnitude = value.unsigned_abs();
    let (radix, every, separator, prefix) = match parsed.type_char {
        'b' => (
            2u32,
            4,
            if parsed.grouping == Some('_') {
                '_'
            } else {
                '\0'
            },
            if parsed.alternate { "0b" } else { "" },
        ),
        'o' => (
            8,
            4,
            if parsed.grouping == Some('_') {
                '_'
            } else {
                '\0'
            },
            if parsed.alternate { "0o" } else { "" },
        ),
        'x' | 'X' => (
            16,
            4,
            if parsed.grouping == Some('_') {
                '_'
            } else {
                '\0'
            },
            if parsed.alternate { "0x" } else { "" },
        ),
        'd' | 'n' => (10, 3, parsed.grouping.unwrap_or('\0'), ""),
        other => return Err(PyFormatError::UnknownPresentationType(other)),
    };

    let mut digits = to_radix(magnitude, radix);
    if parsed.type_char == 'X' {
        digits = digits.to_ascii_uppercase();
    }
    let prefix = if parsed.type_char == 'X' {
        prefix.to_ascii_uppercase()
    } else {
        prefix.to_string()
    };
    let sign = sign_char(negative, parsed.sign);
    let split = Split {
        int_digits: digits,
        has_decimal: false,
        frac_digits: String::new(),
        remainder: String::new(),
    };
    Ok(assemble(
        &parsed, sign, &prefix, &split, every, separator, '\0',
    ))
}

fn to_radix(mut value: u64, radix: u32) -> String {
    if value == 0 {
        return "0".to_string();
    }
    let alphabet = b"0123456789abcdef";
    let mut out = Vec::new();
    while value > 0 {
        out.push(alphabet[(value % radix as u64) as usize] as char);
        value /= radix as u64;
    }
    out.reverse();
    out.into_iter().collect()
}

/// `format(value, spec)` for a Python `str`.
pub fn format_str(value: &str, spec: &str) -> Result<String, PyFormatError> {
    if spec.is_empty() {
        return Ok(value.to_string());
    }
    let parsed = parse_spec(spec, 's', '<', "str")?;
    if parsed.sign.is_some() {
        return Err(PyFormatError::NotAllowedForType {
            what: "sign",
            type_name: "str",
        });
    }
    if parsed.no_neg_0 {
        return Err(PyFormatError::NotAllowedForType {
            what: "negative zero coercion (z)",
            type_name: "str",
        });
    }
    if parsed.alternate {
        return Err(PyFormatError::NotAllowedForType {
            what: "alternate form (#)",
            type_name: "str",
        });
    }
    if parsed.align == '=' {
        return Err(PyFormatError::NotAllowedForType {
            what: "'=' alignment",
            type_name: "str",
        });
    }
    if parsed.type_char != 's' {
        return Err(PyFormatError::UnknownPresentationType(parsed.type_char));
    }
    if let Some(separator) = parsed.grouping {
        return Err(PyFormatError::GroupingNotAllowed {
            separator,
            type_char: 's',
        });
    }

    let chars: Vec<char> = value.chars().collect();
    let len = match parsed.precision {
        Some(p) if p < chars.len() => p,
        _ => chars.len(),
    };
    let truncated: String = chars[..len].iter().collect();
    let width = parsed.width.unwrap_or(0);
    let padding = width.saturating_sub(len);
    let (lpad, rpad) = match parsed.align {
        '>' => (padding, 0),
        '^' => (padding / 2, padding - padding / 2),
        _ => (0, padding),
    };
    let mut out = String::with_capacity(width.max(len));
    for _ in 0..lpad {
        out.push(parsed.fill);
    }
    out.push_str(&truncated);
    for _ in 0..rpad {
        out.push(parsed.fill);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pairs measured from CPython 3.14.7 (`python3 -c 'print(format(v,s))'`).
    /// The corpus differential in `tools/test_netem_tools.py` is the broad
    /// check; these are the anchors a reader can verify in one command.
    const VECTORS: &[(f64, &str, &str)] = &[
        // Tie-to-even and exact-binary rounding, not naive round().
        (2.675, ".2f", "2.67"),
        (1.005, ".2f", "1.00"),
        (0.5, ".0f", "0"),
        (1.5, ".0f", "2"),
        (2.5, ".0f", "2"),
        (1e23, ".0f", "99999999999999991611392"),
        (1e23, "g", "1e+23"),
        // g's exponent threshold, measured across the boundary.
        (0.0001, "g", "0.0001"),
        (0.00001, "g", "1e-05"),
        (999999.0, ".6g", "999999"),
        (1000000.0, ".6g", "1e+06"),
        (999999.9, ".6g", "1e+06"),
        (0.9999999, "g", "1"),
        (12345.6789, "g", "12345.7"),
        (12345.6789, ".4g", "1.235e+04"),
        (12345.6789, ".4g", "1.235e+04"),
        // `#` keeps the zeros and the point.
        (1.5, "#.4g", "1.500"),
        (1.0, "#.0f", "1."),
        (1.0, "#.0e", "1.e+00"),
        (250.0, "#.4g", "250.0"),
        // Percent multiplies by 100 and uses f's decimal places.
        (0.255, ".1%", "25.5%"),
        (1.0, "%", "100.000000%"),
        // Signs, width, fill, alignment, grouping.
        (-1.5, "08.1f", "-00001.5"),
        (1.5, "^8", "  1.5   "),
        (1.5, "*^9", "***1.5***"),
        (12345.6789, ",.2f", "12,345.68"),
        (123456789.0, ",.10g", "123,456,789"),
        (1.5, "+", "+1.5"),
        (-0.0, "z", "0.0"),
        (-0.0001, "z.0f", "0"),
        // Empty type is repr, with the exponent threshold at 1e16.
        (1.0, "", "1.0"),
        (1e15, "", "1000000000000000.0"),
        (1e16, "", "1e+16"),
        (0.0001, "", "0.0001"),
        (0.00001, "", "1e-05"),
        // Empty type with a precision is g plus "one digit past the point".
        (1.5, ".1", "2e+00"),
        (1.5, ".2", "1.5"),
        (100.0, ".3", "1e+02"),
        (100.0, ".4", "100.0"),
        // Specials.
        (f64::NAN, ".2f", "nan"),
        (f64::NAN, "E", "NAN"),
        (f64::INFINITY, "<8", "inf     "),
        (f64::NEG_INFINITY, "+", "-inf"),
    ];

    #[test]
    fn matches_the_cpython_anchors() {
        for (value, spec, expected) in VECTORS {
            let got = format_float(*value, spec)
                .unwrap_or_else(|e| panic!("format({value:?}, {spec:?}) failed: {e}"));
            assert_eq!(&got, expected, "format({value:?}, {spec:?})");
        }
    }

    /// The differential's own guard: a check that cannot fail proves nothing,
    /// so the vector set must actually disagree with the naive Rust spelling
    /// for the specs where Python differs from Rust.
    #[test]
    fn the_anchors_are_not_satisfied_by_rusts_own_formatting() {
        // `{:.4}` means decimal places in Rust, significant digits in Python.
        assert_eq!(format!("{:.4}", 12345.6789_f64), "12345.6789");
        assert_eq!(format_float(12345.6789, ".4g").unwrap(), "1.235e+04");
        // Rust has no `g` at all: `{}` is the shortest repr, not 6 digits.
        assert_eq!(format!("{}", 12345.6789_f64), "12345.6789");
        assert_eq!(format_float(12345.6789, "g").unwrap(), "12345.7");
        // Rust's `{}` never uses an exponent for 1e16; Python's repr does.
        assert_eq!(format!("{}", 1e16_f64), "10000000000000000");
        assert_eq!(format_float(1e16, "").unwrap(), "1e+16");
    }

    #[test]
    fn ints_and_strings_cover_the_default_and_the_field_layout() {
        assert_eq!(format_int(126000, "").unwrap(), "126000");
        assert_eq!(format_int(1234567, ",").unwrap(), "1,234,567");
        assert_eq!(format_int(255, "#x").unwrap(), "0xff");
        assert_eq!(format_int(-5, "05").unwrap(), "-0005");
        assert_eq!(format_str("ab", ">5").unwrap(), "   ab");
        assert_eq!(format_str("abcdef", ".3").unwrap(), "abc");
        // `_PyLong_FormatAdvancedWriter` routes a float presentation type on
        // an int through `PyNumber_Float`, so these go down the float path.
        // The plot formats an int with `.1f` 1308 times, so it is not a
        // curiosity.
        assert_eq!(format_int(5, ".1f").unwrap(), "5.0");
        assert_eq!(format_int(-5, ".1f").unwrap(), "-5.0");
        assert_eq!(format_int(3, "g").unwrap(), "3");
        assert_eq!(format_int(1000000, "e").unwrap(), "1.000000e+06");
        assert_eq!(format_int(1234567, "%").unwrap(), "123456700.000000%");
        assert_eq!(format_int(2, "#.0f").unwrap(), "2.");
        // An integer type still refuses a precision and `z`.
        assert!(matches!(
            format_int(5, ".2d"),
            Err(PyFormatError::NotAllowedForType { .. })
        ));
        assert!(matches!(
            format_int(5, "zd"),
            Err(PyFormatError::NotAllowedForType { .. })
        ));
    }

    /// The `g` threshold, derived by probing CPython 3.14.7 across the boundary
    /// at several precisions (fixed table in `tools/pyformat_diff.py`'s
    /// `EDGE_VALUES`, which re-measures it against this module on every suite
    /// run). The rule, in CPython's own terms (`Python/pystrtod.c`,
    /// `format_float_short`): round the value to `p` significant digits, let
    /// `decpt` be that digit string's decimal-point position (the value is
    /// `0.<digits> * 10^decpt`, so the scientific exponent is `decpt - 1`), and
    /// use exponent notation iff `decpt <= -4 || decpt > p`. Two consequences
    /// are the ones a one-off guess gets wrong: the comparison is against `p`
    /// (not `p - 1`), so `decpt == p` is still fixed; and it is evaluated
    /// *after* rounding, so a carry across a decade can move a value over the
    /// boundary (`999_999.9` at `.6g` rounds to `1e+06`, while `999_999.0`
    /// stays fixed).
    #[test]
    fn g_exponent_threshold_is_the_measured_rule() {
        // decpt == p stays fixed; decpt == p + 1 goes to exponent notation.
        assert_eq!(format_float(100.0, ".2g").unwrap(), "1e+02");
        assert_eq!(format_float(10.0, ".2g").unwrap(), "10");
        assert_eq!(format_float(1000.0, ".3g").unwrap(), "1e+03");
        assert_eq!(format_float(100.0, ".3g").unwrap(), "100");
        assert_eq!(format_float(10000.0, ".4g").unwrap(), "1e+04");
        assert_eq!(format_float(1000.0, ".4g").unwrap(), "1000");
        assert_eq!(format_float(1000000.0, ".6g").unwrap(), "1e+06");
        assert_eq!(format_float(100000.0, ".6g").unwrap(), "100000");
        // The lower edge: decpt == -3 (1e-4) fixed, decpt == -4 (1e-5) exponent.
        assert_eq!(format_float(0.0001, ".6g").unwrap(), "0.0001");
        assert_eq!(format_float(0.00001, ".6g").unwrap(), "1e-05");
        // The threshold is decided after rounding, so a carry moves it.
        assert_eq!(format_float(999999.0, ".6g").unwrap(), "999999");
        assert_eq!(format_float(999999.9, ".6g").unwrap(), "1e+06");
        assert_eq!(format_float(9999.99, ".4g").unwrap(), "1e+04");
        assert_eq!(format_float(999.99, ".4g").unwrap(), "1000");
        assert_eq!(format_float(9.999999999e-5, ".2g").unwrap(), "0.0001");
        // The empty type with a precision is `g` with `decpt > p - 1`, because
        // fixed notation must keep a digit past the point (issue 5864).
        assert_eq!(format_float(100.0, ".3").unwrap(), "1e+02");
        assert_eq!(format_float(100.0, ".4").unwrap(), "100.0");
        // `#` keeps the point and the zeros but does not move the threshold.
        assert_eq!(format_float(100.0, "#.3g").unwrap(), "100.");
        assert_eq!(format_float(100.0, "#.4g").unwrap(), "100.0");
    }

    /// CPython rounds the exact binary value to the requested decimal place,
    /// ties to even. That is *not* `round()` on the decimal literal: `2.675`
    /// is `2.67499999...` in binary, and `1e23` expands to its exact integer.
    #[test]
    fn rounding_is_correctly_rounded_half_to_even_on_the_binary_value() {
        assert_eq!(format_float(2.675, ".2f").unwrap(), "2.67");
        assert_eq!(format_float(0.5, ".0f").unwrap(), "0");
        assert_eq!(format_float(1.5, ".0f").unwrap(), "2");
        assert_eq!(format_float(2.5, ".0f").unwrap(), "2");
        assert_eq!(format_float(0.125, ".2f").unwrap(), "0.12");
        assert_eq!(format_float(1.005, ".2f").unwrap(), "1.00");
        assert_eq!(
            format_float(1e23, ".0f").unwrap(),
            "99999999999999991611392"
        );
        assert_eq!(format_float(1e23, "g").unwrap(), "1e+23");
        assert_eq!(format_float(5e-324, ".0e").unwrap(), "5e-324");
        // The naive alternative disagrees, so the assertions above have teeth:
        // multiply-and-round gives the decimal-literal answer 2.68.
        let naive = (2.675_f64 * 100.0).round() / 100.0;
        assert_eq!(format!("{naive:.2}"), "2.68");
        assert_eq!(format_float(2.675, ".2f").unwrap(), "2.67");
    }

    #[test]
    fn rejects_what_cpython_rejects() {
        assert_eq!(
            format_float(1.0, "d"),
            Err(PyFormatError::UnknownPresentationType('d'))
        );
        assert_eq!(format_float(1.0, "."), Err(PyFormatError::MissingPrecision));
        assert_eq!(
            format_float(1.0, ",_"),
            Err(PyFormatError::BothGroupingSeparators)
        );
        assert_eq!(format_float(1.0, ",e"), Ok("1.000000e+00".to_string()));
        // `,` on `n` is rejected by the parser (the switch omits 'n').
        assert!(matches!(
            format_float(1.0, ",n"),
            Err(PyFormatError::GroupingNotAllowed { .. })
        ));
    }
}

/// The file-name-free face of the formatter: what the `py-format` subcommand
/// and the corpus differential drive.
///
/// The wire shape is one request per line, `kind \t value \t spec`, where
/// `kind` is `f`, `i` or `s`; the answer is one line per request, `ok \t text`
/// or `err \t message`. Values arrive as Python `repr()` text, which Rust's
/// correctly-rounded parser reads back to the same bits, so a differential can
/// stream hundreds of thousands of pairs through one process instead of paying
/// a spawn per pair.
#[derive(Debug, Clone, Default)]
pub struct Args {
    /// Format a single value instead of reading stdin.
    pub value: Option<String>,
    /// The single value's kind (`f`, `i`, `s`).
    pub kind: String,
    /// The single value's spec.
    pub spec: Option<String>,
}

/// Format one request, returning `ok \t text` or `err \t message`.
fn format_one(kind: &str, value: &str, spec: &str) -> String {
    let outcome = match kind {
        "f" => match value.parse::<f64>() {
            Ok(v) => format_float(v, spec),
            Err(e) => return format!("err\tcannot parse float {value:?}: {e}"),
        },
        "i" => match value.parse::<i64>() {
            Ok(v) => format_int(v, spec),
            Err(e) => return format!("err\tcannot parse int {value:?}: {e}"),
        },
        "s" => format_str(value, spec),
        other => return format!("err\tunknown kind {other:?}"),
    };
    match outcome {
        Ok(text) => format!("ok\t{text}"),
        Err(e) => format!("err\t{e}"),
    }
}

/// Run the `py-format` face and return a process exit status.
pub fn main(args: Args) -> i32 {
    use std::io::{BufRead, Write};

    if let Some(value) = args.value {
        let kind = if args.kind.is_empty() {
            "f".to_string()
        } else {
            args.kind.clone()
        };
        let spec = args.spec.clone().unwrap_or_default();
        let line = format_one(&kind, &value, &spec);
        match line.strip_prefix("ok\t") {
            Some(text) => {
                println!("{text}");
                0
            }
            None => {
                eprintln!(
                    "netem-tools py-format: {}",
                    line.strip_prefix("err\t").unwrap_or(&line)
                );
                2
            }
        }
    } else {
        let stdin = std::io::stdin();
        let stdout = std::io::stdout();
        let mut out = std::io::BufWriter::new(stdout.lock());
        for line in stdin.lock().lines() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("netem-tools py-format: reading stdin: {e}");
                    return 2;
                }
            };
            let mut fields = line.splitn(3, '\t');
            let kind = fields.next().unwrap_or("");
            let value = fields.next().unwrap_or("");
            let spec = fields.next().unwrap_or("");
            if writeln!(out, "{}", format_one(kind, value, spec)).is_err() {
                return 2;
            }
        }
        if out.flush().is_err() {
            return 2;
        }
        0
    }
}
