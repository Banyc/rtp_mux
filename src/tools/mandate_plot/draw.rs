//! The chart primitives the mandate panels are drawn with.
//!
//! Ported from `tools/rtp_trace_report.py` (its bound-label metrics and
//! placement, its line/CDF chart, and the sampling-hole analysis a line panel's
//! honesty rests on) and from `mandate_plot.py`'s own `svg_bar_chart`, which was
//! written against the report's constants so the two families of panel cannot
//! be held to different standards.
//!
//! Only what the mandate plotter calls is ported. `rtp_trace_report`'s
//! trace-report rendering stays Python and stays authoritative for the report;
//! the primitives here are a second copy of the chart geometry, and that
//! duplication is recorded in `crates/AUDIT_COVERAGE.md` rather than left
//! implicit.

use super::checks::*;
use super::*;

/// The series palette. A bound is drawn in a neutral dark that no series uses.
pub const COLORS: [&str; 8] = [
    "#2563eb", "#dc2626", "#059669", "#7c3aed", "#d97706", "#0891b2", "#be185d", "#4d7c0f",
];

/// The stroke of a bound line.
pub const BOUND_STROKE: &str = "#111827";

/// The text style of a bound label.
pub const BOUND_LABEL_STYLE: &str = "fill:#111827";

/// A note's style, haloed white: a note usually lands across the data it explains.
pub const NOTE_LABEL_STYLE: &str = "fill:#111827;stroke:#ffffff;stroke-width:3;paint-order:stroke";

/// A bar panel's bound label, haloed white.
pub const BAR_BOUND_LABEL_STYLE: &str = NOTE_LABEL_STYLE;

/// The reading band's text style, shared with the bound labels.
pub const ARM_READING_STYLE: &str = BOUND_LABEL_STYLE;

pub const WIDTH: i64 = 960;
pub const HEIGHT: i64 = 300;
pub const PAD_LEFT: i64 = 72;
pub const PAD_RIGHT: i64 = 24;
pub const PAD_TOP: i64 = 24;
pub const PAD_BOTTOM: i64 = 48;

/// The width of a line/CDF panel's plot area.
pub const READING_PLOT_WIDTH: i64 = WIDTH - PAD_LEFT - PAD_RIGHT;

pub const LABEL_FONT_PX: f64 = 11.0;
pub const LABEL_ASCENT_PX: f64 = 11.0;
pub const LABEL_DESCENT_PX: f64 = 3.0;
pub const LABEL_LINE_HEIGHT_PX: f64 = 14.0;
pub const LABEL_GAP_PX: f64 = 5.0;
pub const LABEL_INSET_PX: f64 = 4.0;
pub const LABEL_MARGIN_PX: f64 = 2.0;
pub const LABEL_MAX_LINES: usize = 4;
pub const LABEL_ADVANCE_SAFETY: f64 = 1.10;
pub const LABEL_ADVANCE_OTHER: f64 = 12.0;

/// The widest advance any of the fonts a browser resolves gives a character of
/// each class, as an upper bound over the panel's own 11px text style.
pub const LABEL_ADVANCE_CLASSES: [(&str, f64); 12] = [
    ("%", 11.85),
    ("MW", 10.89),
    ("mw", 10.71),
    ("=\u{b1}", 9.01),
    ("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 8.97),
    ("0123456789", 7.15),
    ("_", 7.08),
    ("abcdefghijklmnopqrstuvwxyz", 6.97),
    ("[](){}|", 5.5),
    ("-/\\+';:\"!?", 5.5),
    (",.", 4.01),
    (" ", 3.9),
];

/// The least elapsed time a step has to take to be a hole in the sampling.
pub const GAP_WALL_MIN_SECONDS: f64 = 0.5;

/// How far above the series' own median step a step has to be to be a hole.
pub const GAP_WALL_STEP_MULTIPLE: f64 = 10.0;

/// The radius of the dot drawn at every drawn sample of a line panel.
pub const SAMPLE_MARKER_RADIUS_PX: f64 = 2.0;

/// The line height of the per-arm reading band a line panel reserves.
pub const ARM_READING_LINE_HEIGHT_PX: f64 = 13.0;

/// How far below the frame's top edge a clipped sample is drawn.
pub const Y_CLIP_INSET_PX: f64 = 2.0;

/// The drop from the legend to the first baseline of the reading band.
pub const ARM_READING_TOP_PX: f64 = 10.0;

/// An upper bound on the advance of `character` at `LABEL_FONT_PX`.
pub fn label_char_advance(character: char) -> f64 {
    for (characters, advance) in LABEL_ADVANCE_CLASSES {
        if characters.contains(character) {
            return advance * LABEL_ADVANCE_SAFETY;
        }
    }
    LABEL_ADVANCE_OTHER * LABEL_ADVANCE_SAFETY
}

/// An upper bound on the width of `text` in the bound-label text style.
pub fn label_text_width(text: &str) -> f64 {
    text.chars().map(label_char_advance).sum()
}

/// The longest prefix of `word` whose width is at most `budget`.
fn label_prefix_that_fits(word: &str, budget: f64) -> usize {
    let mut width = 0.0;
    for (index, character) in word.chars().enumerate() {
        width += label_char_advance(character);
        if width > budget {
            return index.max(1);
        }
    }
    word.chars().count()
}

/// Greedy word wrap of a bound label; a word wider than `budget` is broken.
pub fn wrap_label(text: &str, budget: f64) -> Vec<String> {
    wrap_label_lines(text, budget, LABEL_MAX_LINES)
}

/// [`wrap_label`] with the line cap stated, which the report's own caller keeps
/// as a parameter.
pub fn wrap_label_lines(text: &str, budget: f64, max_lines: usize) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    let mut current = String::new();
    for word in text.split(' ') {
        if word.is_empty() {
            continue;
        }
        let candidate = if current.is_empty() {
            word.to_string()
        } else {
            format!("{current} {word}")
        };
        if label_text_width(&candidate) <= budget {
            current = candidate;
            continue;
        }
        if !current.is_empty() {
            lines.push(std::mem::take(&mut current));
        }
        let mut word = word.to_string();
        while label_text_width(&word) > budget {
            let cut = label_prefix_that_fits(&word, budget);
            let head: String = word.chars().take(cut).collect();
            lines.push(head);
            word = word.chars().skip(cut).collect();
        }
        current = word;
    }
    if !current.is_empty() {
        lines.push(current);
    }
    if lines.is_empty() {
        lines.push(text.to_string());
    }
    if lines.len() > max_lines {
        let mut kept: Vec<String> = lines[..max_lines - 1].to_vec();
        kept.push(lines[max_lines - 1..].join(" "));
        lines = kept;
    }
    lines
}

/// A laid-out bound label: its lines, the anchor each is drawn at, and its boxes.
pub struct LabelLayout {
    pub lines: Vec<String>,
    pub anchors: Vec<(f64, f64)>,
    pub boxes: Vec<(f64, f64, f64, f64)>,
}

/// Place a bound's label inside `plot`, returning its lines and baselines.
pub fn layout_bound_label(
    text: &str,
    line_right: f64,
    bound_y: f64,
    plot: (f64, f64, f64, f64),
) -> LabelLayout {
    let (left, top, right, _bottom) = plot;
    let budget = ((right - LABEL_INSET_PX) - (left + LABEL_INSET_PX)).max(LABEL_FONT_PX);
    let lines = wrap_label(text, budget);
    let widest = lines
        .iter()
        .map(|line| label_text_width(line))
        .fold(0.0, f64::max);
    let anchor = (line_right - LABEL_INSET_PX)
        .max(left + LABEL_INSET_PX + widest)
        .min(right - LABEL_INSET_PX);
    let block = (lines.len() as f64 - 1.0) * LABEL_LINE_HEIGHT_PX;
    let mut baselines: Vec<f64> = (0..lines.len())
        .map(|index| bound_y - LABEL_GAP_PX - block + index as f64 * LABEL_LINE_HEIGHT_PX)
        .collect();
    if baselines[0] - LABEL_ASCENT_PX < top + LABEL_MARGIN_PX {
        baselines = (0..lines.len())
            .map(|index| {
                bound_y + LABEL_GAP_PX + LABEL_ASCENT_PX + index as f64 * LABEL_LINE_HEIGHT_PX
            })
            .collect();
    }
    let boxes = lines
        .iter()
        .zip(baselines.iter())
        .map(|(line, baseline)| {
            (
                anchor - label_text_width(line),
                baseline - LABEL_ASCENT_PX,
                anchor,
                baseline + LABEL_DESCENT_PX,
            )
        })
        .collect();
    LabelLayout {
        lines,
        anchors: baselines
            .iter()
            .map(|baseline| (anchor, *baseline))
            .collect(),
        boxes,
    }
}

/// The `<text>` markup for one bound's label, laid out inside `plot`.
pub fn bound_label_markup(
    text: &str,
    line_right: f64,
    bound_y: f64,
    plot: (f64, f64, f64, f64),
    style: &str,
) -> (String, LabelLayout) {
    let layout = layout_bound_label(text, line_right, bound_y, plot);
    let mut parts = String::new();
    for (index, (line, (x, y))) in layout.lines.iter().zip(layout.anchors.iter()).enumerate() {
        let mut inner = pyjson::escape(line);
        if index == 0 {
            inner = format!("<title>{}</title>{inner}", pyjson::escape(text));
        }
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<text class=\"bound-label\" x=\"{}\" y=\"{}\" text-anchor=\"end\" \
                 style=\"{style}\">{inner}</text>",
                f1(*x),
                f1(*y)
            ),
        );
    }
    (parts, layout)
}

/// The `<text>` markup for a **vertical** bound's label, laid out beside it.
pub fn x_bound_label_markup(
    text: &str,
    line_x: f64,
    plot: (f64, f64, f64, f64),
    style: &str,
) -> String {
    let (left, top, right, _bottom) = plot;
    let right_budget = ((right - LABEL_INSET_PX) - (line_x + LABEL_GAP_PX)).max(0.0);
    let left_budget = ((line_x - LABEL_GAP_PX) - (left + LABEL_INSET_PX)).max(0.0);
    let to_the_right = right_budget >= left_budget;
    let budget = (if to_the_right {
        right_budget
    } else {
        left_budget
    })
    .max(LABEL_FONT_PX);
    let lines = wrap_label(text, budget);
    let anchor = if to_the_right {
        line_x + LABEL_GAP_PX
    } else {
        line_x - LABEL_GAP_PX
    };
    let text_anchor = if to_the_right { "start" } else { "end" };
    let mut parts = String::new();
    for (index, line) in lines.iter().enumerate() {
        let mut inner = pyjson::escape(line);
        if index == 0 {
            inner = format!("<title>{}</title>{inner}", pyjson::escape(text));
        }
        let baseline = top + LABEL_ASCENT_PX + 2.0 + index as f64 * LABEL_LINE_HEIGHT_PX;
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<text class=\"bound-label\" x=\"{}\" y=\"{}\" text-anchor=\"{text_anchor}\" \
                 style=\"{style}\">{inner}</text>",
                f1(anchor),
                f1(baseline)
            ),
        );
    }
    parts
}

/// The least step a series would have to take to be a hole, or `None`.
pub fn gap_wall_seconds(points: &[(f64, f64)]) -> Option<f64> {
    if points.len() < 3 {
        return None;
    }
    let mut steps: Vec<f64> = points
        .windows(2)
        .map(|pair| pair[1].0 - pair[0].0)
        .collect();
    steps.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = steps[steps.len() / 2];
    Some(GAP_WALL_MIN_SECONDS.max(GAP_WALL_STEP_MULTIPLE * median))
}

/// The holes in a drawn series, as `(index, before_x, after_x, gap)`.
pub fn series_walls(points: &[(f64, f64)]) -> Vec<(usize, f64, f64, f64)> {
    let Some(wall) = gap_wall_seconds(points) else {
        return Vec::new();
    };
    points
        .windows(2)
        .enumerate()
        .filter(|(_, pair)| pair[1].0 - pair[0].0 >= wall)
        .map(|(index, pair)| (index, pair[0].0, pair[1].0, pair[1].0 - pair[0].0))
        .collect()
}

/// The contiguous runs of `points` between its holes, in order.
pub fn split_at_walls(
    points: &[(f64, f64)],
    walls: &[(usize, f64, f64, f64)],
) -> Vec<Vec<(f64, f64)>> {
    let mut runs = Vec::new();
    let mut start = 0usize;
    for (index, _, _, _) in walls {
        runs.push(points[start..=*index].to_vec());
        start = index + 1;
    }
    runs.push(points[start.min(points.len())..].to_vec());
    runs.retain(|run| !run.is_empty());
    runs
}

/// The wrapped lines of each `(name, sentence)` reading, in draw order.
pub fn reading_lines(readings: &[(String, String)], plot_width: Option<i64>) -> Vec<Vec<String>> {
    if readings.is_empty() {
        return Vec::new();
    }
    let budget = ((plot_width.unwrap_or(READING_PLOT_WIDTH)) as f64 - 2.0 * LABEL_INSET_PX)
        .max(LABEL_FONT_PX);
    readings
        .iter()
        .map(|(_, text)| wrap_label(text, budget))
        .collect()
}

/// The pixel height of a line panel's plot area, as `svg_line_chart` lays it out.
///
/// A float, because `ARM_READING_LINE_HEIGHT_PX` is one: Python's arithmetic
/// mixes them and its f-strings print `24.0` where a bar panel prints `24`, so
/// a plot height kept as an integer would spell the same geometry differently.
pub fn line_plot_height(series_count: usize, reading_rows: usize) -> f64 {
    let legend_columns = series_count.clamp(1, 4);
    let legend_rows = ceil_usize(series_count.max(1) as f64 / legend_columns as f64);
    HEIGHT as f64
        - (PAD_TOP as f64
            + (legend_rows as i64 - 1) as f64 * 18.0
            + reading_rows as f64 * ARM_READING_LINE_HEIGHT_PX)
        - PAD_BOTTOM as f64
}

/// The pixel height of a bar panel's plot area, as `svg_bar_chart` lays it out.
pub fn bar_plot_height(series_count: usize) -> i64 {
    let legend_columns = series_count.clamp(1, LEGEND_COLUMNS);
    let legend_rows = ceil_usize(series_count.max(1) as f64 / legend_columns as f64);
    HEIGHT - (PAD_TOP + (legend_rows as i64 - 1) * 18) - PAD_BOTTOM
}

/// Python's `points[::step]` decimation, with the report's own 3000-point limit.
pub fn decimate(points: &[(f64, f64)]) -> Vec<(f64, f64)> {
    decimate_limit(points, 3000)
}

/// [`decimate`] with the limit stated.
pub fn decimate_limit(points: &[(f64, f64)], limit: usize) -> Vec<(f64, f64)> {
    if points.len() <= limit {
        return points.to_vec();
    }
    let step = ceil_usize(points.len() as f64 / limit as f64).max(1);
    points.iter().step_by(step).cloned().collect()
}

/// The auto-computed y extent of a set of series, with the report's own margin.
pub fn finite_extent(series: &Series) -> (f64, f64) {
    let values: Vec<f64> = series
        .iter()
        .flat_map(|(_, points)| points.iter().map(|(_, value)| *value))
        .filter(|value| value.is_finite())
        .collect();
    if values.is_empty() {
        return (0.0, 1.0);
    }
    let low = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let high = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if low == high {
        let margin = (low.abs() * 0.05).max(1.0);
        return (low - margin, high + margin);
    }
    let margin = (high - low) * 0.05;
    (low - margin, high + margin)
}

/// Widen an auto-computed y extent so every bound line stays on canvas.
pub fn extent_including_bounds(extent: (f64, f64), bounds: &[(f64, String)]) -> (f64, f64) {
    if bounds.is_empty() {
        return extent;
    }
    let low = bounds
        .iter()
        .map(|(value, _)| *value)
        .fold(extent.0, f64::min);
    let high = bounds
        .iter()
        .map(|(value, _)| *value)
        .fold(extent.1, f64::max);
    if (low, high) == extent {
        return extent;
    }
    if low == high {
        let margin = (low.abs() * 0.05).max(1.0);
        return (low - margin, high + margin);
    }
    let margin = (high - low) * 0.05;
    (low - margin, high + margin)
}

/// The area, in square pixels, two `(x0, y0, x1, y1)` boxes share.
pub fn box_overlap(first: (f64, f64, f64, f64), second: (f64, f64, f64, f64)) -> f64 {
    let width = first.2.min(second.2) - first.0.max(second.0);
    let height = first.3.min(second.3) - first.1.max(second.1);
    width.max(0.0) * height.max(0.0)
}

/// The x-window a bound is drawn over: its declared governance, or its arms.
pub fn drawn_bound_window(bound: &Bound) -> Option<(f64, f64)> {
    if let Some(window) = bound_governed_x(bound) {
        return Some(window);
    }
    let arms = bound.window.as_ref()?;
    if arms.is_empty() {
        return None;
    }
    let low = arms.iter().cloned().fold(f64::INFINITY, f64::min);
    let high = arms.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    Some((low, high))
}

/// The note an axis that is not 0-based has to carry, or `""`.
pub fn band_view_note(extent: (f64, f64)) -> String {
    let (low, high) = extent;
    if low <= 0.0 {
        return String::new();
    }
    let decimals = 2usize.max(ceil_usize(-(high - low).log10()) + 2);
    format!(
        "band view {}..{}, not 0-based",
        fdec_g(low, decimals),
        fdec_g(high, decimals)
    )
}

/// The lines a panel's note draws as, and the baselines they are placed at.
pub fn note_baselines(
    text: &str,
    plot_top: f64,
    plot_bottom: f64,
    budget: f64,
    note_row: usize,
    label_area: &[(f64, f64, f64, f64)],
) -> Vec<(String, f64)> {
    if text.is_empty() {
        return Vec::new();
    }
    let lines = wrap_label(text, budget);
    let width = lines
        .iter()
        .map(|line| label_text_width(line))
        .fold(0.0, f64::max);
    let left = PAD_LEFT as f64 + 5.0;

    let block = |row: usize| -> Vec<f64> {
        (0..lines.len())
            .map(|index| plot_top + 13.0 + (row + index) as f64 * LABEL_LINE_HEIGHT_PX)
            .collect()
    };
    let boxes = |baselines: &[f64]| -> Vec<(f64, f64, f64, f64)> {
        baselines
            .iter()
            .map(|y| {
                (
                    left,
                    y - LABEL_ASCENT_PX,
                    left + width,
                    y + LABEL_DESCENT_PX,
                )
            })
            .collect()
    };

    let top = block(note_row);
    let mut last_row = note_row;
    while block(last_row)[lines.len() - 1] + LABEL_DESCENT_PX <= plot_bottom - 4.0 {
        last_row += 1;
    }
    for row in note_row..last_row {
        let baselines = block(row);
        if boxes(&baselines).iter().all(|box_| {
            label_area
                .iter()
                .all(|other| box_overlap(*box_, *other) <= LABEL_OVERLAP_PX2)
        }) {
            return lines.into_iter().zip(baselines).collect();
        }
    }
    lines.into_iter().zip(top).collect()
}

/// The arguments of one line/CDF chart.
pub struct LineChart<'a> {
    pub title: &'a str,
    pub x_label: &'a str,
    pub y_label: &'a str,
    pub series: &'a Series,
    pub y_extent: Option<(f64, f64)>,
    pub bounds: &'a [(f64, String)],
    pub walls: bool,
    pub markers: bool,
    pub readings: &'a [(String, String)],
    pub note: &'a str,
    pub x_bounds: &'a [(f64, String)],
    pub x_scale: &'a str,
    pub y_clip: Option<f64>,
}

/// A line or CDF chart, with optional labelled bound lines on either axis.
pub fn svg_line_chart(chart: &LineChart<'_>) -> String {
    let series: Series = chart
        .series
        .iter()
        .filter(|(_, points)| !points.is_empty())
        .map(|(name, points)| (name.clone(), decimate(points)))
        .collect();
    if series.is_empty() {
        return format!(
            "<section><h2>{}</h2><p>No samples.</p></section>",
            pyjson::escape(chart.title)
        );
    }
    let xs: Vec<f64> = series
        .iter()
        .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
        .collect();
    let x_min = xs.iter().cloned().fold(f64::INFINITY, f64::min);
    let mut x_max = xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if x_min == x_max {
        x_max = x_min + 1.0;
    }
    let (y_min, y_max) = match chart.y_extent {
        Some(extent) => extent,
        None => extent_including_bounds(finite_extent(&series), chart.bounds),
    };
    let legend_columns = series.len().min(4);
    let legend_rows = ceil_usize(series.len() as f64 / legend_columns as f64);
    let legend_bottom = PAD_TOP + (legend_rows as i64 - 1) * 18;
    let plot_width = WIDTH - PAD_LEFT - PAD_RIGHT;
    let wrapped = reading_lines(chart.readings, Some(plot_width));
    let reading_rows: usize = wrapped.iter().map(Vec::len).sum();
    let note_rows: Vec<String> = if chart.note.is_empty() {
        Vec::new()
    } else {
        wrap_label(chart.note, plot_width as f64 - 2.0 * LABEL_INSET_PX)
    };
    let plot_top =
        legend_bottom as f64 + (reading_rows + note_rows.len()) as f64 * ARM_READING_LINE_HEIGHT_PX;
    let plot_height = HEIGHT as f64 - plot_top - PAD_BOTTOM as f64;

    let logarithmic = chart.x_scale == "log" && x_min > 0.0 && x_max > x_min;
    let log_low = if logarithmic { x_min.log10() } else { 0.0 };
    let log_span = if logarithmic {
        x_max.log10() - log_low
    } else {
        0.0
    };
    let sx = |value: f64| -> f64 {
        if logarithmic && value > 0.0 {
            PAD_LEFT as f64 + (value.log10() - log_low) / log_span * plot_width as f64
        } else {
            PAD_LEFT as f64 + (value - x_min) / (x_max - x_min) * plot_width as f64
        }
    };
    let sy = |value: f64| -> f64 {
        if let Some(clip) = chart.y_clip
            && value > clip
        {
            return plot_top + Y_CLIP_INSET_PX;
        }
        plot_top + (y_max - value) / (y_max - y_min) * plot_height
    };

    let mut parts = String::new();
    let _ = std::fmt::Write::write_fmt(
        &mut parts,
        format_args!(
            "<section><h2>{}</h2><svg viewBox=\"0 0 {WIDTH} {HEIGHT}\" role=\"img\">",
            pyjson::escape(chart.title)
        ),
    );
    let _ = std::fmt::Write::write_fmt(
        &mut parts,
        format_args!(
            "<rect x=\"{PAD_LEFT}\" y=\"{}\" width=\"{plot_width}\" \
             height=\"{}\" class=\"plot-bg\"/>",
            frepr(plot_top),
            frepr(plot_height)
        ),
    );
    if chart.y_clip.is_some() {
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<line class=\"y-clip\" x1=\"{PAD_LEFT}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" \
                 stroke=\"{BOUND_STROKE}\" stroke-width=\"1.4\"/>",
                f1(plot_top),
                WIDTH - PAD_RIGHT,
                f1(plot_top)
            ),
        );
    }
    for tick in 0..6 {
        let fraction = tick as f64 / 5.0;
        let (x_value, tick_text) = if logarithmic {
            let value = 10.0_f64.powf(log_low + log_span * fraction);
            (value, f4g(value))
        } else {
            let value = x_min + (x_max - x_min) * fraction;
            (value, f1(value))
        };
        let x = sx(x_value);
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"grid\"/>",
                f1(x),
                frepr(plot_top),
                f1(x),
                HEIGHT - PAD_BOTTOM
            ),
        );
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<text x=\"{}\" y=\"{}\" text-anchor=\"middle\">{tick_text}</text>",
                f1(x),
                HEIGHT - 24
            ),
        );
        let y_value = y_min + (y_max - y_min) * fraction;
        let y = sy(y_value);
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<line x1=\"{PAD_LEFT}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"grid\"/>",
                f1(y),
                WIDTH - PAD_RIGHT,
                f1(y)
            ),
        );
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<text x=\"{}\" y=\"{}\" text-anchor=\"end\">{}</text>",
                PAD_LEFT - 9,
                f1(y + 4.0),
                f2(y_value)
            ),
        );
    }
    for (index, (_, points)) in series.iter().enumerate() {
        let color = COLORS[index % COLORS.len()];
        let holes = if chart.walls {
            series_walls(points)
        } else {
            Vec::new()
        };
        for run in split_at_walls(points, &holes) {
            if run.len() < 2 {
                continue;
            }
            let path: Vec<String> = run
                .iter()
                .map(|(x, y)| format!("{},{}", f1(sx(*x)), f1(sy(*y))))
                .collect();
            let _ = std::fmt::Write::write_fmt(
                &mut parts,
                format_args!(
                    "<polyline points=\"{}\" fill=\"none\" stroke=\"{color}\" \
                     stroke-width=\"1.7\"/>",
                    path.join(" ")
                ),
            );
        }
        if chart.markers {
            for (x, y) in points {
                let _ = std::fmt::Write::write_fmt(
                    &mut parts,
                    format_args!(
                        "<circle class=\"sample\" cx=\"{}\" cy=\"{}\" r=\"{}\" \
                         fill=\"{color}\"/>",
                        f1(sx(*x)),
                        f1(sy(*y)),
                        frepr(SAMPLE_MARKER_RADIUS_PX)
                    ),
                );
            }
        }
    }
    let mut bound_label_boxes: Vec<(f64, f64, f64, f64)> = Vec::new();
    for (y_value, label) in chart.bounds {
        let y = sy(*y_value);
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<line class=\"bound\" x1=\"{PAD_LEFT}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" \
                 stroke=\"{BOUND_STROKE}\" stroke-width=\"1.4\" stroke-dasharray=\"6 4\"/>",
                f1(y),
                WIDTH - PAD_RIGHT,
                f1(y)
            ),
        );
        let (markup, layout) = bound_label_markup(
            label,
            (WIDTH - PAD_RIGHT) as f64,
            y,
            (
                PAD_LEFT as f64,
                plot_top,
                (WIDTH - PAD_RIGHT) as f64,
                (HEIGHT - PAD_BOTTOM) as f64,
            ),
            BOUND_LABEL_STYLE,
        );
        parts.push_str(&markup);
        bound_label_boxes.extend(layout.boxes);
    }
    for (x_value, label) in chart.x_bounds {
        let x = sx(*x_value);
        let plot = (
            PAD_LEFT as f64,
            plot_top,
            (WIDTH - PAD_RIGHT) as f64,
            (HEIGHT - PAD_BOTTOM) as f64,
        );
        if (PAD_LEFT as f64) <= x && x <= (WIDTH - PAD_RIGHT) as f64 {
            let _ = std::fmt::Write::write_fmt(
                &mut parts,
                format_args!(
                    "<line class=\"x-bound\" x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" \
                     stroke=\"{BOUND_STROKE}\" stroke-width=\"1.4\" \
                     stroke-dasharray=\"6 4\"/>",
                    f1(x),
                    f1(plot_top),
                    f1(x),
                    f1((HEIGHT - PAD_BOTTOM) as f64)
                ),
            );
        }
        let anchor_x = x.max(PAD_LEFT as f64).min((WIDTH - PAD_RIGHT) as f64);
        parts.push_str(&x_bound_label_markup(
            label,
            anchor_x,
            plot,
            BOUND_LABEL_STYLE,
        ));
    }
    if !wrapped.is_empty() {
        parts.push_str("<g class=\"arm-readings\">");
        let mut row = 0usize;
        for lines in &wrapped {
            for line in lines {
                let baseline = legend_bottom as f64
                    + ARM_READING_TOP_PX
                    + row as f64 * ARM_READING_LINE_HEIGHT_PX;
                let _ = std::fmt::Write::write_fmt(
                    &mut parts,
                    format_args!(
                        "<text class=\"arm-reading\" x=\"{}\" y=\"{}\" \
                         style=\"{ARM_READING_STYLE}\">{}</text>",
                        f1(PAD_LEFT as f64 + LABEL_INSET_PX),
                        f1(baseline),
                        pyjson::escape(line)
                    ),
                );
                row += 1;
            }
        }
        parts.push_str("</g>");
    }
    let mut note_top = plot_top + 13.0;
    if !note_rows.is_empty() && !bound_label_boxes.is_empty() {
        note_top = note_top.max(
            bound_label_boxes
                .iter()
                .map(|box_| box_.3)
                .fold(f64::NEG_INFINITY, f64::max)
                + LABEL_ASCENT_PX
                + 2.0,
        );
    }
    for (index, line) in note_rows.iter().enumerate() {
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<text class=\"panel-note\" x=\"{}\" y=\"{}\" \
                 style=\"{NOTE_LABEL_STYLE}\">{}</text>",
                f1(PAD_LEFT as f64 + LABEL_INSET_PX),
                f1(note_top + index as f64 * ARM_READING_LINE_HEIGHT_PX),
                pyjson::escape(line)
            ),
        );
    }
    let _ = std::fmt::Write::write_fmt(
        &mut parts,
        format_args!(
            "<text x=\"{}\" y=\"{}\" text-anchor=\"middle\">{}</text>",
            frepr(WIDTH as f64 / 2.0),
            HEIGHT - 5,
            pyjson::escape(chart.x_label)
        ),
    );
    let _ = std::fmt::Write::write_fmt(
        &mut parts,
        format_args!(
            "<text x=\"18\" y=\"{}\" text-anchor=\"middle\" transform=\"rotate(-90 18 {})\">{}</text>",
            frepr(HEIGHT as f64 / 2.0),
            frepr(HEIGHT as f64 / 2.0),
            pyjson::escape(chart.y_label)
        ),
    );
    parts.push_str("<g class=\"legend\">");
    for (index, (name, _)) in series.iter().enumerate() {
        let column = index % legend_columns;
        let row = index / legend_columns;
        let x = PAD_LEFT as f64 + column as f64 * (plot_width as f64 / legend_columns as f64);
        let y = 14 + row as i64 * 18;
        let color = COLORS[index % COLORS.len()];
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<line x1=\"{}\" y1=\"{y}\" x2=\"{}\" y2=\"{y}\" stroke=\"{color}\" \
                 stroke-width=\"3\"/>",
                f1(x),
                f1(x + 20.0)
            ),
        );
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<text x=\"{}\" y=\"{}\">{}</text>",
                f1(x + 25.0),
                y + 4,
                pyjson::escape(name)
            ),
        );
    }
    parts.push_str("</g></svg></section>");
    parts
}

/// One or more empirical CDFs on a fixed 0-100% percentile axis.
pub fn svg_cdf_chart(chart: &LineChart<'_>) -> String {
    let fixed = LineChart {
        title: chart.title,
        x_label: chart.x_label,
        y_label: chart.y_label,
        series: chart.series,
        y_extent: Some((0.0, 100.0)),
        bounds: chart.bounds,
        walls: false,
        markers: false,
        readings: &[],
        note: chart.note,
        x_bounds: chart.x_bounds,
        x_scale: chart.x_scale,
        y_clip: chart.y_clip,
    };
    svg_line_chart(&fixed)
}

/// Grouped bars for `[(name, [(x, y)])]` on the axis `bar_axis_extent` picks.
#[allow(clippy::too_many_arguments)]
pub fn svg_bar_chart(
    title: &str,
    x_label: &str,
    y_label: &str,
    series: &Series,
    bounds: &[Bound],
    extent: Option<(f64, f64)>,
    run_values: Option<&J>,
    note: &str,
) -> String {
    let series: Series = series
        .iter()
        .filter(|(_, points)| !points.is_empty())
        .cloned()
        .collect();
    if series.is_empty() {
        return format!(
            "<section><h2>{}</h2><p>No samples.</p></section>",
            pyjson::escape(title)
        );
    }
    let xs: Vec<f64> = series
        .iter()
        .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
        .collect();
    let mut categories: Vec<f64> = xs.clone();
    categories.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    categories.dedup();
    let (x_min, x_max) = bar_x_extent(&categories);
    let (y_min, y_max) =
        extent.unwrap_or_else(|| bar_axis_extent(&series, bounds, run_values, None));
    let legend_columns = series.len().min(LEGEND_COLUMNS);
    let legend_rows = ceil_usize(series.len() as f64 / legend_columns as f64);
    let plot_top = PAD_TOP + (legend_rows as i64 - 1) * 18;
    let plot_width = WIDTH - PAD_LEFT - PAD_RIGHT;
    let plot_height = HEIGHT - plot_top - PAD_BOTTOM;
    let sx = |value: f64| -> f64 {
        PAD_LEFT as f64 + (value - x_min) / (x_max - x_min) * plot_width as f64
    };
    let sy = |value: f64| -> f64 {
        plot_top as f64 + (y_max - value) / (y_max - y_min) * plot_height as f64
    };
    let band = plot_width as f64 / categories.len() as f64;
    let inset = (band * BAR_BAND_INSET_SHARE).min(band / 2.0);
    let bar_slot = ((band - 2.0 * inset) / series.len() as f64).max(0.0);
    let bar_gap = (bar_slot * BAR_GAP_SHARE).min(bar_slot / 2.0);
    let bar_width = (bar_slot - bar_gap).max(0.0);
    let baseline = sy(bar_baseline_value((y_min, y_max)));
    let step = (y_max - y_min) / 5.0;
    let mut y_decimals = 2usize;
    if step > 0.0 {
        y_decimals = (ceil_usize(-step.log10()) + 1).clamp(2, 6);
    }
    let mut parts = String::new();
    let _ = std::fmt::Write::write_fmt(
        &mut parts,
        format_args!(
            "<section><h2>{}</h2><svg viewBox=\"0 0 {WIDTH} {HEIGHT}\" role=\"img\">",
            pyjson::escape(title)
        ),
    );
    let _ = std::fmt::Write::write_fmt(
        &mut parts,
        format_args!(
            "<rect x=\"{PAD_LEFT}\" y=\"{plot_top}\" width=\"{plot_width}\" \
             height=\"{plot_height}\" class=\"plot-bg\"/>"
        ),
    );
    for tick in 0..6 {
        let fraction = tick as f64 / 5.0;
        let x_value = x_min + (x_max - x_min) * fraction;
        let x = sx(x_value);
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<line x1=\"{}\" y1=\"{plot_top}\" x2=\"{}\" y2=\"{}\" class=\"grid\"/>",
                f1(x),
                f1(x),
                HEIGHT - PAD_BOTTOM
            ),
        );
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<text x=\"{}\" y=\"{}\" text-anchor=\"middle\">{}</text>",
                f1(x),
                HEIGHT - 24,
                f2(x_value)
            ),
        );
        let y_value = y_min + (y_max - y_min) * fraction;
        let y = sy(y_value);
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<line x1=\"{PAD_LEFT}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"grid\"/>",
                f1(y),
                WIDTH - PAD_RIGHT,
                f1(y)
            ),
        );
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<text x=\"{}\" y=\"{}\" text-anchor=\"end\">{}</text>",
                PAD_LEFT - 9,
                f1(y + 4.0),
                tick_label(y_value, y_decimals)
            ),
        );
    }
    for (category_index, category) in categories.iter().enumerate() {
        for (index, (_, points)) in series.iter().enumerate() {
            let color = COLORS[index % COLORS.len()];
            for (x_value, y_value) in points {
                if x_value != category {
                    continue;
                }
                let left = PAD_LEFT as f64
                    + category_index as f64 * band
                    + inset
                    + index as f64 * bar_slot;
                let top = sy(*y_value);
                let height = (top - baseline).abs();
                if height == 0.0 {
                    // A value at the baseline has no bar to paint, so a
                    // zero-height <rect> is not drawable and the panel would
                    // carry no geometry for a value the producer measured.
                    // The floor mark is hollow and dashed, which no filled bar
                    // is, so it can never be read as a small non-zero bar: its
                    // whole paint is an outline, where every bar is a solid
                    // fill.
                    let mark_top = baseline - ZERO_BAR_MARK_HEIGHT_PX;
                    let _ = std::fmt::Write::write_fmt(
                        &mut parts,
                        format_args!(
                            "<rect class=\"{ZERO_BAR_CLASS}\" x=\"{}\" y=\"{}\" \
                             width=\"{}\" height=\"{}\" fill=\"none\" stroke=\"{color}\" \
                             stroke-width=\"{}\" stroke-dasharray=\"{ZERO_BAR_DASH}\"/>",
                            f1(left),
                            f1(mark_top),
                            f1(bar_width),
                            f1(ZERO_BAR_MARK_HEIGHT_PX),
                            f1(ZERO_BAR_STROKE_WIDTH_PX)
                        ),
                    );
                    continue;
                }
                let _ = std::fmt::Write::write_fmt(
                    &mut parts,
                    format_args!(
                        "<rect x=\"{}\" y=\"{}\" width=\"{}\" height=\"{}\" fill=\"{color}\"/>",
                        f1(left),
                        f1(top.min(baseline)),
                        f1(bar_width),
                        f1((top - baseline).abs())
                    ),
                );
            }
        }
    }
    let mut label_area: Vec<(f64, f64, f64, f64)> = Vec::new();
    for bound in bounds {
        let y = sy(bound.y);
        let (left, right) = match drawn_bound_window(bound) {
            None => (PAD_LEFT as f64, (WIDTH - PAD_RIGHT) as f64),
            Some(window) => {
                let a = (sx(window.0) - band / 2.0).min(sx(window.1) + band / 2.0);
                let b = (sx(window.0) - band / 2.0).max(sx(window.1) + band / 2.0);
                let left = a.max(PAD_LEFT as f64);
                let right = b.min((WIDTH - PAD_RIGHT) as f64);
                (left.min(right), left.max(right))
            }
        };
        let label = governed_label(bound, &series, run_values, true);
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<line class=\"bound\" x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" \
                 stroke=\"{BOUND_STROKE}\" stroke-width=\"1.4\" stroke-dasharray=\"6 4\"/>",
                f1(left),
                f1(y),
                f1(right),
                f1(y)
            ),
        );
        if bound.unlabelled {
            continue;
        }
        let (markup, layout) = bound_label_markup(
            &label,
            right,
            y,
            (
                PAD_LEFT as f64,
                plot_top as f64,
                (WIDTH - PAD_RIGHT) as f64,
                (HEIGHT - PAD_BOTTOM) as f64,
            ),
            BAR_BOUND_LABEL_STYLE,
        );
        parts.push_str(&markup);
        label_area.extend(layout.boxes);
    }
    let band_note = band_view_note((y_min, y_max));
    let mut note_row = 0usize;
    if !band_note.is_empty() {
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<text class=\"axis-note\" x=\"{}\" y=\"{}\" style=\"{BAR_BOUND_LABEL_STYLE}\">{}</text>",
                f1(PAD_LEFT as f64 + 5.0),
                f1(plot_top as f64 + 13.0),
                pyjson::escape(&band_note)
            ),
        );
        note_row += 1;
    }
    for (line, y) in note_baselines(
        note,
        plot_top as f64,
        (HEIGHT - PAD_BOTTOM) as f64,
        plot_width as f64 - 2.0 * LABEL_INSET_PX,
        note_row,
        &label_area,
    ) {
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<text class=\"panel-note\" x=\"{}\" y=\"{}\" style=\"{BAR_BOUND_LABEL_STYLE}\">{}</text>",
                f1(PAD_LEFT as f64 + 5.0),
                f1(y),
                pyjson::escape(&line)
            ),
        );
    }
    let _ = std::fmt::Write::write_fmt(
        &mut parts,
        format_args!(
            "<text x=\"{}\" y=\"{}\" text-anchor=\"middle\">{}</text>",
            frepr(WIDTH as f64 / 2.0),
            HEIGHT - 5,
            pyjson::escape(x_label)
        ),
    );
    let _ = std::fmt::Write::write_fmt(
        &mut parts,
        format_args!(
            "<text x=\"18\" y=\"{}\" text-anchor=\"middle\" transform=\"rotate(-90 18 {})\">{}</text>",
            frepr(HEIGHT as f64 / 2.0),
            frepr(HEIGHT as f64 / 2.0),
            pyjson::escape(y_label)
        ),
    );
    parts.push_str("<g class=\"legend\">");
    for (index, (name, _)) in series.iter().enumerate() {
        let column = index % legend_columns;
        let row = index / legend_columns;
        let x = PAD_LEFT as f64 + column as f64 * (plot_width as f64 / legend_columns as f64);
        let y = 14 + row as i64 * 18;
        let color = COLORS[index % COLORS.len()];
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<line x1=\"{}\" y1=\"{y}\" x2=\"{}\" y2=\"{y}\" stroke=\"{color}\" \
                 stroke-width=\"4\"/>",
                f1(x),
                f1(x + 20.0)
            ),
        );
        let _ = std::fmt::Write::write_fmt(
            &mut parts,
            format_args!(
                "<text x=\"{}\" y=\"{}\">{}</text>",
                f1(x + 25.0),
                y + 4,
                pyjson::escape(&series_label(name))
            ),
        );
    }
    parts.push_str("</g></svg></section>");
    parts
}

/// The x extent a categorical bar panel is drawn over, as `(low, high)`.
pub fn bar_x_extent(categories: &[f64]) -> (f64, f64) {
    let slot = categories
        .windows(2)
        .map(|pair| pair[1] - pair[0])
        .filter(|step| *step > 0.0)
        .fold(f64::INFINITY, f64::min);
    let slot = if slot.is_finite() { slot } else { 1.0 };
    (
        categories[0] - slot / 2.0,
        categories[categories.len() - 1] + slot / 2.0,
    )
}

/// The y extent for one bar panel, as `(low, high)`.
pub fn bar_axis_extent(
    series: &Series,
    bounds: &[Bound],
    run_values: Option<&J>,
    plot_height: Option<i64>,
) -> (f64, f64) {
    let values = bound_values(series);
    let ys: Vec<f64> = bounds.iter().map(|bound| bound.y).collect();
    let guards = named_guard_values(series, bounds, run_values, true);
    let plot_height = plot_height.unwrap_or_else(|| bar_plot_height(series.len()));
    let unit = unit_span(&values, &ys);
    if let Some(unit) = unit {
        for bound in bounds {
            if bound_is_the_scale(&values, bound.y, Some(unit)) {
                let band = bound_band(&values, bound.y, Some(unit), &[]);
                let span = 2.0 * band;
                let headroom = FRAME_HEADROOM * span;
                return (unit - span - headroom, unit + headroom);
            }
        }
    }
    let named: Vec<f64> = ys.iter().chain(guards.iter()).cloned().collect();
    let low = std::iter::once(0.0)
        .chain(values.iter().cloned())
        .chain(named.iter().cloned())
        .fold(f64::INFINITY, f64::min);
    let high = std::iter::once(0.0)
        .chain(values.iter().cloned())
        .chain(named.iter().cloned())
        .fold(f64::NEG_INFINITY, f64::max);
    let floors: Vec<f64> = bounds
        .iter()
        .filter(|bound| {
            bound.band_arm.as_deref() == Some("lower") || failure_side(&values, bound.y) < 0.0
        })
        .map(|bound| bound.y)
        .collect();
    let top = named.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let top = if named.is_empty() { high } else { top };
    let bottom = if floors.is_empty() {
        None
    } else {
        Some(floors.iter().cloned().fold(f64::INFINITY, f64::min))
    };
    axis_with_headroom(low, high, top, plot_height as f64, bottom)
}

/// Widen an axis so a value past its highest (or lowest) bound is drawable.
pub fn axis_with_headroom(
    low: f64,
    high: f64,
    top: f64,
    plot_height: f64,
    bottom: Option<f64>,
) -> (f64, f64) {
    let mut low = low;
    let mut high = high;
    let span = high - low;
    if span > 0.0 {
        high += FRAME_HEADROOM * span;
    }
    // The margin the *policy* targets is a hair over the one the checks demand:
    // a boundary met only to the last bit of a double reads as `5.999... px` to
    // `check_bound_headroom` and would refuse the panel the policy just widened.
    let minimum = MIN_HEADROOM_PIXELS * (1.0 + 1e-6) / plot_height;
    if top > high {
        return (low, high);
    }
    if minimum < 1.0 && high - top < minimum * (high - low) {
        high = high.max((top - minimum * low) / (1.0 - minimum));
    }
    if let Some(bottom) = bottom
        && minimum < 1.0
    {
        if bottom < low {
            return (low, high);
        }
        if bottom - low < minimum * (high - low) {
            low = low.min((bottom - minimum * high) / (1.0 - minimum));
        }
    }
    (low, high)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_label_wider_than_its_budget_is_broken_and_a_narrow_one_is_not() {
        let narrow = wrap_label("short label", 200.0);
        assert_eq!(narrow, vec!["short label".to_string()]);
        // Every line of an uncapped wrap fits its budget; the cap is what can
        // exceed it, so the capped form is where a label too long to lay out
        // becomes visible to `check_label_fit` instead of being drawn.
        let broken = wrap_label_lines("averyveryverylongsingletokenindeed", 40.0, 100);
        assert!(broken.len() > 1, "{broken:?}");
        assert!(broken.iter().all(|line| label_text_width(line) <= 40.0));
        let capped = wrap_label("averyveryverylongsingletokenindeed", 40.0);
        assert!(capped.len() <= LABEL_MAX_LINES);
        assert!(capped.iter().any(|line| label_text_width(line) > 40.0));
        // Vacuity: the same text at a budget it fits in is one line, so the
        // break above measures the budget and not the wrapper's eagerness.
        assert_eq!(
            wrap_label("averyveryverylongsingletokenindeed", 500.0).len(),
            1
        );
    }

    #[test]
    fn a_hole_in_the_sampling_is_told_from_a_steady_cadence() {
        let steady: Vec<(f64, f64)> = (0..10).map(|index| (index as f64, 1.0)).collect();
        assert!(series_walls(&steady).is_empty());
        let mut holed = steady.clone();
        for point in holed.iter_mut().skip(5) {
            point.0 += 30.0;
        }
        let walls = series_walls(&holed);
        assert_eq!(walls.len(), 1);
        assert_eq!(walls[0].0, 4);
        assert_eq!(split_at_walls(&holed, &walls).len(), 2);
        // Two points are a chord, not a series with a cadence.
        assert!(series_walls(&[(0.0, 1.0), (10.0, 2.0)]).is_empty());
    }

    #[test]
    fn the_axis_policy_spends_the_pixel_floor_when_the_span_cannot() {
        // A line panel with a clip: the axis is drawn to the value the panel is
        // read against, and a reading band leaves it short enough that the
        // span's own 5 % is under the six pixels a crossing bar needs.
        let (low, high) = axis_with_headroom(20.0, 250.0, 250.0, 100.0, None);
        let headroom = (high - 250.0) / (high - low) * 100.0;
        assert!(headroom >= MIN_HEADROOM_PIXELS, "{headroom}");
        // Vacuity: the frame's own 5 % of the bumped span is 4.76 px here, so
        // the demand above is met by the pixel floor and not by it -- and the
        // floor is why the branch exists.
        let span_share = FRAME_HEADROOM / (1.0 + FRAME_HEADROOM) * 100.0;
        assert!(span_share < MIN_HEADROOM_PIXELS, "{span_share}");
        // A tall enough plot meets the demand from the span alone, so the floor
        // is not what every panel relies on.
        let (low2, high2) = axis_with_headroom(20.0, 250.0, 250.0, 228.0, None);
        let headroom2 = (high2 - 250.0) / (high2 - low2) * 228.0;
        assert!((headroom2 - FRAME_HEADROOM / (1.0 + FRAME_HEADROOM) * 228.0).abs() < 1e-9);
    }

    #[test]
    fn a_floor_far_below_the_data_keeps_the_zero_baseline() {
        let series: Series = vec![(
            "fraction".to_string(),
            vec![(1.0, 0.958217), (2.0, 0.958271)],
        )];
        let bounds = vec![Bound::new(0.35, "M3 floor 0.35x link rate".to_string())];
        assert_eq!(bar_axis_extent(&series, &bounds, None, None).0, 0.0);
    }

    #[test]
    fn the_label_width_model_does_not_underestimate_the_rendered_text() {
        // Widths measured on real standalone panels by headless Chrome
        // (`getBBox().width`, 11 px text with no font-family declared,
        // resolved as Times). The model is a *model*, so the only thing that
        // keeps it honest is a check that fails when it starts
        // underestimating the drawn text.
        let rendered: [(&str, f64); 8] = [
            (
                "M2 non-degrading p99 bound (ms) [1 of 3 bars beyond it; run guards \
                 hostile_p99_guard=200 lone_p99_guard=400]",
                512.22,
            ),
            (
                "M1 ceiling 250 ms [4 of 16 bars beyond it; run guards \
                 hostile_p99_guard=900]",
                349.0,
            ),
            ("M4 per-flow delivery floor 0.995", 144.94),
            ("fair-share bound \u{b1}1.0%", 103.94),
            ("fair share 25.0%", 72.36),
            ("M3 floor 0.35x link rate", 105.37),
            ("M2 delivery floor 1.000", 105.1),
            ("M1 ceiling 250 ms", 82.77),
        ];
        for (label, measured) in rendered {
            assert!(
                label_text_width(label) >= measured,
                "{label:?}: the model {} is narrower than the drawn {measured}",
                label_text_width(label)
            );
        }
        // Vacuity: a model narrowed below the measured widths would fail the
        // assertion above, so it is about the model and not a tautology. The
        // safety factor is what carries the margin, and 0.2 of it is below the
        // narrowest measured label.
        let narrowed = |text: &str| {
            text.chars()
                .map(|character| label_char_advance(character) * 0.2 / LABEL_ADVANCE_SAFETY)
                .sum::<f64>()
        };
        let (label, measured) = rendered[0];
        assert!(narrowed(label) < measured);
    }
}
