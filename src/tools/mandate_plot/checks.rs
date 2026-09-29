//! The panel checks, each of them a measurement of the artifact that was
//! written rather than a trust in the code that wrote it.
//!
//! `AGENTS.md` makes a panel that cannot show the failure it is drawn for a
//! defect of the same family as an assertion that cannot fail, and these are
//! the refusals that enforce it: the axis and headroom tests (a bound the axis
//! cannot resolve is refused), the sliver statement (a sub-pixel bound the run's
//! data has clearly departed from is stated on the panel's face instead), the
//! clip statement (an outlier may not set a line panel's axis silently), the
//! summary test (a panel owes a machine-readable statement of what it drew,
//! measured back out of the drawn points and lines), the governance tests (a
//! bound drawn across arms the run bounds differently names what governs each),
//! the label-fit/overlap tests, the bar-separation test, the legend test, the
//! tick-resolution test, the gap test (no segment drawn across a hole in the
//! sampling) and the stated-reading/stated-number tests.
//!
//! Every reader here parses the markup the renderer wrote, so a check cannot
//! pass on a panel whose text never reached the SVG.

// `!(x > y)` is deliberate throughout this module: it is Python's own
// `not (x > y)`, and unlike `x <= y` it is true for a NaN. The values here come
// from a CSV whose finiteness was checked on the way in, but the port keeps the
// Python spelling rather than trading its semantics for a clearer `partial_cmp`.
#![allow(clippy::neg_cmp_op_on_partial_ord)]

use super::draw;
use super::draw::*;
use super::*;

// -- reading the artifact back -------------------------------------------------

/// The reading lines a panel actually draws, joined into one sentence each.
pub fn drawn_readings(markup: &str) -> Vec<String> {
    let Some(group) = readings_group_re().search(markup) else {
        return Vec::new();
    };
    let body = group.group(1).unwrap_or_default();
    text_element_re()
        .find_all(&body)
        .iter()
        .map(|groups| {
            pyjson::unescape(
                &bound_label_title_re().replace_all(
                    groups
                        .get(1)
                        .and_then(|value| value.clone())
                        .as_deref()
                        .unwrap_or(""),
                    "",
                ),
            )
        })
        .collect()
}

/// How many series segments a panel draws per stroke colour.
pub fn drawn_polylines(markup: &str) -> BTreeMap<String, usize> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for groups in polyline_element_re().find_all(markup) {
        let attributes = groups[0].clone().unwrap_or_default();
        let values: Vec<(String, String)> = text_attribute_re()
            .find_all(&attributes)
            .iter()
            .map(|pair| {
                (
                    pair[0].clone().unwrap_or_default(),
                    pair[1].clone().unwrap_or_default(),
                )
            })
            .collect();
        let lookup = |key: &str| {
            values
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.clone())
        };
        if lookup("fill").as_deref() != Some("none") {
            continue;
        }
        if let Some(stroke) = lookup("stroke") {
            *counts.entry(stroke).or_insert(0) += 1;
        }
    }
    counts
}

/// The notes a panel draws on its own face, in draw order.
pub fn drawn_notes(markup: &str) -> Vec<String> {
    panel_note_re()
        .find_all(markup)
        .iter()
        .map(|groups| {
            let content = groups[2].clone().unwrap_or_default();
            pyjson::unescape(&bound_label_title_re().replace_all(&content, ""))
                .trim()
                .to_string()
        })
        .collect()
}

/// Each drawn note line as `(text, (x0, y0, x1, y1))`.
pub fn note_boxes(markup: &str) -> Vec<(String, (f64, f64, f64, f64))> {
    let mut boxes = Vec::new();
    for groups in panel_note_re().find_all(markup) {
        let x = groups[0].clone().unwrap_or_default();
        let y = groups[1].clone().unwrap_or_default();
        let content = groups[2].clone().unwrap_or_default();
        let text = pyjson::unescape(&bound_label_title_re().replace_all(&content, ""))
            .trim()
            .to_string();
        let left: f64 = x.parse().unwrap_or(0.0);
        let baseline: f64 = y.parse().unwrap_or(0.0);
        boxes.push((
            text.clone(),
            (
                left,
                baseline - draw::LABEL_ASCENT_PX,
                left + draw::label_text_width(&text),
                baseline + draw::LABEL_DESCENT_PX,
            ),
        ));
    }
    boxes
}

/// The y tick labels a panel draws, in draw order.
pub fn axis_tick_labels(markup: &str) -> Vec<String> {
    axis_tick_re()
        .find_all(markup)
        .iter()
        .map(|groups| groups[0].clone().unwrap_or_default())
        .collect()
}

/// The y each drawn bound line sits at, in document order.
pub fn drawn_bound_lines(markup: &str) -> Vec<f64> {
    drawn_bound_re()
        .find_all(markup)
        .iter()
        .map(|groups| groups[0].clone().unwrap_or_default().parse().unwrap_or(0.0))
        .collect()
}

/// The data value each drawn bound line sits at, on the panel's own frame.
pub fn drawn_bound_values(markup: &str, extent: (f64, f64)) -> Vec<f64> {
    let Some(rect) = plot_bg_re().search(markup) else {
        return Vec::new();
    };
    let rect = pyre_groups(&rect, 4);
    let top: f64 = rect[1].parse().unwrap_or(0.0);
    let height: f64 = rect[3].parse().unwrap_or(0.0);
    if height <= 0.0 {
        return Vec::new();
    }
    let (low, high) = extent;
    drawn_bound_lines(markup)
        .iter()
        .map(|y| high - (y - top) / height * (high - low))
        .collect()
}

fn pyre_groups(found: &crate::tools::pyre::Match, count: usize) -> Vec<String> {
    (1..=count)
        .map(|index| found.group(index).unwrap_or_default())
        .collect()
}

/// The `(left, top, right, bottom)` rectangle a panel's data is drawn in.
pub fn panel_plot_rect(panel_id: &str, markup: &str) -> PlotResult<(f64, f64, f64, f64)> {
    let Some(rect) = plot_bg_re().search(markup) else {
        return fail(format!(
            "panel {} was drawn without a plot area, so there is no rectangle its \
             bound labels could be checked against",
            pyjson::repr_str(panel_id)
        ));
    };
    let groups = pyre_groups(&rect, 4);
    let left: f64 = groups[0].parse().unwrap_or(0.0);
    let top: f64 = groups[1].parse().unwrap_or(0.0);
    let width: f64 = groups[2].parse().unwrap_or(0.0);
    let height: f64 = groups[3].parse().unwrap_or(0.0);
    Ok((left, top, left + width, top + height))
}

/// One drawn bound label: the sentence it declares, the line it draws, and the
/// box it occupies.
pub type BoundLabelBox = (String, String, (f64, f64, f64, f64));

/// Each drawn bound label as `(declared, line, (x0, y0, x1, y1))`.
pub fn label_boxes(markup: &str) -> Vec<BoundLabelBox> {
    let mut boxes = Vec::new();
    for groups in bound_label_re().find_all(markup) {
        let attributes = groups[0].clone().unwrap_or_default();
        let content = groups[1].clone().unwrap_or_default();
        let values: Vec<(String, String)> = text_attribute_re()
            .find_all(&attributes)
            .iter()
            .map(|pair| {
                (
                    pair[0].clone().unwrap_or_default(),
                    pair[1].clone().unwrap_or_default(),
                )
            })
            .collect();
        let lookup = |key: &str| {
            values
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.clone())
        };
        let titles = bound_label_title_re().find_all_whole(&content);
        let declared = pyjson::unescape(&match titles.first() {
            Some(title) => title_tag_re().replace_all(title, ""),
            None => content.clone(),
        });
        let line = pyjson::unescape(&bound_label_title_re().replace_all(&content, ""));
        let anchor: f64 = lookup("x").unwrap_or_default().parse().unwrap_or(0.0);
        let baseline: f64 = lookup("y").unwrap_or_default().parse().unwrap_or(0.0);
        let width = draw::label_text_width(&line);
        let left = match lookup("text-anchor").as_deref() {
            Some("end") => anchor - width,
            Some("middle") => anchor - width / 2.0,
            _ => anchor,
        };
        boxes.push((
            declared,
            line,
            (
                left,
                baseline - draw::LABEL_ASCENT_PX,
                left + width,
                baseline + draw::LABEL_DESCENT_PX,
            ),
        ));
    }
    boxes
}

/// Each drawn bar as `(x0, y0, x1, y1)`, in document order.
pub fn bar_boxes(markup: &str) -> Vec<(f64, f64, f64, f64)> {
    bar_rect_re()
        .find_all(markup)
        .iter()
        .map(|groups| {
            let x: f64 = groups[0].clone().unwrap_or_default().parse().unwrap_or(0.0);
            let y: f64 = groups[1].clone().unwrap_or_default().parse().unwrap_or(0.0);
            let width: f64 = groups[2].clone().unwrap_or_default().parse().unwrap_or(0.0);
            let height: f64 = groups[3].clone().unwrap_or_default().parse().unwrap_or(0.0);
            (x, y, x + width, y + height)
        })
        .collect()
}

/// A zero-height bar's floor mark, as the artifact draws it: the box it
/// occupies and how it is painted. A mark is only a mark when it is hollow,
/// which is what tells it from the filled rect every non-zero bar is.
#[derive(Debug, Clone, PartialEq)]
pub struct ZeroBarMark {
    /// `(left, top, right, bottom)` in the panel's own pixel space.
    pub box_: (f64, f64, f64, f64),
    pub fill: String,
    pub stroke: String,
}

/// Every floor mark a panel draws for a value at its bar baseline.
pub fn zero_bar_marks(markup: &str) -> Vec<ZeroBarMark> {
    let mut marks = Vec::new();
    for element in zero_bar_re().find_all_whole(markup) {
        let attributes: Vec<(String, String)> = text_attribute_re()
            .find_all(&element)
            .iter()
            .map(|pair| {
                (
                    pair[0].clone().unwrap_or_default(),
                    pair[1].clone().unwrap_or_default(),
                )
            })
            .collect();
        let lookup = |key: &str| {
            attributes
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.clone())
                .unwrap_or_default()
        };
        let (Ok(x), Ok(y), Ok(width), Ok(height)) = (
            lookup("x").parse::<f64>(),
            lookup("y").parse::<f64>(),
            lookup("width").parse::<f64>(),
            lookup("height").parse::<f64>(),
        ) else {
            continue;
        };
        marks.push(ZeroBarMark {
            box_: (x, y, x + width, y + height),
            fill: lookup("fill"),
            stroke: lookup("stroke"),
        });
    }
    marks
}

/// How many of a bar panel's measurements sit at its own bar baseline.
///
/// This is the data side of the floor mark: the drawer cannot drop it without
/// the drawn count disagreeing, and it cannot invent one for a value that is
/// not there without the same disagreement.
pub fn floor_value_count(series: &Series, extent: (f64, f64)) -> usize {
    let floor = bar_baseline_value(extent);
    series
        .iter()
        .flat_map(|(_, points)| points.iter())
        .filter(|(_, value)| *value == floor)
        .count()
}

/// What a bar panel drew for the values at its floor, or `null` when it has
/// none: `null` is the statement that no measurement sits at the baseline.
fn zero_bar_document(series: &Series, extent: (f64, f64), markup: &str) -> J {
    let marks = zero_bar_marks(markup);
    let values = floor_value_count(series, extent);
    if marks.is_empty() && values == 0 {
        return J::Null;
    }
    let height = marks
        .iter()
        .map(|mark| mark.box_.3 - mark.box_.1)
        .fold(0.0, f64::max);
    J::Obj(vec![
        ("mark".to_string(), J::Str("hollow-bar".to_string())),
        ("floor".to_string(), J::Float(bar_baseline_value(extent))),
        ("bars".to_string(), J::Int(marks.len() as i64)),
        ("values".to_string(), J::Int(values as i64)),
        ("height_px".to_string(), J::Float(height)),
    ])
}

/// Problems that make the floor marks not what they claim to be.
pub fn check_zero_bar_marks(
    panel_id: &str,
    series: &Series,
    extent: (f64, f64),
    markup: &str,
) -> PlotResult<Vec<String>> {
    let marks = zero_bar_marks(markup);
    let values = floor_value_count(series, extent);
    if marks.is_empty() && values == 0 {
        return Ok(Vec::new());
    }
    let mut problems = Vec::new();
    if marks.len() != values {
        problems.push(format!(
            "panel {}: {} measured value(s) sit at the bar baseline ({}) and the panel \
             draws {} floor mark(s) for them; the count is measured out of the drawn \
             geometry and the data both, so a value at the floor without its mark \
             reads as absent and a mark without its value reads as a claim the run \
             never made",
            pyjson::repr_str(panel_id),
            values,
            fg(bar_baseline_value(extent)),
            marks.len()
        ));
    }
    let (left, top, right, bottom) = panel_plot_rect(panel_id, markup)?;
    if !marks.is_empty() {
        let stated = read_panel_summary(markup)
            .and_then(|summary| summary.get("zero_bar").cloned())
            .filter(J::truthy);
        if stated.is_none() {
            problems.push(format!(
                "panel {}: it draws {} floor mark(s) for values at the bar baseline and \
                 its own summary states none; a mark the panel does not state is a value \
                 the reader has to infer from the pixels, which is the failure the mark \
                 exists to close",
                pyjson::repr_str(panel_id),
                marks.len()
            ));
        }
    }
    for mark in &marks {
        let height = mark.box_.3 - mark.box_.1;
        if mark.fill != "none" {
            problems.push(format!(
                "panel {}: a floor mark is painted {} rather than left hollow; a \
                 filled rect of any height is what a non-zero bar is, so the mark \
                 would be read as a small value instead of as a value at the floor",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(&mark.fill)
            ));
        }
        if mark.stroke.trim().is_empty() {
            problems.push(format!(
                "panel {}: a floor mark is drawn with no stroke, so a hollow fill \
                 paints nothing and the value is invisible",
                pyjson::repr_str(panel_id)
            ));
        }
        if height < ZERO_BAR_MARK_HEIGHT_PX {
            problems.push(format!(
                "panel {}: a floor mark is {} px tall, under the {} px the mark needs \
                 to survive rasterization; a mark too short to see leaves the value \
                 absent, which is the defect the mark exists to close",
                pyjson::repr_str(panel_id),
                f1(height),
                f1(ZERO_BAR_MARK_HEIGHT_PX)
            ));
        }
        if mark.box_.0 < left || mark.box_.2 > right || mark.box_.1 < top || mark.box_.3 > bottom {
            problems.push(format!(
                "panel {}: a floor mark at ({}, {})..({}, {}) is outside the plot area \
                 ({}..{}) x ({}..{}), so it is drawn off the panel the value belongs to",
                pyjson::repr_str(panel_id),
                f1(mark.box_.0),
                f1(mark.box_.1),
                f1(mark.box_.2),
                f1(mark.box_.3),
                f1(left),
                f1(right),
                f1(top),
                f1(bottom)
            ));
        }
    }
    Ok(problems)
}

/// Each drawn `<text>` as `(text, (x0, y0, x1, y1))`.
pub fn drawn_text_boxes(markup: &str) -> Vec<(String, (f64, f64, f64, f64))> {
    let mut boxes = Vec::new();
    for groups in text_element_re().find_all(markup) {
        let attributes = groups[0].clone().unwrap_or_default();
        let content = groups[1].clone().unwrap_or_default();
        let values: Vec<(String, String)> = text_attribute_re()
            .find_all(&attributes)
            .iter()
            .map(|pair| {
                (
                    pair[0].clone().unwrap_or_default(),
                    pair[1].clone().unwrap_or_default(),
                )
            })
            .collect();
        let lookup = |key: &str| {
            values
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.clone())
        };
        let text = pyjson::unescape(&bound_label_title_re().replace_all(&content, ""))
            .trim()
            .to_string();
        let width = draw::label_text_width(&text);
        let x: f64 = lookup("x").unwrap_or_default().parse().unwrap_or(0.0);
        let y: f64 = lookup("y").unwrap_or_default().parse().unwrap_or(0.0);
        let transform = lookup("transform").unwrap_or_default();
        if let Some(rotation) = rotate_re().search(&transform) {
            let groups = pyre_groups(&rotation, 2);
            let centre_x: f64 = groups[0].parse().unwrap_or(0.0);
            let centre_y: f64 = groups[1].parse().unwrap_or(0.0);
            boxes.push((
                text,
                (
                    centre_x - draw::LABEL_ASCENT_PX,
                    centre_y - width / 2.0,
                    centre_x + draw::LABEL_DESCENT_PX,
                    centre_y + width / 2.0,
                ),
            ));
            continue;
        }
        let left = match lookup("text-anchor").as_deref() {
            Some("middle") => x - width / 2.0,
            Some("end") => x - width,
            _ => x,
        };
        boxes.push((
            text,
            (
                left,
                y - draw::LABEL_ASCENT_PX,
                left + width,
                y + draw::LABEL_DESCENT_PX,
            ),
        ));
    }
    boxes
}

/// The label each legend entry draws, in document order.
pub fn legend_text(markup: &str) -> Vec<String> {
    let Some(group) = legend_group_re().search(markup) else {
        return Vec::new();
    };
    let body = group.group(1).unwrap_or_default();
    text_element_re()
        .find_all(&body)
        .iter()
        .map(|groups| {
            let content = groups[1].clone().unwrap_or_default();
            pyjson::unescape(&bound_label_title_re().replace_all(&content, ""))
                .trim()
                .to_string()
        })
        .collect()
}

/// The series labels a panel's legend draws, in draw order.
pub fn legend_series_labels(markup: &str) -> Vec<String> {
    let Some(group) = legend_group_re().search(markup) else {
        return Vec::new();
    };
    let body = group.group(1).unwrap_or_default();
    legend_text_re()
        .find_all(&body)
        .iter()
        .map(|groups| pyjson::unescape(&groups[0].clone().unwrap_or_default()))
        .collect()
}

/// The names the drawn bound labels state as governed or their own-bounded.
pub fn governed_names(markup: &str) -> Vec<String> {
    let mut names = Vec::new();
    for (declared, _, _) in label_boxes(markup) {
        for clause in governance_clause_re().find_iter(&declared) {
            let body = clause.named("body").unwrap_or_default();
            for name in governance_name_re().find_all_whole(&body) {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
    }
    names
}

// -- the gap in the sampling, and the readings a panel states ----------------

/// The drawn series segments of one panel, grouped by the stroke that draws
/// them: the gap check's own attribution, and how it tells two series apart.
type StrokeGroups = BTreeMap<String, Vec<(String, Vec<(f64, f64)>)>>;

/// Problems that let a hole in the sampling read as a climb.
pub fn check_gap_honesty(panel_id: &str, series: &Series, markup: &str) -> Vec<String> {
    let drawn: Series = series
        .iter()
        .filter(|(_, points)| !points.is_empty())
        .map(|(name, points)| (name.clone(), draw::decimate(points)))
        .collect();
    let mut problems = Vec::new();
    let expected_markers: usize = drawn.iter().map(|(_, points)| points.len()).sum();
    let markers = sample_marker_re().find_all(markup).len();
    if markers != expected_markers {
        problems.push(format!(
            "panel {}: it draws {markers} sample marker(s) for {expected_markers} \
             drawn sample(s); without a dot at every sample the series' own \
             discreteness is not on the panel, so a hole in the sampling is drawn \
             as the line's own steepness",
            pyjson::repr_str(panel_id)
        ));
    }
    let counts = drawn_polylines(markup);
    let mut strokes: StrokeGroups = BTreeMap::new();
    for (index, (name, points)) in drawn.iter().enumerate() {
        strokes
            .entry(draw::COLORS[index % draw::COLORS.len()].to_string())
            .or_default()
            .push((name.clone(), points.clone()));
    }
    for (colour, entries) in strokes {
        if entries.len() > 1 {
            let mut sorted: Vec<String> = entries.iter().map(|(name, _)| name.clone()).collect();
            sorted.sort();
            let sorted_py = pyjson::py_list(&sorted);
            problems.push(format!(
                "panel {}: series {sorted_py} are drawn in the same stroke {colour}, \
                 so this panel's segments cannot be attributed to the series they \
                 belong to and its holes cannot be checked",
                pyjson::repr_str(panel_id)
            ));
            continue;
        }
        let (name, points) = &entries[0];
        let holes = draw::series_walls(points);
        let runs: Vec<Vec<(f64, f64)>> = draw::split_at_walls(points, &holes)
            .into_iter()
            .filter(|run| run.len() >= 2)
            .collect();
        let drawn_count = counts.get(&colour).copied().unwrap_or(0);
        if drawn_count == runs.len() {
            continue;
        }
        let where_ = if holes.is_empty() {
            "no hole".to_string()
        } else {
            let largest =
                holes.iter().cloned().fold(
                    holes[0],
                    |best, hole| if hole.3 > best.3 { hole } else { best },
                );
            let biggest = holes.iter().map(|hole| hole.3).fold(0.0, f64::max);
            format!(
                "the {} s hole between {} s and {} s",
                f2(biggest),
                f2(largest.1),
                f2(largest.2)
            )
        };
        problems.push(format!(
            "panel {}: series {} is drawn as {drawn_count} polyline segment(s) \
             where its {} hole(s) require {} ({where_}); a segment is drawn across a \
             hole in the sampling, so a period nobody observed is painted as a \
             near-vertical climb the run never measured",
            pyjson::repr_str(panel_id),
            pyjson::repr_str(name),
            holes.len(),
            runs.len()
        ));
    }
    problems
}

/// Problems that leave a run's own reading off the panel it is about.
pub fn check_readings_stated(
    panel_id: &str,
    series: &Series,
    readings: &[(String, String)],
    markup: &str,
) -> Vec<String> {
    let names: Vec<String> = series.iter().map(|(name, _)| name.clone()).collect();
    let names_py = pyjson::py_list(&names);
    let stated = drawn_readings(markup).join(" ");
    let mut problems = Vec::new();
    for (arm, text) in readings {
        if !names.contains(arm) {
            problems.push(format!(
                "panel {}: the run's reading for arm {} is about no series this \
                 panel draws (its series are {names_py}); a verdict no panel carries \
                 is evidence no reader sees",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(arm)
            ));
        } else if !stated.contains(text.as_str()) {
            problems.push(format!(
                "panel {}: the run read arm {} and the panel does not state it. As \
                 drawn, a hole in the sampling, a peak that returned and a climb cut \
                 off by the window's end are the same shape, so the reader cannot \
                 draw the opposite conclusion from the pixels. Missing: {}",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(arm),
                pyjson::repr_str(text)
            ));
        }
    }
    problems
}

/// The slice of a joined reading band each arm's own sentence occupies.
pub fn stated_spans(text: &str, arms: &[String]) -> BTreeMap<String, String> {
    let mut found: Vec<(String, usize)> = Vec::new();
    for arm in arms {
        for marker in [format!("{arm}: "), format!("{arm} - ")] {
            if let Some(index) = text.find(&marker) {
                found.push((arm.clone(), index));
                break;
            }
        }
    }
    found.sort_by_key(|(_, index)| *index);
    let mut spans = BTreeMap::new();
    for (position, (arm, start)) in found.iter().enumerate() {
        let end = found
            .get(position + 1)
            .map(|(_, index)| *index)
            .unwrap_or(text.len());
        // `start` indexes bytes from `str::find`; the slice is safe because the
        // markers are ASCII and the text is UTF-8.
        spans.insert(arm.clone(), text[*start..end].to_string());
    }
    spans
}

/// Half a unit in the last place a written number carries, plus rounding slack.
pub fn stated_tolerance(text: &str) -> Option<f64> {
    let trimmed = text.trim();
    // The Python pattern is unanchored and its groups are optional, so a value
    // like `12abc` would still answer `12`: the port keeps that behaviour by
    // taking the match from the pattern rather than parsing strictly.
    let found = stated_number_re().find_iter(trimmed).into_iter().next()?;
    let decimals = found.group(3).map(|text| text.len()).unwrap_or(0);
    let exponent: i64 = found
        .group(4)
        .and_then(|text| text.parse().ok())
        .unwrap_or(0);
    Some(10.0_f64.powi((exponent - decimals as i64) as i32) * 0.5 * 1.001 + 1e-9)
}

/// A problem when a written number is not the value the series measures.
pub fn stated_problem(
    panel_id: &str,
    arm: &str,
    what: &str,
    written: &str,
    measured: f64,
) -> Option<String> {
    let Some(tolerance) = stated_tolerance(written) else {
        return Some(format!(
            "panel {}: the reading drawn for arm {} states {what} as {}, which is \
             not a number; the band exists so the reader does not have to interpret \
             the pixels, so a value the reader cannot read is not a reading",
            pyjson::repr_str(panel_id),
            pyjson::repr_str(arm),
            pyjson::repr_str(written)
        ));
    };
    let value: f64 = written.trim().parse().unwrap_or(f64::NAN);
    if (value - measured).abs() <= tolerance {
        return None;
    }
    Some(format!(
        "panel {}: the reading drawn for arm {} states {what} {written}, which the \
         series it is drawn from does not measure: that point is {}. A caption whose \
         numbers come from anywhere but its own series is worse than no caption, \
         because the band is what the reader trusts instead of the pixels",
        pyjson::repr_str(panel_id),
        pyjson::repr_str(arm),
        f6g(measured)
    ))
}

/// Every number a drawn reading states, measured against the drawn series.
pub fn stated_reading_problems(
    panel_id: &str,
    arm: &str,
    points: &[(f64, f64)],
    text: &str,
) -> Vec<String> {
    let xs: Vec<f64> = points.iter().map(|(x, _)| *x).collect();
    let ys: Vec<f64> = points.iter().map(|(_, y)| *y).collect();
    let peak = ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let peak_index = ys
        .iter()
        .rposition(|value| *value == peak)
        .unwrap_or(ys.len() - 1);
    let after = points.len() - 1 - peak_index;
    let wall = draw::gap_wall_seconds(points);
    let holes = draw::series_walls(points);
    let mut problems: Vec<String> = Vec::new();

    let note = |what: &str, written: &str, measured: f64, problems: &mut Vec<String>| {
        if let Some(problem) = stated_problem(panel_id, arm, what, written, measured) {
            problems.push(problem);
        }
    };

    match stated_peak_re().search(text) {
        None => problems.push(format!(
            "panel {}: the reading drawn for arm {} states no maximum, so the band's \
             own claim cannot be read back against the series it is drawn from; a \
             caption whose numbers cannot be checked is the same defect as no \
             caption at all",
            pyjson::repr_str(panel_id),
            pyjson::repr_str(arm)
        )),
        Some(found) => {
            note(
                "its maximum as",
                &found.group(1).unwrap_or_default(),
                peak,
                &mut problems,
            );
            note(
                "where its maximum is as",
                &found.group(2).unwrap_or_default(),
                xs[peak_index],
                &mut problems,
            );
        }
    }
    match stated_after_re().search(text) {
        None => {
            if stated_end_re().search(text).is_none() {
                problems.push(format!(
                    "panel {}: the reading drawn for arm {} states neither what \
                     follows its maximum nor that nothing does, so the one clause \
                     that tells a peak which returned from a climb the window cut \
                     off cannot be read back against the series",
                    pyjson::repr_str(panel_id),
                    pyjson::repr_str(arm)
                ));
            } else if after > 0 {
                problems.push(format!(
                    "panel {}: the reading drawn for arm {} states that nothing \
                     follows its maximum, and the series it is drawn from has {after} \
                     sample(s) after it; the whole point of the clause is to tell a \
                     peak that returned from a climb the window cut off",
                    pyjson::repr_str(panel_id),
                    pyjson::repr_str(arm)
                ));
            }
        }
        Some(found) => {
            let stated_after: i64 = found.group(1).unwrap_or_default().parse().unwrap_or(-1);
            if stated_after != after as i64 {
                problems.push(format!(
                    "panel {}: the reading drawn for arm {} states {stated_after} \
                     sample(s) after its maximum, and the series it is drawn from has \
                     {after}: a reader told how long a peak lasted is told a number \
                     about a different series",
                    pyjson::repr_str(panel_id),
                    pyjson::repr_str(arm)
                ));
            }
            if after > 0 {
                note(
                    "the sample after its maximum as",
                    &found.group(2).unwrap_or_default(),
                    ys[peak_index + 1],
                    &mut problems,
                );
                note(
                    "that sample's time as",
                    &found.group(3).unwrap_or_default(),
                    xs[peak_index + 1],
                    &mut problems,
                );
            }
            note(
                "its last sample as",
                &found.group(4).unwrap_or_default(),
                ys[ys.len() - 1],
                &mut problems,
            );
            note(
                "its last sample's time as",
                &found.group(5).unwrap_or_default(),
                xs[xs.len() - 1],
                &mut problems,
            );
        }
    }
    let holes_match = stated_holes_re().search(text);
    if holes_match.is_none()
        && stated_gap_wall_re().search(text).is_none()
        && stated_no_gap_re().search(text).is_none()
    {
        problems.push(format!(
            "panel {}: the reading drawn for arm {} states nothing about the holes \
             in its own sampling, so whether the line is drawn across a period \
             nobody observed cannot be checked from the caption; the holes are the \
             one thing about the shape the pixels cannot carry",
            pyjson::repr_str(panel_id),
            pyjson::repr_str(arm)
        ));
    }
    if let Some(found) = holes_match {
        let stated_holes: i64 = found.group(1).unwrap_or_default().parse().unwrap_or(-1);
        if stated_holes != holes.len() as i64 {
            problems.push(format!(
                "panel {}: the reading drawn for arm {} states {stated_holes} sample \
                 gap(s), and the series it is drawn from has {}: a hole the reader is \
                 not told about is a hole read as the end of the line",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(arm),
                holes.len()
            ));
        }
        if let Some(wall) = wall {
            note(
                "the least step it would call a gap as",
                &found.group(2).unwrap_or_default(),
                wall,
                &mut problems,
            );
        }
        if !holes.is_empty() {
            let largest =
                holes.iter().cloned().fold(
                    holes[0],
                    |best, hole| if hole.3 > best.3 { hole } else { best },
                );
            note(
                "its largest gap as",
                &found.group(3).unwrap_or_default(),
                largest.3,
                &mut problems,
            );
            note(
                "where that gap starts as",
                &found.group(4).unwrap_or_default(),
                largest.1,
                &mut problems,
            );
            note(
                "where that gap ends as",
                &found.group(5).unwrap_or_default(),
                largest.2,
                &mut problems,
            );
        }
    } else if let Some(found) = stated_gap_wall_re().search(text)
        && let Some(wall) = wall
    {
        note(
            "the least step it would call a gap as",
            &found.group(1).unwrap_or_default(),
            wall,
            &mut problems,
        );
    }
    problems
}

/// Problems that let a drawn caption state numbers its own series did not measure.
pub fn check_reading_numbers(panel_id: &str, series: &Series, markup: &str) -> Vec<String> {
    let joined = drawn_readings(markup).join(" ");
    if joined.is_empty() {
        return Vec::new();
    }
    let arms: Vec<String> = series.iter().map(|(name, _)| name.clone()).collect();
    let spans = stated_spans(&joined, &arms);
    let mut problems = Vec::new();
    for (name, points) in series {
        let Some(text) = spans.get(name) else {
            continue;
        };
        let drawn = draw::decimate(points);
        if drawn.is_empty() {
            continue;
        }
        problems.extend(stated_reading_problems(panel_id, name, &drawn, text));
    }
    problems
}

/// Problems that make an axis unreadable: its ticks repeat a value.
pub fn check_tick_labels_distinct(panel_id: &str, markup: &str) -> Vec<String> {
    let ticks = axis_tick_labels(markup);
    let mut repeated: Vec<String> = ticks
        .iter()
        .filter(|tick| ticks.iter().filter(|other| *other == *tick).count() > 1)
        .cloned()
        .collect();
    repeated.sort();
    repeated.dedup();
    if repeated.is_empty() {
        return Vec::new();
    }
    let ticks_py = pyjson::py_list(&ticks);
    let repeated_py = pyjson::py_list(&repeated);
    vec![format!(
        "panel {}: its y axis draws {} tick(s) as {ticks_py}, so {} of them repeat \
         ({repeated_py}); a tick the reader cannot tell from its neighbour cannot \
         carry the quantity the panel is drawn to be read against",
        pyjson::repr_str(panel_id),
        ticks.len(),
        repeated.len()
    )]
}

/// Problems that make the reading band eat the shape it explains.
pub fn check_reading_band(panel_id: &str, plot_height: f64) -> Vec<String> {
    if plot_height >= ARM_READING_MIN_PLOT_PIXELS {
        return Vec::new();
    }
    vec![format!(
        "panel {}: the per-arm reading band leaves the plot {} px of the {} px \
         canvas, under the {} px a latency panel needs to show the shape its \
         readings are about; state fewer or shorter readings",
        pyjson::repr_str(panel_id),
        f0(plot_height),
        draw::HEIGHT,
        f0(ARM_READING_MIN_PLOT_PIXELS)
    )]
}

/// Every arm the run read must be a series some line panel actually draws.
pub fn check_censoring_drawn(
    panels: &[Panel],
    points: &Points,
    censoring: Option<&J>,
) -> Vec<String> {
    let mut line_series: Vec<String> = Vec::new();
    for panel in panels {
        if panel.chart != Chart::Line {
            continue;
        }
        for entry in &panel.series {
            if points.contains_key(&(panel.id.clone(), entry.name.clone()))
                && !line_series.contains(&entry.name)
            {
                line_series.push(entry.name.clone());
            }
        }
    }
    let Some(censoring) = censoring.and_then(J::as_obj) else {
        return Vec::new();
    };
    let mut arms: Vec<&String> = censoring.iter().map(|(arm, _)| arm).collect();
    arms.sort();
    let mut sorted_line = line_series.clone();
    sorted_line.sort();
    let sorted_line_py = pyjson::py_list(&sorted_line);
    arms.iter()
        .filter(|arm| !line_series.contains(arm))
        .map(|arm| {
            format!(
                "the run's per-arm reading for {} is about no series any line panel \
                 of this mandate draws ({sorted_line_py}), so no panel can state it: a \
                 machine verdict no panel carries is evidence a reader never sees",
                pyjson::repr_str(arm)
            )
        })
        .collect()
}

// -- the line panel's clipped axis -------------------------------------------

/// The value a line panel's axis must be clipped at, or `None`.
pub fn line_clip_owed(series: &Series, bounds: &[Bound]) -> Option<f64> {
    if bounds.is_empty() {
        return None;
    }
    let values = bound_values(series);
    if values.is_empty() {
        return None;
    }
    let anchor = bounds
        .iter()
        .map(|bound| bound.y)
        .fold(f64::NEG_INFINITY, f64::max);
    if !(anchor > 0.0) {
        return None;
    }
    if values.iter().cloned().fold(f64::NEG_INFINITY, f64::max) < Y_CLIP_FACTOR * anchor {
        return None;
    }
    let above: Vec<f64> = values
        .iter()
        .copied()
        .filter(|value| *value > anchor)
        .collect();
    if above.is_empty() || above.len() as f64 > Y_CLIP_SHARE * values.len() as f64 {
        return None;
    }
    Some(anchor)
}

/// The value a line panel's drawn axis clips at, or `None`.
pub fn line_axis_clip(
    series: &Series,
    bounds: &[Bound],
    pinned: Option<(f64, f64)>,
) -> Option<f64> {
    if pinned.is_some() {
        return None;
    }
    line_clip_owed(series, bounds)
}

/// The clip a line axis with this clip value carries, as the summary states it.
pub fn y_clip_document_for(series: &Series, clip: Option<f64>) -> Option<J> {
    let clip = clip?;
    let values = bound_values(series);
    let above: Vec<f64> = values
        .iter()
        .copied()
        .filter(|value| *value > clip)
        .collect();
    if above.is_empty() {
        return None;
    }
    Some(J::Obj(vec![
        ("value".to_string(), J::Float(clip)),
        ("clipped".to_string(), J::Int(above.len() as i64)),
        (
            "max".to_string(),
            J::Float(above.iter().cloned().fold(f64::NEG_INFINITY, f64::max)),
        ),
    ]))
}

/// The clip the *drawn* line axis applies, or `None`.
pub fn y_clip_document(series: &Series, bounds: &[Bound], extent: (f64, f64)) -> Option<J> {
    let values = bound_values(series);
    if values.is_empty() || extent.1 >= values.iter().cloned().fold(f64::NEG_INFINITY, f64::max) {
        return None;
    }
    let clip = line_clip_owed(series, bounds);
    y_clip_document_for(series, clip)
}

/// The sentence a clipped axis owes its reader, or `""`.
pub fn y_clip_statement(series: &Series, clip: Option<f64>) -> String {
    let Some(document) = y_clip_document_for(series, clip) else {
        return String::new();
    };
    let value = document.get("value").and_then(J::as_f64).unwrap_or(0.0);
    let clipped = document.get("clipped").and_then(J::as_i64).unwrap_or(0);
    let max = document.get("max").and_then(J::as_f64).unwrap_or(0.0);
    format!(
        "y axis clipped at {}: {clipped} of {} value(s) up to {} drawn at the top edge",
        sliver_number(value),
        bound_values(series).len(),
        sliver_number(max)
    )
}

/// Problems that let one outlier set a line panel's axis, or hide the clip.
pub fn check_line_axis_clip_stated(
    panel_id: &str,
    chart: Chart,
    series: &Series,
    bounds: &[Bound],
    extent: (f64, f64),
    markup: &str,
    pinned: Option<(f64, f64)>,
) -> Vec<String> {
    if chart != Chart::Line {
        return Vec::new();
    }
    let Some(owed) = line_clip_owed(series, bounds) else {
        return Vec::new();
    };
    if pinned.is_some() {
        return Vec::new();
    }
    let values = bound_values(series);
    let peak = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if extent.1 >= peak {
        let above = values.iter().filter(|value| **value > owed).count();
        return vec![format!(
            "panel {}: its axis tops out at {}, which reaches the {} the run drew, \
             while {above} of {} value(s) lie above the {} the panel is read against: \
             one outlier set the axis, so every body is drawn to its scale and a \
             change in either is sub-pixel. Draw the axis to the read-at value and \
             state where it clips",
            pyjson::repr_str(panel_id),
            f4g(extent.1),
            fg(peak),
            values.len(),
            fg(owed)
        )];
    }
    let mut problems = Vec::new();
    if !y_clip_line_re().is_match(markup) {
        problems.push(format!(
            "panel {}: its axis is clipped at {} and draws no class=\"y-clip\" mark \
             at the frame top, so the clipped samples are drawn at a pixel the panel \
             does not explain",
            pyjson::repr_str(panel_id),
            fg(owed)
        ));
    }
    let statement = y_clip_statement(series, Some(owed));
    if !statement.is_empty() && !drawn_notes(markup).join(" ").contains(&statement) {
        problems.push(format!(
            "panel {}: its axis is clipped at {} and the panel does not state it, so \
             a sample drawn at the frame's top edge reads as a value at the axis' own \
             top rather than as an excursion past it. Missing from the panel's face: {}",
            pyjson::repr_str(panel_id),
            fg(owed),
            pyjson::repr_str(&statement)
        ));
    }
    problems
}

/// The y extent of a line or CDF panel, as the report's own chart lays it out.
pub fn line_axis_extent(
    series: &Series,
    bounds: &[Bound],
    pinned: Option<(f64, f64)>,
    plot_height: Option<f64>,
) -> (f64, f64) {
    if let Some(pinned) = pinned {
        return pinned;
    }
    let labelled: Vec<(f64, String)> = bounds
        .iter()
        .map(|bound| (bound.y, String::new()))
        .collect();
    let extent = draw::extent_including_bounds(draw::finite_extent(series), &labelled);
    let clip = line_axis_clip(series, bounds, None);
    let Some(clip) = clip else {
        return extent;
    };
    if !(clip > extent.0) {
        return extent;
    }
    let plot_height =
        plot_height.unwrap_or((draw::HEIGHT - draw::PAD_TOP - draw::PAD_BOTTOM) as f64);
    axis_with_headroom(extent.0, clip, clip, plot_height, None)
}

/// The axis a panel is drawn on, so the checks measure the drawn axis.
pub fn panel_axis_extent(
    panel: &Panel,
    series: &Series,
    bounds: &[Bound],
    run_values: Option<&J>,
    plot_height: f64,
) -> PlotResult<(f64, f64)> {
    match panel.chart {
        Chart::Cdf => Ok((0.0, 100.0)),
        Chart::Bar => {
            if let Some(pinned) = &panel.y_extent {
                return Ok(*pinned);
            }
            Ok(draw::bar_axis_extent(
                series,
                bounds,
                run_values,
                Some(plot_height as i64),
            ))
        }
        Chart::Line => Ok(line_axis_extent(
            series,
            bounds,
            panel.y_extent,
            Some(plot_height),
        )),
    }
}

/// The x extent a panel draws, in its own units, as `(low, high)`.
pub fn panel_x_axis_extent(chart: Chart, series: &Series) -> (f64, f64) {
    if chart == Chart::Bar {
        let mut categories: Vec<f64> = series
            .iter()
            .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
            .collect();
        categories.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        categories.dedup();
        if categories.is_empty() {
            return (0.0, 1.0);
        }
        return draw::bar_x_extent(&categories);
    }
    let xs: Vec<f64> = series
        .iter()
        .flat_map(|(_, points)| {
            draw::decimate(points)
                .into_iter()
                .map(|(x, _)| x)
                .collect::<Vec<f64>>()
        })
        .collect();
    if xs.is_empty() {
        return (0.0, 1.0);
    }
    let low = xs.iter().cloned().fold(f64::INFINITY, f64::min);
    let mut high = xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if low == high {
        high = low + 1.0;
    }
    (low, high)
}

// -- the axis test, and the statement that answers it -------------------------

/// The band the axis test measures for one bound, and the pixels it has.
pub fn bound_band_pixels(
    series: &Series,
    bounds: &[Bound],
    bound: &Bound,
    extent: (f64, f64),
    plot_height: f64,
    run_values: Option<&J>,
) -> (f64, f64) {
    let values = bound_values(series);
    let ys: Vec<f64> = bounds.iter().map(|item| item.y).collect();
    let unit = unit_span(&values, &ys);
    let tolerances: Vec<f64> = run_guards(run_values, Some(series), &bound.label)
        .iter()
        .map(|(_, value)| *value)
        .collect();
    let band = bound_band(&values, bound.y, unit, &tolerances);
    let span = extent.1 - extent.0;
    (band, band / span * plot_height)
}

/// One number as the sliver statement writes it and the check reads it back.
pub fn sliver_number(value: f64) -> String {
    fg(value)
}

/// `(pixels, value)`: the most legible departure the bars draw from a bound.
pub fn sliver_departure(
    values: &[f64],
    bound: &Bound,
    extent: (f64, f64),
    plot_height: f64,
) -> (f64, Option<f64>) {
    let y = bound.y;
    let band_arm = bound.band_arm.is_some() || two_sided_bound(bound);
    let side = if band_arm {
        None
    } else {
        bound_side(values, y)
    };
    let candidates: Vec<f64> = match side {
        Some("cap") => values.iter().copied().filter(|value| *value > y).collect(),
        Some("floor") => values.iter().copied().filter(|value| *value < y).collect(),
        _ => values.to_vec(),
    };
    if candidates.is_empty() {
        return (0.0, None);
    }
    let furthest = candidates
        .iter()
        .cloned()
        .fold(candidates[0], |best, value| {
            if (value - y).abs() > (best - y).abs() {
                value
            } else {
                best
            }
        });
    let span = extent.1 - extent.0;
    ((furthest - y).abs() / span * plot_height, Some(furthest))
}

/// The sentence that states a sub-pixel bound's position, or `""`.
pub fn bound_sliver_statement(
    bound: &Bound,
    values: &[f64],
    band: f64,
    extent: (f64, f64),
    plot_height: f64,
) -> String {
    if values.is_empty() {
        return String::new();
    }
    let (low, high) = extent;
    let span = high - low;
    let y = bound.y;
    if band / span * plot_height >= MIN_BOUND_PIXELS {
        return String::new();
    }
    let (departure_px, departure) = sliver_departure(values, bound, extent, plot_height);
    let Some(departure) = departure else {
        return String::new();
    };
    if departure_px < MIN_BOUND_PIXELS {
        return String::new();
    }
    let nearest = values.iter().cloned().fold(values[0], |best, value| {
        if (value - y).abs() < (best - y).abs() {
            value
        } else {
            best
        }
    });
    format!(
        "bound \"{}\" at {} on axis {}..{}: band {} = {} px; nearest bar {}, {} \
         away; furthest {}, {} px from the bound",
        bound.label,
        sliver_number(y),
        sliver_number(low),
        sliver_number(high),
        sliver_number(band),
        f1(band / span * plot_height),
        sliver_number(nearest),
        sliver_number((nearest - y).abs()),
        sliver_number(departure),
        f1(departure_px)
    )
}

/// `(bound, sentence)` for every bound whose sliver the panel owes stated.
// The loop walks indices because it names one bound while mutating another in
// the same slice (`unlabelled`); an iterator would borrow the slice twice.
#[allow(clippy::needless_range_loop)]
pub fn sliver_bound_statements(
    series: &Series,
    bounds: &mut [Bound],
    extent: (f64, f64),
    plot_height: f64,
    run_values: Option<&J>,
) -> Vec<(usize, String)> {
    let values = bound_values(series);
    let span = extent.1 - extent.0;
    let mut statements = Vec::new();
    for index in 0..bounds.len() {
        let y = bounds[index].y;
        if bound_side(&values, y).is_none() && crossing_values(&values, y).is_empty() {
            continue;
        }
        let band = bound_band_pixels(
            series,
            bounds,
            &bounds[index],
            extent,
            plot_height,
            run_values,
        )
        .0;
        let mut sentence =
            bound_sliver_statement(&bounds[index], &values, band, extent, plot_height);
        if sentence.is_empty() {
            continue;
        }
        for other in 0..bounds.len() {
            if other == index || bounds[other].band_arm.is_none() {
                continue;
            }
            let gap = (bounds[other].y - y).abs() / span * plot_height;
            if gap >= draw::LABEL_LINE_HEIGHT_PX {
                continue;
            }
            bounds[other].unlabelled = true;
            sentence += &format!(
                "; its arm at {} ({} px away) is drawn unlabelled",
                sliver_number(bounds[other].y),
                f1(gap)
            );
        }
        statements.push((index, sentence));
    }
    statements
}

/// Problems that make a drawn sliver statement's numbers not the run's.
pub fn sliver_statement_problems(
    panel_id: &str,
    bound: &Bound,
    matches: &[crate::tools::pyre::Match],
    values: &[f64],
    band: f64,
    extent: (f64, f64),
    plot_height: f64,
) -> Vec<String> {
    let (low, high) = extent;
    let span = high - low;
    let y = bound.y;
    let nearest = values.iter().cloned().fold(values[0], |best, value| {
        if (value - y).abs() < (best - y).abs() {
            value
        } else {
            best
        }
    });
    let (departure_px, departure) = sliver_departure(values, bound, extent, plot_height);
    let measured: Vec<(&str, f64)> = vec![
        ("value", y),
        ("low", low),
        ("high", high),
        ("band", band),
        ("band_px", band / span * plot_height),
        ("near", nearest),
        ("near_dist", (nearest - y).abs()),
        ("far", departure.unwrap_or(0.0)),
        ("far_px", departure_px),
    ];
    let mut problems = Vec::new();
    for found in matches {
        for (field, expected) in &measured {
            let Some(printed) = found.named(field).and_then(|text| text.parse::<f64>().ok()) else {
                continue;
            };
            let slack = if field.ends_with("_px") {
                0.05
            } else {
                (1e-4 * expected.abs()).max(1e-9)
            };
            if (printed - expected).abs() <= slack {
                continue;
            }
            problems.push(format!(
                "panel {}: the statement for the bound {} prints {field}={}, where \
                 the drawn points and the drawn axis {}..{} measure {}; a stated \
                 distance that is not the run's is a claim the reader has no way to \
                 check",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(&bound.label),
                fg(printed),
                f4g(low),
                f4g(high),
                fg(*expected)
            ));
        }
    }
    problems
}

/// Problems that leave a sub-pixel bound's own position unstated.
pub fn check_sliver_bound_stated(
    panel_id: &str,
    series: &Series,
    bounds: &[Bound],
    extent: (f64, f64),
    markup: &str,
    plot_height: Option<f64>,
    run_values: Option<&J>,
) -> Vec<String> {
    let values = bound_values(series);
    if values.is_empty() {
        return Vec::new();
    }
    let (low, high) = extent;
    let span = high - low;
    if !(span > 0.0) {
        let low_py = frepr(low);
        let high_py = frepr(high);
        return vec![format!(
            "panel {}: the axis {low_py}..{high_py} has no span",
            pyjson::repr_str(panel_id)
        )];
    }
    let plot_height =
        plot_height.unwrap_or((draw::HEIGHT - draw::PAD_TOP - draw::PAD_BOTTOM) as f64);
    let notes = drawn_notes(markup).join(" ");
    let mut stated: BTreeMap<String, Vec<crate::tools::pyre::Match>> = BTreeMap::new();
    for found in sliver_statement_re().find_iter(&notes) {
        stated
            .entry(found.named("label").unwrap_or_default())
            .or_default()
            .push(found);
    }
    let drawn_labels: Vec<String> = label_boxes(markup)
        .into_iter()
        .map(|(declared, _, _)| declared)
        .collect();
    let mut problems = Vec::new();
    for bound in bounds {
        let y = bound.y;
        if bound_side(&values, y).is_none() && crossing_values(&values, y).is_empty() {
            continue;
        }
        let (band, pixels) =
            bound_band_pixels(series, bounds, bound, extent, plot_height, run_values);
        if pixels >= MIN_BOUND_PIXELS {
            continue;
        }
        let matches = stated.get(&bound.label).cloned().unwrap_or_default();
        if matches.is_empty() {
            problems.push(format!(
                "panel {}: the bound {} (y={}) has a band of {} ({} px of {} on the \
                 axis {}..{}), under the {} px it needs to show the departure it \
                 exists to catch, and the panel states nothing about where the bound \
                 sits: a reader can see a departure and cannot read it against the \
                 bound, which is the same panel as one drawn silently",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(&bound.label),
                fg(y),
                f4g(band),
                f1(pixels),
                f0(plot_height),
                f4g(low),
                f4g(high),
                f0(MIN_BOUND_PIXELS)
            ));
            continue;
        }
        problems.extend(sliver_statement_problems(
            panel_id,
            bound,
            &matches,
            &values,
            band,
            extent,
            plot_height,
        ));
        for other in bounds {
            if std::ptr::eq(other, bound) || other.band_arm.is_none() {
                continue;
            }
            let gap = (other.y - y).abs() / (high - low) * plot_height;
            if gap >= draw::LABEL_LINE_HEIGHT_PX {
                continue;
            }
            if drawn_labels.contains(&other.label) {
                continue;
            }
            let satisfied = matches.iter().any(|found| {
                found
                    .named("arm")
                    .and_then(|text| text.parse::<f64>().ok())
                    .is_some_and(|arm| (arm - other.y).abs() <= 1e-9)
                    && found
                        .named("arm_px")
                        .and_then(|text| text.parse::<f64>().ok())
                        .is_some_and(|pixels| (pixels - gap).abs() <= 0.05)
            });
            if satisfied {
                continue;
            }
            problems.push(format!(
                "panel {}: the band arm at {} is drawn {} px from the stated arm, too \
                 close for a label of its own, and the panel draws no label for it and \
                 states nothing about where it is: an arm neither labelled nor stated \
                 is a line the reader cannot read a bar against",
                pyjson::repr_str(panel_id),
                fg(other.y),
                f1(gap)
            ));
        }
    }
    problems
}

/// Problems that make an axis unable to show a bound drawn on it.
pub fn check_panel_axis(
    panel_id: &str,
    series: &Series,
    bounds: &[Bound],
    extent: (f64, f64),
    plot_height: Option<f64>,
    run_values: Option<&J>,
    stated: &str,
) -> Vec<String> {
    let mut problems = Vec::new();
    let (low, high) = extent;
    let span = high - low;
    if !(span > 0.0) {
        let low_py = frepr(low);
        let high_py = frepr(high);
        return vec![format!(
            "panel {}: the axis {low_py}..{high_py} has no span",
            pyjson::repr_str(panel_id)
        )];
    }
    let plot_height =
        plot_height.unwrap_or((draw::HEIGHT - draw::PAD_TOP - draw::PAD_BOTTOM) as f64);
    let values = bound_values(series);
    for bound in bounds {
        let y = bound.y;
        if bound_side(&values, y).is_none() && crossing_values(&values, y).is_empty() {
            continue;
        }
        let (band, pixels) =
            bound_band_pixels(series, bounds, bound, extent, plot_height, run_values);
        if pixels >= MIN_BOUND_PIXELS {
            continue;
        }
        if !stated.is_empty() && stated.contains(&bound.label) {
            continue;
        }
        let nearest = values
            .iter()
            .map(|value| (value - y).abs())
            .fold(f64::INFINITY, f64::min);
        let nearest = if values.is_empty() { 0.0 } else { nearest };
        problems.push(format!(
            "panel {}: the axis {}..{} leaves the bound {} (y={}) a band of {} ({} of \
             its height, {} px of {}), under the {} px a bound needs to show the \
             departure it exists to catch, so that departure would be sub-pixel. The \
             panel may draw the bound and state its position with the measured \
             distance instead (this run's nearest bar is {} from it, {} px of this \
             axis); it states nothing, so the panel is refused rather than drawn",
            pyjson::repr_str(panel_id),
            f4g(low),
            f4g(high),
            pyjson::repr_str(&bound.label),
            fg(y),
            f4g(band),
            pct1(band / span),
            f1(pixels),
            f0(plot_height),
            f0(MIN_BOUND_PIXELS),
            f4g(nearest),
            f1(nearest / span * plot_height)
        ));
    }
    problems
}

// -- the panel summary --------------------------------------------------------

/// The summary a panel carries: what it drew, with the drawn coordinates.
#[allow(clippy::too_many_arguments)]
pub fn panel_summary_document(
    panel_id: &str,
    chart: Chart,
    x_label: &str,
    y_label: &str,
    series: &Series,
    bounds: &[Bound],
    extent: (f64, f64),
    markup: &str,
    stated_labels: &str,
    plot_height: f64,
    run_values: Option<&J>,
    fault: Option<&str>,
) -> J {
    let drawn_pixels = drawn_bound_re()
        .find_all(markup)
        .iter()
        .map(|groups| {
            groups[0]
                .clone()
                .unwrap_or_default()
                .parse::<f64>()
                .unwrap_or(0.0)
        })
        .collect::<Vec<f64>>();
    let drawn_labels: Vec<String> = label_boxes(markup)
        .into_iter()
        .map(|(declared, _, _)| declared)
        .collect();
    let mut entries = Vec::new();
    for (index, bound) in bounds.iter().enumerate() {
        let label = bound.label.clone();
        let has_label = drawn_labels
            .iter()
            .any(|declared| declared.starts_with(&label));
        let state = if stated_labels.contains(&label) {
            BOUND_DRAWN_STATED_SLIVER
        } else if bound.unlabelled || !has_label {
            BOUND_DRAWN_UNLABELLED
        } else {
            BOUND_DRAWN_LABELLED
        };
        let band_pixels = if chart == Chart::Bar {
            Some(bound_band_pixels(series, bounds, bound, extent, plot_height, run_values).1)
        } else {
            None
        };
        entries.push(J::Obj(vec![
            ("label".to_string(), J::Str(label)),
            ("value".to_string(), J::Float(bound.y)),
            (
                "px".to_string(),
                match drawn_pixels.get(index) {
                    Some(value) => J::Float(*value),
                    None => J::Null,
                },
            ),
            (
                "band_px".to_string(),
                match band_pixels {
                    Some(value) => J::Float(value),
                    None => J::Null,
                },
            ),
            (
                "reason".to_string(),
                J::Str(bound_reason(bound).to_string()),
            ),
            ("drawn".to_string(), J::Str(state.to_string())),
        ]));
    }
    let (x_low, x_high) = panel_x_axis_extent(chart, series);
    let series_entries: Vec<J> = series
        .iter()
        .map(|(name, points)| {
            let scores: Vec<f64> = points.iter().map(|(_, value)| *value).collect();
            J::Obj(vec![
                ("name".to_string(), J::Str(drawn_series_name(chart, name))),
                ("points".to_string(), J::Int(points.len() as i64)),
                (
                    "min".to_string(),
                    match scores.iter().cloned().fold(None::<f64>, |best, value| {
                        Some(match best {
                            Some(best) => best.min(value),
                            None => value,
                        })
                    }) {
                        Some(value) => J::Float(value),
                        None => J::Null,
                    },
                ),
                (
                    "max".to_string(),
                    match scores.iter().cloned().fold(None::<f64>, |best, value| {
                        Some(match best {
                            Some(best) => best.max(value),
                            None => value,
                        })
                    }) {
                        Some(value) => J::Float(value),
                        None => J::Null,
                    },
                ),
            ])
        })
        .collect();
    J::Obj(vec![
        ("panel".to_string(), J::Str(panel_id.to_string())),
        ("chart".to_string(), J::Str(chart.name().to_string())),
        (
            "axis".to_string(),
            J::Arr(vec![J::Float(extent.0), J::Float(extent.1)]),
        ),
        (
            "x_axis".to_string(),
            J::Arr(vec![J::Float(x_low), J::Float(x_high)]),
        ),
        (
            "y_clip".to_string(),
            y_clip_document(series, bounds, extent).unwrap_or(J::Null),
        ),
        ("x_label".to_string(), J::Str(x_label.to_string())),
        ("y_label".to_string(), J::Str(y_label.to_string())),
        ("series".to_string(), J::Arr(series_entries)),
        (
            "zero_bar".to_string(),
            if chart == Chart::Bar {
                zero_bar_document(series, extent, markup)
            } else {
                J::Null
            },
        ),
        ("bounds".to_string(), J::Arr(entries)),
        (
            "reading".to_string(),
            J::Str(panel_reading(chart, series, bounds, extent, plot_height)),
        ),
        (
            "fault".to_string(),
            match fault {
                Some(value) => J::Str(value.to_string()),
                None => J::Null,
            },
        ),
    ])
}

/// The summary as the compact human-readable block every reader sees.
pub fn panel_summary_block(document: &J) -> String {
    let axis = document.get("axis").and_then(J::as_arr).unwrap_or(&[]);
    let x_axis = document.get("x_axis").and_then(J::as_arr);
    let mut line = format!(
        "panel {}  chart={}  axis={}..{}  ",
        document.get("panel").and_then(J::as_str).unwrap_or(""),
        document.get("chart").and_then(J::as_str).unwrap_or(""),
        sliver_number(axis.first().and_then(J::as_f64).unwrap_or(0.0)),
        sliver_number(axis.get(1).and_then(J::as_f64).unwrap_or(0.0))
    );
    if let Some(x_axis) = x_axis
        && x_axis.len() >= 2
    {
        line.push_str(&format!(
            "x_axis={}..{}  ",
            sliver_number(x_axis[0].as_f64().unwrap_or(0.0)),
            sliver_number(x_axis[1].as_f64().unwrap_or(0.0))
        ));
    }
    line.push_str(&format!(
        "x={}  y={}",
        document.get("x_label").and_then(J::as_str).unwrap_or(""),
        document.get("y_label").and_then(J::as_str).unwrap_or("")
    ));
    let mut lines = vec![line];
    if let Some(clip) = document.get("y_clip")
        && clip.truthy()
    {
        lines.push(format!(
            "  y_clip: clipped at {} \u{2014} {} drawn value(s) up to {} are drawn \
                 at the frame's top edge",
            sliver_number(clip.get("value").and_then(J::as_f64).unwrap_or(0.0)),
            clip.get("clipped").and_then(J::as_i64).unwrap_or(0),
            sliver_number(clip.get("max").and_then(J::as_f64).unwrap_or(0.0))
        ));
    }
    let series = document.get("series").and_then(J::as_arr).unwrap_or(&[]);
    if !series.is_empty() {
        let listed: Vec<String> = series
            .iter()
            .map(|entry| {
                format!(
                    "{} {} pts {}..{}",
                    entry.get("name").and_then(J::as_str).unwrap_or(""),
                    entry.get("points").and_then(J::as_i64).unwrap_or(0),
                    sliver_number(entry.get("min").and_then(J::as_f64).unwrap_or(0.0)),
                    sliver_number(entry.get("max").and_then(J::as_f64).unwrap_or(0.0))
                )
            })
            .collect();
        lines.push(format!("  series: {}", listed.join("; ")));
    }
    if let Some(zero_bar) = document.get("zero_bar")
        && zero_bar.truthy()
    {
        lines.push(format!(
            "  zero_bar: {} bar(s) drawn as a {} outline {} px tall at the bar \
             baseline ({}); {} measured value(s) sit on that baseline",
            zero_bar.get("bars").and_then(J::as_i64).unwrap_or(0),
            zero_bar.get("mark").and_then(J::as_str).unwrap_or(""),
            f1(zero_bar.get("height_px").and_then(J::as_f64).unwrap_or(0.0)),
            sliver_number(zero_bar.get("floor").and_then(J::as_f64).unwrap_or(0.0)),
            zero_bar.get("values").and_then(J::as_i64).unwrap_or(0)
        ));
    }
    let bounds = document.get("bounds").and_then(J::as_arr).unwrap_or(&[]);
    if bounds.is_empty() {
        lines.push("  bound: none by design".to_string());
    } else {
        for entry in bounds {
            let pixel = match entry.get("px") {
                Some(J::Float(value)) => f1(*value),
                _ => "unplaced".to_string(),
            };
            let band = match entry.get("band_px").and_then(J::as_f64) {
                Some(value) => format!("{}px", f1(value)),
                None => "n/a".to_string(),
            };
            lines.push(format!(
                "  bound: {} y={} px={pixel} band={band} reason={} drawn={}",
                pyjson::repr_str(entry.get("label").and_then(J::as_str).unwrap_or("")),
                sliver_number(entry.get("value").and_then(J::as_f64).unwrap_or(0.0)),
                entry.get("reason").and_then(J::as_str).unwrap_or(""),
                entry.get("drawn").and_then(J::as_str).unwrap_or("")
            ));
        }
    }
    lines.push(format!(
        "  reading: {}",
        document.get("reading").and_then(J::as_str).unwrap_or("")
    ));
    if let Some(fault) = document.get("fault").and_then(J::as_str) {
        lines.push(format!(
            "  fault: {fault} \u{2014} a deliberate input fault on this mandate's arm; \
             the arm it perturbs failed as intended, so this is a fault render and not \
             a refused panel"
        ));
        let slivers: Vec<&J> = bounds
            .iter()
            .filter(|entry| {
                entry.get("drawn").and_then(J::as_str) == Some(BOUND_DRAWN_STATED_SLIVER)
                    && entry
                        .get("band_px")
                        .map(|value| value.as_f64().is_some())
                        .unwrap_or(false)
            })
            .collect();
        if !slivers.is_empty() {
            let listed: Vec<String> = slivers
                .iter()
                .map(|entry| {
                    format!(
                        "{} is {} px of this axis and stated at px={}",
                        pyjson::repr_str(entry.get("label").and_then(J::as_str).unwrap_or("")),
                        f1(entry.get("band_px").and_then(J::as_f64).unwrap_or(0.0)),
                        f1(entry.get("px").and_then(J::as_f64).unwrap_or(0.0))
                    )
                })
                .collect();
            lines.push(format!("  fault scale: {}", listed.join("; ")));
        }
    }
    lines.join("\n")
}

/// Insert a panel's summary as a machine-readable `<desc>` in its SVG.
pub fn introduce_panel_summary(markup: &str, document: &J) -> String {
    let body = pyjson::escape(&pyjson::dumps(document, true, None));
    let desc = format!("<desc class=\"panel-summary\">{body}</desc>");
    let Some(found) = svg_opening_re().search(markup) else {
        return markup.to_string();
    };
    let chars: Vec<char> = markup.chars().collect();
    let mut out: String = chars[..found.end].iter().collect();
    out.push_str(&desc);
    out.extend(chars[found.end..].iter());
    out
}

/// The summary a written panel carries, or `None` when it carries none.
pub fn read_panel_summary(markup: &str) -> Option<J> {
    let found = panel_summary_re().search(markup)?;
    let body = pyjson::unescape(&found.named("body").unwrap_or_default());
    let document = pyjson::parse(&body).ok()?;
    document.as_obj()?;
    Some(document)
}

/// One x tick label, formatted the way the chart that draws it formats it.
pub fn x_axis_tick_text(chart: Chart, scale: &str, low: f64, high: f64, fraction: f64) -> String {
    if chart != Chart::Bar && scale == "log" && low > 0.0 && high > low {
        let low_log = low.log10();
        let span = high.log10() - low_log;
        return f4g(10.0_f64.powf(low_log + span * fraction));
    }
    let value = low + (high - low) * fraction;
    if chart == Chart::Bar {
        return f2(value);
    }
    f1(value)
}

/// Problems that leave a panel's stated x extent different from the drawn one.
pub fn check_x_axis_extent_stated(
    panel_id: &str,
    chart: Chart,
    x_axis: (f64, f64),
    markup: &str,
) -> Vec<String> {
    let drawn = x_axis_tick_re()
        .find_all(markup)
        .iter()
        .map(|groups| groups[0].clone().unwrap_or_default())
        .collect::<Vec<String>>();
    if drawn.len() < 2 {
        return vec![format!(
            "panel {}: its x axis draws {} tick label(s), so the x extent its summary \
             states cannot be measured back against the axis the panel drew",
            pyjson::repr_str(panel_id),
            drawn.len()
        )];
    }
    let (low, high) = x_axis;
    let scale = drawn_x_scale(markup);
    let expected: Vec<String> = (0..drawn.len())
        .map(|index| {
            x_axis_tick_text(
                chart,
                scale,
                low,
                high,
                index as f64 / (drawn.len() - 1) as f64,
            )
        })
        .collect();
    if expected == drawn {
        return Vec::new();
    }
    let expected_py = pyjson::py_list(&expected);
    let drawn_py = pyjson::py_list(&drawn);
    vec![format!(
        "panel {}: its summary states the x axis {}..{}, which draws the tick labels \
         {expected_py}, where the SVG's own x axis draws {drawn_py}; the summary has to \
         be the drawn geometry",
        pyjson::repr_str(panel_id),
        fg(low),
        fg(high)
    )]
}

/// Problems that leave a panel without a true statement of what it drew.
#[allow(clippy::too_many_arguments)]
pub fn check_panel_summary_stated(
    panel_id: &str,
    chart: Chart,
    x_label: &str,
    y_label: &str,
    series: &Series,
    bounds: &[Bound],
    extent: (f64, f64),
    markup: &str,
    plot_height: f64,
    stated_labels: &str,
    run_values: Option<&J>,
    fault: Option<&str>,
) -> Vec<String> {
    let expected = panel_summary_document(
        panel_id,
        chart,
        x_label,
        y_label,
        series,
        bounds,
        extent,
        markup,
        stated_labels,
        plot_height,
        run_values,
        fault,
    );
    let Some(found) = panel_summary_re().search(markup) else {
        return vec![format!(
            "panel {}: it carries no panel summary, so the only thing that says what \
             it drew is the render and the reader has to infer the producer's answer \
             from it. Every panel owes a <desc class=\"panel-summary\"> stating its \
             axis, its series, every bound line with its pixel position and why it is \
             drawn, and its reading; a plot step that cannot produce one is refused",
            pyjson::repr_str(panel_id)
        )];
    };
    let body = pyjson::unescape(&found.named("body").unwrap_or_default());
    let stated = match pyjson::parse(&body) {
        Ok(value) => value,
        Err(error) => {
            return vec![format!(
                "panel {}: its panel summary is not readable JSON ({error}), so \
                 nothing can be measured against it",
                pyjson::repr_str(panel_id)
            )];
        }
    };
    let mut problems = Vec::new();
    if stated.as_obj().is_none() {
        return vec![format!(
            "panel {}: its panel summary is a {}, not an object of the panel's own \
             fields",
            pyjson::repr_str(panel_id),
            stated.type_name()
        )];
    }
    let expected_members = expected.as_obj().unwrap_or_default().to_vec();
    for (key, value) in &expected_members {
        let matches_ = json_equal(stated.get(key), Some(value));
        if matches_ {
            continue;
        }
        problems.push(format!(
            "panel {}: its summary's {} is {} where the drawn panel measures {}; a \
             summary that is not the drawn panel's is worse than none, because the \
             reader trusts it instead of the pixels",
            pyjson::repr_str(panel_id),
            pyjson::repr_str(key),
            stated
                .get(key)
                .map(J::repr)
                .unwrap_or_else(|| "None".to_string()),
            value.repr()
        ));
    }
    let mut extra: Vec<String> = stated
        .as_obj()
        .unwrap_or_default()
        .iter()
        .map(|(key, _)| key.clone())
        .filter(|key| !expected_members.iter().any(|(name, _)| name == key))
        .collect();
    extra.sort();
    let extra_py = pyjson::py_list(&extra);
    if !extra.is_empty() {
        problems.push(format!(
            "panel {}: its summary carries field(s) {extra_py} the drawn panel does not \
             define, so at least one number is about something other than this render",
            pyjson::repr_str(panel_id)
        ));
    }
    let drawn_lines = drawn_bound_lines(markup);
    if drawn_lines.len() != bounds.len() {
        problems.push(format!(
            "panel {}: it draws {} bound line(s) and its summary states {}, so a \
             drawn bound is unaccounted for or a stated one is not drawn",
            pyjson::repr_str(panel_id),
            drawn_lines.len(),
            bounds.len()
        ));
    } else {
        let expected_bounds = expected.get("bounds").and_then(J::as_arr).unwrap_or(&[]);
        for (entry, drawn_y) in expected_bounds.iter().zip(drawn_lines.iter()) {
            let Some(pixel) = entry.get("px").and_then(J::as_f64) else {
                continue;
            };
            if (pixel - drawn_y).abs() <= SUMMARY_PIXEL_SLACK {
                continue;
            }
            problems.push(format!(
                "panel {}: its summary states the bound {} at px={pixel}, where the \
                 SVG draws that line at y1={drawn_y}; the summary has to be the drawn \
                 geometry",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(entry.get("label").and_then(J::as_str).unwrap_or(""))
            ));
        }
    }
    let legend = legend_series_labels(markup);
    let expected_names: Vec<String> = expected
        .get("series")
        .and_then(J::as_arr)
        .unwrap_or(&[])
        .iter()
        .map(|entry| {
            entry
                .get("name")
                .and_then(J::as_str)
                .unwrap_or("")
                .to_string()
        })
        .collect();
    if !legend.is_empty() && legend != expected_names {
        let expected_names_py = pyjson::py_list(&expected_names);
        let legend_py = pyjson::py_list(&legend);
        problems.push(format!(
            "panel {}: its summary names the series {expected_names_py}, where the \
             drawn legend names {legend_py}; a summary of series the panel does not \
             draw is a claim about another panel",
            pyjson::repr_str(panel_id)
        ));
    }
    if stated.get("x_axis").is_some() {
        let x_axis = expected.get("x_axis").and_then(J::as_arr).unwrap_or(&[]);
        problems.extend(check_x_axis_extent_stated(
            panel_id,
            chart,
            (
                x_axis.first().and_then(J::as_f64).unwrap_or(0.0),
                x_axis.get(1).and_then(J::as_f64).unwrap_or(0.0),
            ),
            markup,
        ));
    }
    problems
}

/// The tags a summary names, for the reader who wants the reason vocabulary.
pub const BOUND_DRAWN_LABELLED: &str = "labelled";
/// A bound line the panel draws without a label of its own.
pub const BOUND_DRAWN_UNLABELLED: &str = "unlabelled";
/// A bound line whose sliver the panel states in prose instead of a label.
pub const BOUND_DRAWN_STATED_SLIVER: &str = "stated-as-sliver";

/// The pixel slack a stated bound position is allowed against the drawn line.
pub const SUMMARY_PIXEL_SLACK: f64 = 0.05;

/// Python's `==` for two parsed JSON values, where an object's key order is
/// not part of the value.
pub fn json_equal(first: Option<&J>, second: Option<&J>) -> bool {
    match (first, second) {
        (Some(J::Obj(a)), Some(J::Obj(b))) => {
            a.len() == b.len()
                && a.iter().all(|(key, value)| {
                    b.iter()
                        .find(|(name, _)| name == key)
                        .is_some_and(|(_, other)| json_equal(Some(value), Some(other)))
                })
        }
        (Some(J::Arr(a)), Some(J::Arr(b))) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| json_equal(Some(x), Some(y)))
        }
        (Some(a), Some(b)) => a == b,
        (None, None) => true,
        _ => false,
    }
}

// -- values a panel names must be on the axis it draws ------------------------

/// The run's own guards the panel's drawn labels will name on its own axis.
pub fn named_guard_values(
    series: &Series,
    bounds: &[Bound],
    run_values: Option<&J>,
    crossing: bool,
) -> Vec<f64> {
    if !crossing || run_values.is_none() {
        return Vec::new();
    }
    let mut guards: Vec<f64> = Vec::new();
    for bound in bounds {
        for (_, value) in run_guards(run_values, Some(series), &bound.label) {
            if !guards.contains(&value) {
                guards.push(value);
            }
        }
    }
    guards.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    guards
}

/// Problems that make a named value unreadable: the axis does not resolve it.
pub fn check_named_values_in_axis(
    panel_id: &str,
    bounds: &[Bound],
    guards: &[f64],
    extent: (f64, f64),
    plot_height: Option<f64>,
) -> Vec<String> {
    let (low, high) = extent;
    let span = high - low;
    if !(span > 0.0) {
        let low_py = frepr(low);
        let high_py = frepr(high);
        return vec![format!(
            "panel {}: the axis {low_py}..{high_py} has no span",
            pyjson::repr_str(panel_id)
        )];
    }
    let plot_height =
        plot_height.unwrap_or((draw::HEIGHT - draw::PAD_TOP - draw::PAD_BOTTOM) as f64);
    let mut named: Vec<(f64, String)> = bounds
        .iter()
        .map(|bound| (bound.y, format!("bound {}", pyjson::repr_str(&bound.label))))
        .collect();
    named.extend(
        guards
            .iter()
            .map(|value| (*value, format!("named guard {}", fg(*value)))),
    );
    let mut problems = Vec::new();
    for (value, what) in named {
        let where_ = if low <= value && value <= high {
            let clear = (value - low).min(high - value) * plot_height / span;
            if clear >= MIN_AXIS_INSET_PIXELS {
                continue;
            }
            format!("only {} px from the nearest edge of it", f1(clear))
        } else if value > high {
            format!("{} px above it", f1((value - high) * plot_height / span))
        } else {
            format!("{} px below it", f1((low - value) * plot_height / span))
        };
        problems.push(format!(
            "panel {}: the axis {}..{} does not resolve the {what} (y={}) that the \
             panel names: it is {where_}, and a named value the axis does not show is \
             a claim the reader has no way to check",
            pyjson::repr_str(panel_id),
            f4g(low),
            f4g(high),
            fg(value)
        ));
    }
    problems
}

/// Problems that make a guard the panel *names* unreadable: no line at it.
pub fn check_named_guards_drawn(
    panel_id: &str,
    guards: &[f64],
    extent: (f64, f64),
    markup: &str,
    plot_height: Option<f64>,
) -> Vec<String> {
    if guards.is_empty() {
        return Vec::new();
    }
    let (low, high) = extent;
    let span = high - low;
    if !(span > 0.0) {
        let low_py = frepr(low);
        let high_py = frepr(high);
        return vec![format!(
            "panel {}: the axis {low_py}..{high_py} has no span",
            pyjson::repr_str(panel_id)
        )];
    }
    let plot_height =
        plot_height.unwrap_or((draw::HEIGHT - draw::PAD_TOP - draw::PAD_BOTTOM) as f64);
    let drawn = drawn_bound_values(markup, extent);
    let slack = 0.5 / plot_height * span;
    let mut problems = Vec::new();
    for value in guards {
        if drawn.iter().any(|line| (value - line).abs() <= slack) {
            continue;
        }
        let rounded: Vec<f64> = drawn
            .iter()
            .map(|line| fdec(*line, 4).parse().unwrap_or(0.0))
            .collect();
        let rounded_py = pyjson::py_float_list(&rounded);
        problems.push(format!(
            "panel {}: its label names the guard {}, but the artifact draws {} bound \
             line(s) at {rounded_py} and none of them is that guard: a tolerance the \
             panel names and does not draw is a claim the reader has to take on trust, \
             and a bar between that guard and the bound it is read against has no \
             second line to sit inside",
            pyjson::repr_str(panel_id),
            fg(*value),
            drawn.len()
        ));
    }
    problems
}

/// Problems that make an over-bound bar undrawable: the axis is too short.
pub fn check_bound_headroom(
    panel_id: &str,
    bounds: &[Bound],
    guards: &[f64],
    extent: (f64, f64),
    plot_height: Option<f64>,
    series: Option<&Series>,
) -> Vec<String> {
    if bounds.is_empty() && guards.is_empty() {
        return Vec::new();
    }
    let (low, high) = extent;
    let span = high - low;
    if !(span > 0.0) {
        let low_py = frepr(low);
        let high_py = frepr(high);
        return vec![format!(
            "panel {}: the axis {low_py}..{high_py} has no span",
            pyjson::repr_str(panel_id)
        )];
    }
    let plot_height =
        plot_height.unwrap_or((draw::HEIGHT - draw::PAD_TOP - draw::PAD_BOTTOM) as f64);
    let mut problems = Vec::new();
    let top = bounds
        .iter()
        .map(|bound| bound.y)
        .chain(guards.iter().cloned())
        .fold(f64::NEG_INFINITY, f64::max);
    let headroom = (high - top) / span * plot_height;
    if headroom < MIN_HEADROOM_PIXELS {
        problems.push(format!(
            "panel {}: the axis {}..{} keeps {} px above the highest value it names \
             (y={}), under the {} px an over-bound bar needs: a breach and a value \
             exactly at that bound would be drawn as the same picture, so the panel \
             could not show the failure it exists for",
            pyjson::repr_str(panel_id),
            f4g(low),
            f4g(high),
            f1(headroom),
            fg(top),
            f0(MIN_HEADROOM_PIXELS)
        ));
    }
    let Some(series) = series else {
        return problems;
    };
    let values = bound_values(series);
    for bound in bounds {
        if bound.band_arm.as_deref() != Some("lower") && failure_side(&values, bound.y) >= 0.0 {
            continue;
        }
        let y = bound.y;
        let below = (y - low) / span * plot_height;
        if below >= MIN_HEADROOM_PIXELS {
            continue;
        }
        problems.push(format!(
            "panel {}: the axis {}..{} keeps {} px below the bound {} (y={}), under \
             the {} px an under-bound bar needs: a bar that crosses that bound \
             downwards would be drawn flush with the frame, where it reads as the \
             frame's border and not as a crossing",
            pyjson::repr_str(panel_id),
            f4g(low),
            f4g(high),
            f1(below),
            pyjson::repr_str(&bound.label),
            fg(y),
            f0(MIN_HEADROOM_PIXELS)
        ));
    }
    problems
}

/// Problems that make half of a declared two-sided bound missing.
pub fn check_two_sided_bound_drawn(
    panel_id: &str,
    bounds: &[Bound],
    extent: (f64, f64),
    markup: &str,
    plot_height: Option<f64>,
) -> Vec<String> {
    let (low, high) = extent;
    let span = high - low;
    if !(span > 0.0) {
        let low_py = frepr(low);
        let high_py = frepr(high);
        return vec![format!(
            "panel {}: the axis {low_py}..{high_py} has no span",
            pyjson::repr_str(panel_id)
        )];
    }
    if !bounds
        .iter()
        .any(|bound| bound_band_half_width(bound).is_some())
    {
        return Vec::new();
    }
    let plot_height =
        plot_height.unwrap_or((draw::HEIGHT - draw::PAD_TOP - draw::PAD_BOTTOM) as f64);
    let drawn = drawn_bound_values(markup, extent);
    let slack = 0.5 / plot_height * span;
    let mut problems = Vec::new();
    for bound in bounds {
        let Some(half) = bound_band_half_width(bound) else {
            continue;
        };
        let y = bound.y;
        if !two_sided_bound(bound) {
            problems.push(format!(
                "panel {}: the bound {} declares a symmetric band of {}, while the \
                 value it declares is {}; the declaration does not say where the band \
                 is centred, so the arm on the other side is unknowable and a \
                 departure there would be drawn with no line to cross",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(&bound.label),
                fg(half),
                fg(y)
            ));
            continue;
        }
        let missing: Vec<f64> = [y, -y]
            .into_iter()
            .filter(|arm| !drawn.iter().any(|sample| (sample - arm).abs() <= slack))
            .collect();
        if !missing.is_empty() {
            let arms = missing
                .iter()
                .map(|arm| format!("{}{}", sign_char(*arm), f4g(arm.abs())))
                .collect::<Vec<String>>()
                .join(", ");
            let drawn_arms = drawn
                .iter()
                .map(|value| f4g(*value))
                .collect::<Vec<String>>()
                .join(", ");
            problems.push(format!(
                "panel {}: the bound {} names a two-sided band (+/-{}), but the panel \
                 draws no line at {arms}; its drawn arms are [{drawn_arms}]: a breach \
                 on the side with no line is a bar crossing nothing, which is the \
                 failure the panel is drawn for",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(&bound.label),
                fg(half)
            ));
        }
        for (arm, side, room) in [(y, "above", high - y), (-y, "below", -y - low)] {
            let pixels = room / span * plot_height;
            if pixels >= MIN_HEADROOM_PIXELS {
                continue;
            }
            let where_ = if pixels < 0.0 {
                format!("{} px outside the axis {side} it", f1(-pixels))
            } else {
                format!("only {} px inside the axis {side} it", f1(pixels))
            };
            let arm_text = format!("{}{}", sign_char(arm), f4g(arm.abs()));
            problems.push(format!(
                "panel {}: the band arm at {arm_text} is {where_} ({}..{}), so a bar \
                 crossing it has less than the {} px it needs: the breach and the arm \
                 would be drawn as the same picture",
                pyjson::repr_str(panel_id),
                f4g(low),
                f4g(high),
                f0(MIN_HEADROOM_PIXELS)
            ));
        }
    }
    problems
}

fn sign_char(value: f64) -> char {
    if value.is_sign_negative() { '-' } else { '+' }
}

// -- governance ---------------------------------------------------------------

/// Problems that make a crossed bound unreadable: nothing says what governs it.
pub fn check_bound_governance(
    panel_id: &str,
    series: &Series,
    bounds: &[Bound],
    run_values: Option<&J>,
) -> Vec<String> {
    let mut problems = Vec::new();
    let values = bound_values(series);
    for bound in bounds {
        let crossing = crossing_values(&values, bound.y);
        if crossing.is_empty() {
            continue;
        }
        if bound.series.is_some() || bound.x.is_some() {
            continue;
        }
        if run_values.is_some() {
            continue;
        }
        problems.push(format!(
            "panel {} draws the bound {} (y={}) with {} of {} bar(s) beyond it, and \
             the run's own measurements were not supplied, so the panel cannot say \
             whether that crossing is a breach or an arm's tolerated guard; pass the \
             run's MANDATE values (tools/mandate-check does) or declare which series \
             the bound governs with bounds[].series / bounds[].x",
            pyjson::repr_str(panel_id),
            pyjson::repr_str(&bound.label),
            fg(bound.y),
            crossing.len(),
            values.len()
        ));
    }
    problems
}

/// Problems that leave a bound drawn across arms with different floors unnamed.
pub fn check_bound_arm_governance(
    panel_id: &str,
    panel: &Panel,
    series: &Series,
    bounds: &[Bound],
    run_values: Option<&J>,
    markup: &str,
) -> Vec<String> {
    let planned = bar_bound_plan(panel, series, bounds, run_values);
    let drawn: Vec<String> = label_boxes(markup)
        .into_iter()
        .map(|(declared, _, _)| declared)
        .collect();
    let drawn_py = pyjson::py_list(&drawn);
    let mut problems = Vec::new();
    if panel.chart != Chart::Bar {
        let clause = arm_guard_clause(series, run_values);
        if clause.is_empty() {
            return Vec::new();
        }
        for bound in bounds {
            let label = governed_label(bound, series, run_values, false);
            if drawn.contains(&label) {
                continue;
            }
            problems.push(format!(
                "panel {}: the run states the guards {} for the arms this bound \
                 crosses, so one line across every series would be the bound of none \
                 of them; the panel has to name which arm it governs and what the \
                 other arms are read against. Missing: {} (drawn: {drawn_py})",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(&clause),
                pyjson::repr_str(&label)
            ));
        }
        return problems;
    }
    if planned.len() <= bounds.len() {
        return Vec::new();
    }
    for bound in &planned {
        let label = governed_label(bound, series, run_values, true);
        if drawn.contains(&label) {
            continue;
        }
        problems.push(format!(
            "panel {}: the run states the bound {} for some arms and {} for others, so \
             one line across every bar would be the floor of neither; the panel has to \
             draw each arm's own bound and name the arms it governs. Missing: {} \
             (drawn: {drawn_py})",
            pyjson::repr_str(panel_id),
            pyjson::repr_str(&planned[0].label),
            pyjson::repr_str(&planned[planned.len() - 1].label),
            pyjson::repr_str(&label)
        ));
    }
    problems
}

/// The drawn points beyond a bound that the panel reads as a departure.
pub fn crossing_points(series: &Series, y: f64) -> Vec<(String, f64, f64)> {
    let values = bound_values(series);
    if clustered_around(&values, y) {
        return Vec::new();
    }
    let mut below: Vec<(String, f64, f64)> = Vec::new();
    let mut above: Vec<(String, f64, f64)> = Vec::new();
    for (name, points) in series {
        for (x, value) in points {
            if *value < y {
                below.push((name.clone(), *x, *value));
            } else if *value > y {
                above.push((name.clone(), *x, *value));
            }
        }
    }
    let total = below.len() + above.len();
    if total == 0 {
        return Vec::new();
    }
    if (above.len() as f64) <= CROSSING_BULK_SHARE * total as f64 {
        return above;
    }
    if (below.len() as f64) <= CROSSING_BULK_SHARE * total as f64 {
        return below;
    }
    Vec::new()
}

/// The drawn series -- or arms -- the run states a bound of their own for.
pub fn own_bound_names(chart: Chart, series: &Series, run_values: Option<&J>) -> Vec<String> {
    if run_values.is_none() {
        return Vec::new();
    }
    if chart != Chart::Bar {
        return arm_guard_tokens(series, run_values)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
    }
    let mut own: Vec<String> = Vec::new();
    for (name, _) in series {
        for suffix in [PER_ARM_BOUND_SUFFIXES.0, PER_ARM_BOUND_SUFFIXES.1] {
            let key = format!("{name}{suffix}");
            let present = run_values
                .and_then(J::as_obj)
                .and_then(|members| members.iter().find(|(key_, _)| *key_ == key))
                .map(|(_, value)| numeric(value))
                .unwrap_or(false);
            if present && !own.contains(name) {
                own.push(name.clone());
            }
        }
    }
    if series.len() == 1 {
        let quantity = series[0].0.as_str();
        let arms = run_arm_names(series, run_values);
        let mut categories: Vec<f64> = series[0].1.iter().map(|(x, _)| *x).collect();
        categories.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        categories.dedup();
        if !arms.is_empty() && arms.len() == categories.len() {
            for arm in &arms {
                if arm_bound_source(arm, quantity, run_values).is_some() && !own.contains(arm) {
                    own.push(arm.clone());
                }
            }
        }
    }
    own
}

/// The spellings a drawn label may use for one series or arm.
pub fn series_tokens(name: &str) -> Vec<String> {
    vec![name.to_string(), series_label(name)]
}

/// Problems that leave a bound's crossing attributed to no bound at all.
#[allow(clippy::too_many_arguments)]
pub fn check_crossing_series_governed(
    panel_id: &str,
    chart: Chart,
    series: &Series,
    bounds: &[Bound],
    run_values: Option<&J>,
    markup: &str,
) -> Vec<String> {
    let own = own_bound_names(chart, series, run_values);
    if own.is_empty() {
        return Vec::new();
    }
    let stated = governed_names(markup);
    let arms = if series.len() == 1 {
        run_arm_names(series, run_values)
    } else {
        Vec::new()
    };
    let mut categories: Vec<f64> = series
        .iter()
        .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
        .collect();
    categories.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    categories.dedup();
    let by_category = !arms.is_empty() && arms.len() == categories.len();
    let mut problems = Vec::new();
    for bound in bounds {
        let window = bound_governed_x(bound);
        let points = crossing_points(series, bound.y);
        if points.is_empty() {
            continue;
        }
        let mut missing: Vec<String> = Vec::new();
        for (name, x, _value) in &points {
            if let Some(window) = window
                && window.0 <= *x
                && *x <= window.1
            {
                continue;
            }
            if bound.series.as_deref() == Some(name.as_str()) {
                continue;
            }
            let mut tokens = series_tokens(name);
            if by_category && let Some(index) = categories.iter().position(|value| value == x) {
                tokens.extend(series_tokens(&arms[index]));
            }
            if !tokens.iter().any(|token| own.contains(token)) {
                continue;
            }
            if tokens.iter().any(|token| stated.contains(token)) {
                continue;
            }
            let target = if own.contains(name) {
                name.clone()
            } else if by_category {
                categories
                    .iter()
                    .position(|value| value == x)
                    .map(|index| arms[index].clone())
                    .unwrap_or_else(|| name.clone())
            } else {
                name.clone()
            };
            if !missing.contains(&target) {
                missing.push(target);
            }
        }
        if missing.is_empty() {
            continue;
        }
        missing.sort();
        let missing_py = pyjson::py_list(&missing);
        problems.push(format!(
            "panel {}: the bound {} (y={}) is crossed by the drawn value(s) of \
             {missing_py}, which the run measures against a bound of their own, and no \
             drawn bound line says so: as drawn, a pass under that series' own guard \
             and a breach of this bound are the same picture. Name the series on the \
             bound's own label (the arm-guard clause), or draw the bound that governs \
             it beside it and label that line with `governs series <name>`",
            pyjson::repr_str(panel_id),
            pyjson::repr_str(&bound.label),
            fg(bound.y)
        ));
    }
    problems
}

/// The number and side a bar bound caption's crossing clause states, or `None`
/// when the label carries no such clause.
fn crossing_count_phrase(text: &str) -> Option<(usize, usize, &'static str)> {
    let found = crossing_count_re().search(text)?;
    let count: usize = found.group(1).unwrap_or_default().parse().ok()?;
    let total: usize = found.group(2).unwrap_or_default().parse().ok()?;
    let side = if found.group(3).unwrap_or_default() == "beyond" {
        "beyond"
    } else {
        "under"
    };
    Some((count, total, side))
}

/// Problems that leave a bar bound caption's crossing count contradicting the
/// bars and the line the panel draws.
///
/// The caption is what a reader trusts instead of measuring the pixels, so a
/// count the drawn bars and the drawn line contradict is worse than no caption.
/// This reads both the caption and the geometry back out of the artifact, so it
/// cannot pass on a panel whose text never reached the SVG, and it cannot be
/// satisfied by the generator agreeing with itself.
pub fn check_crossing_count_stated(panel_id: &str, markup: &str) -> Vec<String> {
    let lines = drawn_bound_lines(markup);
    if lines.is_empty() {
        return Vec::new();
    }
    // Every drawn bar's own height, including a value at the baseline whose
    // mark is hollow. A filled bar's is its `y`; a floor mark's is the baseline
    // it sits on.
    let mut heights: Vec<f64> = bar_boxes(markup)
        .iter()
        .map(|(_x0, y0, _x1, _y1)| *y0)
        .collect();
    heights.extend(zero_bar_marks(markup).iter().map(|mark| mark.box_.3));
    if heights.is_empty() {
        return Vec::new();
    }
    let mut problems = Vec::new();
    let mut seen: std::collections::BTreeSet<(String, i64)> = std::collections::BTreeSet::new();
    for (declared, _line, (_x0, _y0, _x1, bottom)) in label_boxes(markup) {
        let Some((stated, total, side)) = crossing_count_phrase(&declared) else {
            continue;
        };
        // The drawn bound line this label sits beside: the one nearest its box.
        let Some(line_y) = lines.iter().cloned().min_by(|a, b| {
            (a - bottom)
                .abs()
                .partial_cmp(&(b - bottom).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        }) else {
            continue;
        };
        // A wrapped label repeats the phrase on its title-bearing first line
        // and, if the wrap lands inside it, on a continuation; report one.
        if !seen.insert((declared.clone(), (line_y * 100.0).round() as i64)) {
            continue;
        }
        let beyond = heights.iter().filter(|height| **height < line_y).count();
        let under = heights.iter().filter(|height| **height > line_y).count();
        let drawn = if side == "beyond" { beyond } else { under };
        if stated == drawn && total == heights.len() {
            continue;
        }
        problems.push(format!(
            "panel {}: the drawn bound caption {} states {} of {} bar(s) {} it, and the \
             bars the panel draws beside that line ({} px) are {} beyond and {} under \
             it: the caption is what the reader trusts instead of the pixels, so a count \
             its own geometry contradicts is worse than no caption",
            pyjson::repr_str(panel_id),
            pyjson::repr_str(&declared),
            stated,
            total,
            side,
            f1(line_y),
            beyond,
            under
        ));
    }
    problems
}

// -- a label's fit, and a note's ---------------------------------------------

/// Problems that make a drawn bound label leave the panel's plot area.
pub fn check_label_fit(panel_id: &str, markup: &str) -> PlotResult<Vec<String>> {
    let (left, top, right, bottom) = panel_plot_rect(panel_id, markup)?;
    let mut problems = Vec::new();
    for (declared, line, (x0, y0, x1, y1)) in label_boxes(markup) {
        let mut outside = Vec::new();
        if x0 < left {
            outside.push(format!("{} px past its left edge", f1(left - x0)));
        }
        if x1 > right {
            outside.push(format!("{} px past its right edge", f1(x1 - right)));
        }
        if y0 < top {
            outside.push(format!("{} px above it", f1(top - y0)));
        }
        if y1 > bottom {
            outside.push(format!("{} px below it", f1(y1 - bottom)));
        }
        if outside.is_empty() {
            continue;
        }
        let detail = if line == declared {
            String::new()
        } else {
            format!(" (on the drawn line {})", pyjson::repr_str(&line))
        };
        problems.push(format!(
            "panel {}: the bound label {} does not fit the plot area{detail} -- its \
             drawn box {},{:.1}..{},{:.1} is {}, and the plot area is \
             {:.1},{:.1}..{:.1},{:.1}. The label is the part of the panel that says \
             what its bound governs, so it has to be inside the panel; shorten the \
             label or its guard list, or give the panel an axis with room for it",
            pyjson::repr_str(panel_id),
            pyjson::repr_str(&declared),
            f1(x0),
            y0,
            f1(x1),
            y1,
            outside.join(", "),
            left,
            top,
            right,
            bottom
        ));
    }
    Ok(problems)
}

/// Problems that make a bound label unreadable: it shares pixels with another.
pub fn check_label_overlap(panel_id: &str, markup: &str) -> Vec<String> {
    let mut problems = Vec::new();
    let boxes = label_boxes(markup);
    for (index, (declared, line, box_)) in boxes.iter().enumerate() {
        for (other_declared, other_line, other) in boxes[index + 1..].iter() {
            let area = draw::box_overlap(*box_, *other);
            if area <= LABEL_OVERLAP_PX2 {
                continue;
            }
            let same_anchor = declared == other_declared
                && (box_.1 - other.1).abs() < draw::LABEL_LINE_HEIGHT_PX - 0.5;
            let what = if same_anchor {
                format!(
                    "the bound label {} is drawn twice on the same anchor",
                    pyjson::repr_str(declared)
                )
            } else {
                format!(
                    "the bound labels {} and {} overlap",
                    pyjson::repr_str(declared),
                    pyjson::repr_str(other_declared)
                )
            };
            let smaller = ((box_.2 - box_.0) * (box_.3 - box_.1))
                .min((other.2 - other.0) * (other.3 - other.1));
            let mut problem = format!(
                "panel {}: {what} -- their boxes share {} of {} px, so both names are \
                 drawn where neither can be read",
                pyjson::repr_str(panel_id),
                f0(area),
                f0(smaller)
            );
            if line != declared || other_line != other_declared {
                problem.push_str(&format!(
                    " (on the drawn lines {} and {})",
                    pyjson::repr_str(line),
                    pyjson::repr_str(other_line)
                ));
            }
            problems.push(problem);
        }
    }
    problems
}

/// Problems that let bars read as one ribbon instead of as values.
pub fn check_bar_separation(panel_id: &str, markup: &str) -> Vec<String> {
    let mut bars = bar_boxes(markup);
    bars.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mut problems = Vec::new();
    for (index, first) in bars.iter().enumerate() {
        for second in bars[index + 1..].iter() {
            let gap = (second.0 - first.2).max(first.0 - second.2);
            if gap >= MIN_BAR_GAP_PIXELS {
                continue;
            }
            let stated = if gap < 0.0 {
                format!("overlap by {} px", f1(-gap))
            } else {
                format!("are {} px apart", f2(gap))
            };
            problems.push(format!(
                "panel {}: two drawn bars {stated}, under the {} px a reader needs to \
                 tell one value from the next; drawn flush they read as one constantly \
                 growing quantity rather than as separate values",
                pyjson::repr_str(panel_id),
                f0(MIN_BAR_GAP_PIXELS)
            ));
        }
    }
    problems
}

/// Problems that make a series unnamed to a human reader.
pub fn check_series_labels(panel_id: &str, markup: &str, series: &Series) -> Vec<String> {
    let drawn = legend_text(markup);
    let expected: Vec<String> = series.iter().map(|(name, _)| series_label(name)).collect();
    let mut problems = Vec::new();
    if drawn != expected {
        let drawn_py = pyjson::py_list(&drawn);
        let expected_py = pyjson::py_list(&expected);
        problems.push(format!(
            "panel {}: the legend draws {drawn_py}, not the labels its series have \
             {expected_py}; every series needs a human name",
            pyjson::repr_str(panel_id)
        ));
    }
    for text in &drawn {
        if raw_column_name_re().match_at(text).is_some() {
            problems.push(format!(
                "panel {}: the legend draws the raw column name {}, a reader is told \
                 the producer's spelling instead of the quantity's name",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(text)
            ));
        }
    }
    problems
}

/// Problems that make a drawn text unreadable or uninformative.
pub fn check_canvas_text_fit(panel_id: &str, markup: &str) -> Vec<String> {
    let mut problems = Vec::new();
    for (text, (x0, y0, x1, y1)) in drawn_text_boxes(markup) {
        if let Some(placeholder) = placeholder_text_re().search(&text) {
            problems.push(format!(
                "panel {}: the drawn text {} carries the empty placeholder {}; a panel \
                 must draw the measurement, not the template its absent evidence left \
                 behind",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(&text),
                pyjson::repr_str(&placeholder.group(0).unwrap_or_default())
            ));
        }
        if x0 < 0.0 || y0 < 0.0 || x1 > draw::WIDTH as f64 || y1 > draw::HEIGHT as f64 {
            let mut outside = Vec::new();
            if x0 < 0.0 {
                outside.push(format!("{} px past the left edge", f1(-x0)));
            }
            if x1 > draw::WIDTH as f64 {
                outside.push(format!(
                    "{} px past the right edge",
                    f1(x1 - draw::WIDTH as f64)
                ));
            }
            if y0 < 0.0 {
                outside.push(format!("{} px above it", f1(-y0)));
            }
            if y1 > draw::HEIGHT as f64 {
                outside.push(format!("{} px below it", f1(y1 - draw::HEIGHT as f64)));
            }
            problems.push(format!(
                "panel {}: the drawn text {} is {}, so the {}x{} canvas draws it \
                 clipped; a label the reader cannot finish is not a label",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(&text),
                outside.join(", "),
                draw::WIDTH,
                draw::HEIGHT
            ));
        }
    }
    problems
}

/// Problems that make a panel's own note unreadable where it is drawn.
pub fn check_note_fit(panel_id: &str, markup: &str) -> PlotResult<Vec<String>> {
    let boxes = note_boxes(markup);
    if boxes.is_empty() {
        return Ok(Vec::new());
    }
    let (left, top, right, bottom) = panel_plot_rect(panel_id, markup)?;
    let mut problems = Vec::new();
    for (text, (x0, y0, x1, y1)) in &boxes {
        if *x0 < left || *y0 < top || *x1 > right || *y1 > bottom {
            problems.push(format!(
                "panel {}: its note {} is drawn at {:.1},{:.1}..{:.1},{:.1}, outside \
                 the plot area {:.1},{:.1}..{:.1},{:.1}; a note about what this frame \
                 cannot show has to be inside the frame",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(text),
                x0,
                y0,
                x1,
                y1,
                left,
                top,
                right,
                bottom
            ));
        }
    }
    let mut others: Vec<(String, (f64, f64, f64, f64))> = label_boxes(markup)
        .into_iter()
        .map(|(declared, _, box_)| (declared, box_))
        .collect();
    others.extend(boxes.iter().cloned());
    for (index, (declared, box_)) in others.iter().enumerate() {
        for (other_text, other) in others[index + 1..].iter() {
            let area = draw::box_overlap(*box_, *other);
            if area <= LABEL_OVERLAP_PX2 {
                continue;
            }
            problems.push(format!(
                "panel {}: the drawn text {} shares {} px with {}, so neither is \
                 readable; a note is what the reader is told instead of the pixels, so \
                 it may not be drawn under another label",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(declared),
                f0(area),
                pyjson::repr_str(other_text)
            ));
        }
    }
    Ok(problems)
}

// -- the composition panels ---------------------------------------------------

/// The bounds a bar panel's own bars straddle, which none of them can fail.
pub fn target_bounds(panel: &Panel, series: &Series) -> Vec<Bound> {
    let values = bound_values(series);
    bound_specs(panel)
        .into_iter()
        .filter(|bound| {
            bound_side(&values, bound.y).is_none() && crossing_values(&values, bound.y).is_empty()
        })
        .collect()
}

/// One drawn point of a panel, keyed the way the two panels' points are matched.
type PointKey = (String, f64);

fn points_of(panel: &Panel, points: &Points) -> Vec<(PointKey, f64)> {
    let mut keyed: Vec<(PointKey, f64)> = panel_series(panel, points)
        .into_iter()
        .flat_map(|(name, series)| {
            series
                .into_iter()
                .map(move |(x, value)| ((name.clone(), x), value))
        })
        .collect();
    keyed.sort_by(|a, b| {
        a.0.0.cmp(&b.0.0).then(
            a.0.1
                .partial_cmp(&b.0.1)
                .unwrap_or(std::cmp::Ordering::Equal),
        )
    });
    keyed
}

/// The mandate panel that draws `(value - y) / y` for this panel's points.
pub fn relative_departure_panel(
    panel: &Panel,
    panels: &[Panel],
    points: &Points,
    y: f64,
) -> Option<(usize, Vec<(PointKey, f64)>)> {
    let mine = points_of(panel, points);
    if mine.is_empty() || y == 0.0 {
        return None;
    }
    for (index, other) in panels.iter().enumerate() {
        if other.id == panel.id || other.chart != Chart::Bar {
            continue;
        }
        let theirs = points_of(other, points);
        if theirs.len() != mine.len() || theirs.iter().zip(mine.iter()).any(|(a, b)| a.0 != b.0) {
            continue;
        }
        if mine
            .iter()
            .enumerate()
            .all(|(position, (_, value))| (theirs[position].1 - (value - y) / y).abs() <= 1e-5)
        {
            return Some((index, theirs));
        }
    }
    None
}

/// The note a share panel owes: what it is, and where its failure is drawn.
pub fn departure_view_note(panel: &Panel, panels: &[Panel], points: &Points) -> String {
    if panel.chart != Chart::Bar {
        return String::new();
    }
    let series = panel_series(panel, points);
    for bound in target_bounds(panel, &series) {
        let Some((index, values)) = relative_departure_panel(panel, panels, points, bound.y) else {
            continue;
        };
        let companion = &panels[index];
        let bounds = bound_specs(companion);
        if bounds.is_empty() {
            continue;
        }
        let departure_bound = bounds
            .iter()
            .map(|bound| bound.y)
            .fold(f64::INFINITY, f64::min);
        let mut worst: Vec<(String, f64)> = Vec::new();
        for entry in &companion.series {
            let scores: Vec<f64> = values
                .iter()
                .filter(|((name, _), _)| *name == entry.name)
                .map(|(_, value)| value.abs())
                .collect();
            if !scores.is_empty() {
                worst.push((
                    entry.name.clone(),
                    scores.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                ));
            }
        }
        let listed: Vec<String> = worst
            .iter()
            .map(|(name, value)| format!("{name} {}", pct2(*value)))
            .collect();
        return format!(
            "composition view - the departure is drawn on panel '{}' (bound {}): worst \
             {}",
            companion.id,
            pct1(departure_bound),
            listed.join(", ")
        );
    }
    String::new()
}

/// The note a panel with no bound of its own owes: where the bound is drawn.
pub fn bound_reference_note(
    panel: &Panel,
    panels: &[Panel],
    points: &Points,
    x_label: &str,
    y_label: &str,
    run_values: Option<&J>,
) -> String {
    if !panel.bounds.is_empty() {
        return String::new();
    }
    if !derived_x_bounds(panel, panels, points, x_label, y_label, run_values).is_empty() {
        return String::new();
    }
    for other in panels {
        if other.id == panel.id {
            continue;
        }
        if other.bounds.is_empty() {
            continue;
        }
        return format!(
            "composition view - this panel draws the quantity; the mandate's bound \
             '{}' is drawn on panel '{}'",
            other.bounds[0].label, other.id
        );
    }
    String::new()
}

/// The note a panel owes for the failure its own frame cannot carry.
pub fn composition_note(
    panel: &Panel,
    panels: &[Panel],
    points: &Points,
    x_label: &str,
    y_label: &str,
    run_values: Option<&J>,
) -> String {
    let departure = departure_view_note(panel, panels, points);
    if !departure.is_empty() {
        return departure;
    }
    bound_reference_note(panel, panels, points, x_label, y_label, run_values)
}

/// Problems that leave a composition panel silent about the failure it cannot show.
#[allow(clippy::too_many_arguments)]
pub fn check_departure_view_stated(
    panel_id: &str,
    panel: &Panel,
    panels: &[Panel],
    points: &Points,
    markup: &str,
    x_label: &str,
    y_label: &str,
    run_values: Option<&J>,
) -> Vec<String> {
    let expected = composition_note(panel, panels, points, x_label, y_label, run_values);
    if expected.is_empty() {
        return Vec::new();
    }
    let drawn = drawn_notes(markup).join(" ");
    if drawn.contains(&expected) {
        return Vec::new();
    }
    vec![format!(
        "panel {}: its own frame cannot carry the failure its mandate is read for -- \
         either its bound is a value its bars straddle, or it draws no bound at all -- \
         and the panel says neither where that failure is drawn nor what it measures. \
         Expected on the panel: {}; drawn: {}. A panel drawn silently is read as \
         evidence that there is no failure to draw",
        pyjson::repr_str(panel_id),
        pyjson::repr_str(&expected),
        pyjson::repr_str(&drawn)
    )]
}

/// Every x a bound declares it governs must be a category the panel draws.
pub fn check_bound_x_categories(panel_id: &str, series: &Series, bounds: &[Bound]) -> Vec<String> {
    let mut problems = Vec::new();
    let mut categories: Vec<f64> = series
        .iter()
        .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
        .collect();
    categories.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    categories.dedup();
    let categories_py = pyjson::py_float_list(&categories);
    let names: Vec<String> = series.iter().map(|(name, _)| name.clone()).collect();
    for bound in bounds {
        if let Some((low, high)) = bound_governed_x(bound) {
            let governed: Vec<f64> = categories
                .iter()
                .copied()
                .filter(|x| low <= *x && *x <= high)
                .collect();
            if governed.is_empty() {
                problems.push(format!(
                    "panel {}: the bound {} declares it governs x={}..{}, and the panel \
                     draws no category there (its categories are {categories_py})",
                    pyjson::repr_str(panel_id),
                    pyjson::repr_str(&bound.label),
                    fg(low),
                    fg(high)
                ));
            } else if governed.len() < categories.len()
                && (low != governed.iter().cloned().fold(f64::INFINITY, f64::min)
                    || high != governed.iter().cloned().fold(f64::NEG_INFINITY, f64::max))
            {
                problems.push(format!(
                    "panel {}: the bound {} declares it governs x={}..{}, which is not \
                     a boundary between the panel's categories {categories_py}",
                    pyjson::repr_str(panel_id),
                    pyjson::repr_str(&bound.label),
                    fg(low),
                    fg(high)
                ));
            }
        }
        if let Some(series_name) = &bound.series
            && !names.contains(series_name)
        {
            let mut sorted = names.clone();
            sorted.sort();
            let sorted_py = pyjson::py_list(&sorted);
            problems.push(format!(
                "panel {}: the bound {} declares it governs series {}, which the \
                     panel does not declare (its series are {sorted_py})",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(&bound.label),
                pyjson::repr_str(series_name)
            ));
        }
    }
    problems
}

// -- axis labels --------------------------------------------------------------

/// Problems that let an axis label contradict the run's own vocabulary.
pub fn check_x_axis_label(
    panel_id: &str,
    x_label: &str,
    categories: &[f64],
    run_values: Option<&J>,
) -> Vec<String> {
    let Some(reps) = repeated_category_index(categories, run_values) else {
        return Vec::new();
    };
    let expected = format!("rep (1..{reps})");
    if x_label == expected {
        return Vec::new();
    }
    vec![format!(
        "panel {}: the run measured reps={reps} and this panel draws a bar at each of \
         1..{reps}, but its x axis is labelled {}: those categories are the run's \
         repetitions, and a reader told they are {} is told something the run never \
         measured",
        pyjson::repr_str(panel_id),
        pyjson::repr_str(x_label),
        pyjson::repr_str(x_label)
    )]
}

/// Problems that label a panel's axis with a sibling panel's quantity.
pub fn check_axis_label(
    panel_id: &str,
    y_label: &str,
    series: &Series,
    declared: Option<&str>,
    _carried: &str,
) -> Vec<String> {
    if declared.is_some() || series.len() != 1 {
        return Vec::new();
    }
    let expected = series_label(&series[0].0);
    if y_label == expected {
        return Vec::new();
    }
    vec![format!(
        "panel {}: its y axis is labelled {} \u{2014} the mandate's shared y_label, \
         carried over from a sibling panel \u{2014} while its only series is {}: a \
         single-series panel names its own quantity, and a reader told the axis is {} \
         is told a unit the run never measured",
        pyjson::repr_str(panel_id),
        pyjson::repr_str(y_label),
        pyjson::repr_str(&expected),
        pyjson::repr_str(y_label)
    )]
}

// -- a cdf panel's x axis -----------------------------------------------------

/// Where `value` sits on an axis, as a share of that axis' span.
pub fn x_axis_share(x_min: f64, x_max: f64, scale: &str, value: f64) -> f64 {
    if scale == "log" {
        let low = x_min.log10();
        let high = x_max.log10();
        return (value.log10() - low) / (high - low);
    }
    (value - x_min) / (x_max - x_min)
}

/// The largest relative departure of `values` from a straight line.
fn straight_line_residual(values: &[f64], fractions: &[f64]) -> f64 {
    let low = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let high = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let span = high - low;
    if span <= 0.0 {
        return f64::INFINITY;
    }
    let count = values.len() as f64;
    let mean_x = fractions.iter().sum::<f64>() / count;
    let mean_y = values.iter().sum::<f64>() / count;
    let denominator: f64 = fractions.iter().map(|x| (x - mean_x).powi(2)).sum();
    if denominator <= 0.0 {
        return f64::INFINITY;
    }
    let slope = fractions
        .iter()
        .zip(values.iter())
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>()
        / denominator;
    let intercept = mean_y - slope * mean_x;
    fractions
        .iter()
        .zip(values.iter())
        .map(|(x, y)| (y - (intercept + slope * x)).abs())
        .fold(f64::NEG_INFINITY, f64::max)
        / span
}

/// `log` or `linear`: the scale the panel's own ticks put its axis on.
pub fn drawn_x_scale(markup: &str) -> &'static str {
    let mut values = Vec::new();
    for (index, _) in x_axis_tick_re().find_iter(markup).iter().enumerate() {
        let _ = index;
    }
    for groups in x_axis_tick_re().find_all(markup) {
        let text = groups[0].clone().unwrap_or_default();
        match text.parse::<f64>() {
            Ok(value) => values.push(value),
            Err(_) => return "linear",
        }
    }
    if values.len() < 4 || values.iter().any(|value| *value <= 0.0) {
        return "linear";
    }
    let fractions: Vec<f64> = (0..values.len())
        .map(|index| index as f64 / (values.len() - 1) as f64)
        .collect();
    let logarithmic = straight_line_residual(
        &values
            .iter()
            .map(|value| value.log10())
            .collect::<Vec<f64>>(),
        &fractions,
    );
    let arithmetic = straight_line_residual(&values, &fractions);
    if logarithmic < arithmetic {
        "log"
    } else {
        "linear"
    }
}

/// The panel's arm names the run asserts no guard of its own for.
pub fn reference_arm_names(series: &Series, run_values: Option<&J>) -> Vec<String> {
    let guarded: Vec<String> = arm_guard_tokens(series, run_values)
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    if guarded.is_empty() || guarded.len() == series.len() {
        return Vec::new();
    }
    series
        .iter()
        .map(|(name, _)| name.clone())
        .filter(|name| !guarded.contains(name))
        .collect()
}

/// The samples the reference arms contribute, and the axis they sit on.
pub fn reference_reach(series: &Series, reference: &[String]) -> (Vec<f64>, (f64, f64)) {
    let values: Vec<f64> = series
        .iter()
        .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
        .collect();
    let reach: Vec<f64> = series
        .iter()
        .filter(|(name, _)| reference.contains(name))
        .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
        .collect();
    if values.is_empty() || reach.is_empty() {
        return (Vec::new(), (0.0, 0.0));
    }
    (
        reach,
        (
            values.iter().cloned().fold(f64::INFINITY, f64::min),
            values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        ),
    )
}

/// `(share, low, high, largest)` for the reference arms, or `None`.
pub fn reference_reach_share(
    series: &Series,
    reference: &[String],
    scale: &str,
) -> Option<(f64, f64, f64, f64)> {
    let (reach, (low, high)) = reference_reach(series, reference);
    if reach.is_empty() || !(high > low) || (scale == "log" && low <= 0.0) {
        return None;
    }
    let largest = reach.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if scale == "log" && largest <= 0.0 {
        return None;
    }
    Some((x_axis_share(low, high, scale, largest), low, high, largest))
}

/// The x scale a cdf panel is drawn on, from its own dynamic range.
pub fn cdf_x_scale(series: &Series, reference: &[String]) -> &'static str {
    let (reach, (low, high)) = reference_reach(series, reference);
    if reach.is_empty() || !(high > low) || low <= 0.0 {
        return "linear";
    }
    if reference_reach_share(series, reference, "linear")
        .map(|(share, _, _, _)| share)
        .unwrap_or(0.0)
        >= MIN_REFERENCE_REACH_SHARE
    {
        return "linear";
    }
    "log"
}

/// The sentence a cdf panel owes when its drawn axis still squeezes.
pub fn cdf_scale_note(series: &Series, reference: &[String], scale: &str) -> String {
    let Some((share, low, high, largest)) = reference_reach_share(series, reference, scale) else {
        return String::new();
    };
    if share >= MIN_REFERENCE_REACH_SHARE {
        return String::new();
    }
    let kind = if scale == "log" {
        "logarithmic (base 10)"
    } else {
        "linear"
    };
    format!(
        "x axis {kind}: the reference arm {} reaches {} on {}..{}, {} of the width, \
         so the region that carries the failure is compressed at the left edge",
        reference.join(" "),
        f4g(largest),
        f4g(low),
        f4g(high),
        pct0(share)
    )
}

/// Whether the panel's own text states that its reference arm is squeezed.
pub fn reference_reach_stated(markup: &str, reference: &[String], share: f64) -> bool {
    let text = drawn_notes(markup).join(" ");
    !text.is_empty()
        && text.contains(&pct0(share))
        && reference.iter().any(|name| text.contains(name))
}

/// Problems that squeeze a cdf panel's reference arm to a sliver.
pub fn check_cdf_reference_reach(
    panel_id: &str,
    panel: &Panel,
    series: &Series,
    reference: &[String],
    markup: &str,
) -> Vec<String> {
    if panel.chart != Chart::Cdf || reference.is_empty() {
        return Vec::new();
    }
    let scale = drawn_x_scale(markup);
    let Some((share, low, high, largest)) = reference_reach_share(series, reference, scale) else {
        return Vec::new();
    };
    if share >= MIN_REFERENCE_REACH_SHARE {
        return Vec::new();
    }
    if reference_reach_stated(markup, reference, share) {
        return Vec::new();
    }
    vec![format!(
        "panel {}: the reference arm(s) {} reach {} on an x axis {}..{}, {} of the \
         width -- under the {} a distribution panel needs to show the shape of the arm \
         it is read for, so the region that carries the failure is a sliver; draw the \
         axis on the scale that keeps it legible, or state the squeeze on the panel",
        pyjson::repr_str(panel_id),
        reference.join(" "),
        f4g(largest),
        f4g(low),
        f4g(high),
        pct1(share),
        pct0(MIN_REFERENCE_REACH_SHARE)
    )]
}

// -- a bound the sibling panel states on this panel's x axis ------------------

/// The bounds a sibling panel draws on the quantity this panel's x axis carries.
pub fn derived_x_bounds(
    panel: &Panel,
    panels: &[Panel],
    points: &Points,
    mandate_x_label: &str,
    mandate_y_label: &str,
    run_values: Option<&J>,
) -> Vec<(f64, String, String)> {
    if panel.chart != Chart::Line && panel.chart != Chart::Cdf {
        return Vec::new();
    }
    let mine = panel_series(panel, points);
    let our_x = panel_x_label_for(
        panel,
        mandate_x_label,
        &mine
            .iter()
            .flat_map(|(_, series)| series.iter().map(|(x, _)| *x))
            .collect::<Vec<f64>>(),
        run_values,
    );
    let mut derived = Vec::new();
    for other in panels {
        if other.id == panel.id {
            continue;
        }
        let their_y = panel_y_label_for(other, mandate_y_label, &panel_series(other, points));
        if their_y != our_x {
            continue;
        }
        for bound in &other.bounds {
            derived.push((bound.y, bound.label.clone(), other.id.clone()));
        }
    }
    derived
}

/// A bound carried to the x axis, with the value every series reads there.
pub fn x_bound_label(
    bound: &(f64, String, String),
    series: &Series,
    x_unit: &str,
    y_unit: &str,
    drawn_range: Option<(f64, f64)>,
) -> String {
    let x = bound.0;
    let mut readings = Vec::new();
    for (name, points) in series {
        let Some(value) = value_at(points, x) else {
            continue;
        };
        readings.push(format!("{name} {}{y_unit}", f4g(value)));
    }
    let outside = drawn_range.is_some_and(|range| !(range.0 <= x && x <= range.1));
    let unit = if x_unit.is_empty() {
        String::new()
    } else {
        format!(" {x_unit}")
    };
    let mut text = format!("{} [at {}{unit}: {}]", bound.1, fg(x), readings.join(", "));
    if outside {
        text.push_str(" (x beyond this panel's drawn range)");
    }
    text
}

/// Problems that leave a mandate bound off a panel that can carry it in-frame.
#[allow(clippy::too_many_arguments)]
pub fn check_x_bound_drawn(
    panel_id: &str,
    panel: &Panel,
    panels: &[Panel],
    points: &Points,
    mandate_x_label: &str,
    mandate_y_label: &str,
    run_values: Option<&J>,
    markup: &str,
) -> PlotResult<Vec<String>> {
    let derived = derived_x_bounds(
        panel,
        panels,
        points,
        mandate_x_label,
        mandate_y_label,
        run_values,
    );
    if derived.is_empty() {
        return Ok(Vec::new());
    }
    let series = panel_series(panel, points);
    let drawn: Series = series
        .iter()
        .map(|(name, points)| (name.clone(), draw::decimate(points)))
        .collect();
    let x_unit = panel_unit(&panel_x_label_for(
        panel,
        mandate_x_label,
        &series
            .iter()
            .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
            .collect::<Vec<f64>>(),
        run_values,
    ));
    let y_unit = panel_unit(&panel_y_label_for(panel, mandate_y_label, &series));
    let xs: Vec<f64> = drawn
        .iter()
        .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
        .collect();
    let drawn_range = if xs.is_empty() {
        None
    } else {
        Some((
            xs.iter().cloned().fold(f64::INFINITY, f64::min),
            xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        ))
    };
    let drawn_xs: Vec<f64> = x_bound_mark_re()
        .find_all(markup)
        .iter()
        .map(|groups| groups[0].clone().unwrap_or_default().parse().unwrap_or(0.0))
        .collect();
    let drawn_xs_py = pyjson::py_float_list(&drawn_xs);
    let labels: Vec<String> = label_boxes(markup)
        .into_iter()
        .map(|(declared, _, _)| declared)
        .collect();
    let labels_py = pyjson::py_list(&labels);
    let (left, _, right, _) = panel_plot_rect(panel_id, markup)?;
    let mut problems = Vec::new();
    for bound in &derived {
        let reading = format!("at {}", fg(bound.0));
        let stated = labels.iter().find(|label| {
            label.starts_with(&bound.1) && **label != bound.1 && label.contains(&reading)
        });
        let Some(stated) = stated else {
            problems.push(format!(
                "panel {}: the mandate's bound {} is stated on panel {} on this \
                 panel's own x quantity, so this panel can draw the failure and has to \
                 say what the bound reads there. Expected on the panel: {}; drawn: \
                 {labels_py}",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(&bound.1),
                pyjson::repr_str(&bound.2),
                pyjson::repr_str(&x_bound_label(bound, &drawn, &x_unit, &y_unit, drawn_range))
            ));
            continue;
        };
        for (name, points) in &drawn {
            let Some(value) = value_at(points, bound.0) else {
                continue;
            };
            let pattern = regex(&format!(
                r"{}\s+([-+0-9.eE]+){}",
                regex_escape(name),
                regex_escape(&y_unit)
            ));
            let Some(written) = pattern.search(stated) else {
                problems.push(format!(
                    "panel {}: the bound {} is drawn without a value for series {}, \
                     which the panel plots; the series reads {}{y_unit} at {} and a \
                     reader told nothing is told the pointer the value replaced \
                     (drawn: {})",
                    pyjson::repr_str(panel_id),
                    pyjson::repr_str(&bound.1),
                    pyjson::repr_str(name),
                    f6g(value),
                    fg(bound.0),
                    pyjson::repr_str(stated)
                ));
                continue;
            };
            if let Some(problem) = stated_problem(
                panel_id,
                name,
                &format!("its value at {} as", fg(bound.0)),
                &written.group(1).unwrap_or_default(),
                value,
            ) {
                problems.push(problem);
            }
        }
        let outside = drawn_range.is_none_or(|range| !(range.0 <= bound.0 && bound.0 <= range.1));
        if outside {
            if !stated.contains("beyond this panel's drawn range") {
                problems.push(format!(
                    "panel {}: the bound {} at {} is outside this panel's drawn x range \
                     and the panel's sentence does not say so, so the value it states \
                     reads as a measurement (drawn: {})",
                    pyjson::repr_str(panel_id),
                    pyjson::repr_str(&bound.1),
                    fg(bound.0),
                    pyjson::repr_str(stated)
                ));
            }
            continue;
        }
        if drawn_xs.is_empty() {
            let range = drawn_range.unwrap();
            problems.push(format!(
                "panel {}: the bound {} at {} lies inside this panel's drawn x range \
                 {}..{}, so the artifact needs a vertical mark at it -- a bound the \
                 panel states but does not draw is a claim with no mark to read it \
                 against",
                pyjson::repr_str(panel_id),
                pyjson::repr_str(&bound.1),
                fg(bound.0),
                fg(range.0),
                fg(range.1)
            ));
            continue;
        }
        let range = drawn_range.unwrap();
        let scale = if range.0 > 0.0 && bound.0 > 0.0 {
            drawn_x_scale(markup)
        } else {
            "linear"
        };
        let want = left + x_axis_share(range.0, range.1, scale, bound.0) * (right - left);
        if drawn_xs.iter().any(|value| (value - want).abs() <= 1.5) {
            continue;
        }
        problems.push(format!(
            "panel {}: its drawn x marks are at {drawn_xs_py} px and the bound {} at {} \
             is {:.1} px; a mark somewhere else on the axis is not the bound the \
             sentence states",
            pyjson::repr_str(panel_id),
            pyjson::repr_str(&bound.1),
            fg(bound.0),
            want
        ));
    }
    Ok(problems)
}

/// The pattern-level escape of a literal, as Python's `re.escape`.
pub fn regex_escape(text: &str) -> String {
    // Python escapes every character that is not an ASCII letter, digit or
    // underscore in 3.7+; escaping a superset is harmless for the literals this
    // port feeds it (a series name and a unit).
    let mut out = String::new();
    for character in text.chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            out.push(character);
        } else {
            out.push('\\');
            out.push(character);
        }
    }
    out
}

// -- the drawn reading, and what a panel's data says --------------------------

/// The sentence one line panel states for one arm, from the arm's series.
pub fn arm_reading(arm: &str, points: &[(f64, f64)], detector: Option<&J>) -> String {
    let xs: Vec<f64> = points.iter().map(|(x, _)| *x).collect();
    let ys: Vec<f64> = points.iter().map(|(_, y)| *y).collect();
    let peak = ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let peak_index = ys
        .iter()
        .rposition(|value| *value == peak)
        .unwrap_or(ys.len() - 1);
    let after = points.len() - 1 - peak_index;
    let verdict = detector
        .and_then(|value| value.get("verdict"))
        .and_then(J::as_str);
    let mut parts = vec![match verdict {
        Some(verdict) if !verdict.trim().is_empty() => format!("{arm}: {verdict}"),
        _ => arm.to_string(),
    }];
    parts.push(format!("peak {} ms at {} s", f4g(peak), f2(xs[peak_index])));
    if after > 0 {
        parts.push(format!(
            "{after} sample(s) after it (next {} ms at {} s, last {} ms at {} s)",
            f4g(ys[peak_index + 1]),
            f2(xs[peak_index + 1]),
            f4g(ys[ys.len() - 1]),
            f2(xs[xs.len() - 1])
        ));
    } else {
        parts.push("nothing after it, so the series ends on its own maximum".to_string());
    }
    let wall = draw::gap_wall_seconds(points);
    let holes = draw::series_walls(points);
    if !holes.is_empty() {
        let largest =
            holes.iter().cloned().fold(
                holes[0],
                |best, hole| if hole.3 > best.3 { hole } else { best },
            );
        // No spaces around the hyphen: the pattern that reads this clause back
        // out (`STATED_HOLES_RE`) is Python's, and Python's writer has none.
        parts.push(format!(
            "{} sample gap(s) over {} s, largest {} s ({}-{} s), drawn as breaks, \
             not climbs",
            holes.len(),
            f2(wall.unwrap_or(0.0)),
            f2(largest.3),
            f2(largest.1),
            f2(largest.2)
        ));
    } else if let Some(wall) = wall {
        parts.push(format!("no sample gap over {} s", f2(wall)));
    } else {
        parts.push("no sample gap".to_string());
    }
    if let Some(detector) = detector {
        let mut measured = Vec::new();
        if let Some(value) = detector.get("rungs_at_edge")
            && numeric(value)
        {
            measured.push(format!("rungs_at_edge {}", json_g(value)));
        }
        if let Some(value) = detector.get("room")
            && numeric(value)
        {
            measured.push(format!("room {} ms", json_g(value)));
        }
        if !measured.is_empty() {
            parts.push(format!("detector {}", measured.join(" ")));
        }
    }
    parts.join(" - ")
}

/// The `(arm, sentence)` readings a line panel states, in series order.
pub fn panel_readings(series: &Series, censoring: Option<&J>) -> Vec<(String, String)> {
    let Some(censoring) = censoring.filter(|value| value.truthy()) else {
        return Vec::new();
    };
    series
        .iter()
        .map(|(name, points)| {
            let detector = censoring.get(name);
            (
                name.clone(),
                arm_reading(name, &draw::decimate(points), detector),
            )
        })
        .collect()
}

/// The series name a panel's own legend draws for this chart.
pub fn drawn_series_name(chart: Chart, name: &str) -> String {
    let _ = chart;
    series_label(name)
}

/// What a panel's data says, in the quantity's own units, as one sentence.
pub fn panel_reading(
    chart: Chart,
    series: &Series,
    bounds: &[Bound],
    extent: (f64, f64),
    plot_height: f64,
) -> String {
    let _ = chart;
    let values = bound_values(series);
    let span = extent.1 - extent.0;
    let mut parts = Vec::new();
    for (name, points) in series {
        let scores: Vec<f64> = points.iter().map(|(_, value)| *value).collect();
        if scores.is_empty() {
            continue;
        }
        parts.push(format!(
            "{} {}..{} ({} pts)",
            drawn_series_name(Chart::Line, name),
            sliver_number(scores.iter().cloned().fold(f64::INFINITY, f64::min)),
            sliver_number(scores.iter().cloned().fold(f64::NEG_INFINITY, f64::max)),
            scores.len()
        ));
    }
    for bound in bounds {
        let crossed = crossing_values(&values, bound.y);
        if crossed.is_empty() {
            continue;
        }
        let y = bound.y;
        let furthest = crossed.iter().cloned().fold(crossed[0], |best, value| {
            if (value - y).abs() > (best - y).abs() {
                value
            } else {
                best
            }
        });
        let drawn = furthest.max(extent.0).min(extent.1);
        let pixels = (drawn - y).abs() / span * plot_height;
        if pixels < MIN_BOUND_PIXELS {
            continue;
        }
        let side = if furthest > y { "beyond" } else { "under" };
        parts.push(format!(
            "{} of {} values {side} {} by up to {} ({} px)",
            crossed.len(),
            values.len(),
            pyjson::repr_str(&bound.label),
            sliver_number((furthest - y).abs()),
            f1(pixels)
        ));
    }
    parts.join("; ")
}

/// The clause naming what a drawn bound governs.
pub fn governed_label(
    bound: &Bound,
    series: &Series,
    run_values: Option<&J>,
    crossing: bool,
) -> String {
    let mut clauses: Vec<String> = Vec::new();
    if let Some(arms) = bound.arms.as_ref()
        && !arms.is_empty()
    {
        clauses.push(format!("governs {}", arms.join(" ")));
    }
    if bound.governs_none {
        clauses.push(
            "governs no arm of this run: every arm states a guard of its own, drawn on \
             its own band"
                .to_string(),
        );
    }
    if let Some(series_name) = &bound.series {
        clauses.push(format!("governs series {series_name}"));
    }
    if let Some((low, high)) = bound_governed_x(bound) {
        clauses.push(if low == high {
            format!("governs x={}", fg(low))
        } else {
            format!("governs x={}..{}", fg(low), fg(high))
        });
    }
    if crossing {
        let values = bound_values(series);
        let mut crossing_clauses: Vec<String> = Vec::new();
        let crossed = crossing_values(&values, bound.y);
        if !crossed.is_empty() {
            let side = if crossed[0] > bound.y {
                "beyond"
            } else {
                "under"
            };
            crossing_clauses.push(format!(
                "{} of {} bars {side} it",
                crossed.len(),
                values.len()
            ));
        }
        let guards = if bound.guard_key.is_some() || bound.guards_drawn {
            Vec::new()
        } else {
            run_guards(run_values, Some(series), &bound.label)
        };
        if !guards.is_empty() {
            let listed: Vec<String> = guards
                .iter()
                .map(|(key, value)| format!("{key}={}", fg(*value)))
                .collect();
            crossing_clauses.push(format!("run guards {}", listed.join(" ")));
        }
        if !crossing_clauses.is_empty() {
            clauses.push(crossing_clauses.join("; "));
        }
    } else {
        let clause = arm_guard_clause(series, run_values);
        if !clause.is_empty() {
            clauses.push(clause);
        }
    }
    if clauses.is_empty() {
        return bound.label.clone();
    }
    format!("{} [{}]", bound.label, clauses.join("; "))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series_of(name: &str, points: Vec<(f64, f64)>) -> Series {
        vec![(name.to_string(), points)]
    }

    #[test]
    fn a_bound_whose_band_the_axis_cannot_resolve_is_refused_and_a_statement_answers_it() {
        // The delivery-floor shape: seven bars at 1.000 and one at 0.994
        // against a 0.995 floor. On an axis the bars' own extent sets, the
        // floor's band is a sliver and the refusal is owed.
        let series = series_of(
            "delivery",
            vec![
                (1.0, 1.0),
                (2.0, 1.0),
                (3.0, 1.0),
                (4.0, 1.0),
                (5.0, 1.0),
                (6.0, 1.0),
                (7.0, 1.0),
                (8.0, 0.994),
            ],
        );
        let bounds = vec![Bound::new(0.995, "delivery floor 0.995".to_string())];
        // The band view the policy picks puts the floor's 1 % band across most
        // of the plot, so a bar through it is legible: green.
        let extent = draw::bar_axis_extent(&series, &bounds, None, None);
        let problems =
            check_panel_axis("delivery", &series, &bounds, extent, Some(228.0), None, "");
        assert!(problems.is_empty(), "{problems:?}");
        // A pinned zero-based axis is the measured defect: over `0..2` the
        // floor's band is about a pixel, and the panel states nothing about it.
        let pinned = (0.0, 2.0);
        let sub = check_panel_axis("delivery", &series, &bounds, pinned, Some(228.0), None, "");
        assert_eq!(sub.len(), 1, "{sub:?}");
        assert!(
            sub[0].contains("under the 6 px a bound needs"),
            "{}",
            sub[0]
        );
        let (band, pixels) = bound_band_pixels(&series, &bounds, &bounds[0], pinned, 228.0, None);
        assert!(pixels < MIN_BOUND_PIXELS, "{pixels}");
        // Nothing here departs from the floor by a legible distance, so a
        // statement would have nothing to say and the refusal stands.
        let statement =
            bound_sliver_statement(&bounds[0], &bound_values(&series), band, pinned, 228.0);
        assert!(statement.is_empty(), "{statement}");
        // A starved bar on the axis a fault sets does depart legibly, and there
        // the same panel may draw the bound and state where it sits.
        let starved = series_of(
            "delivery",
            vec![(1.0, 1.0), (2.0, 1.0), (3.0, 1.0), (4.0, 0.0)],
        );
        // An axis the fault's own body set: the band is sub-pixel there while a
        // starved bar is a long way below the floor.
        let fault_axis = (0.0, 5.0);
        let statement =
            bound_sliver_statement(&bounds[0], &bound_values(&starved), 0.01, fault_axis, 228.0);
        assert!(
            !statement.is_empty(),
            "a departure is drawn, so a statement is owed"
        );
        assert!(
            statement.contains("bound \"delivery floor 0.995\""),
            "{statement}"
        );
        assert!(
            check_panel_axis(
                "delivery",
                &starved,
                &bounds,
                fault_axis,
                Some(228.0),
                None,
                "delivery floor 0.995"
            )
            .is_empty(),
            "the statement is what the refusal is answered with"
        );
    }

    #[test]
    fn a_hole_drawn_as_a_climb_is_refused() {
        let mut points: Vec<(f64, f64)> = (0..10).map(|index| (index as f64, 1.0)).collect();
        for point in points.iter_mut().skip(5) {
            point.0 += 30.0;
        }
        let series = series_of("lone_tail", points.clone());
        // A single polyline with no sample markers: the hole is a wall.
        let bad = "<svg><polyline points=\"1,1 2,1 3,1\" fill=\"none\" stroke=\"#2563eb\"/>\
                   <polyline points=\"4,1 5,1 6,1\" fill=\"none\" stroke=\"#dc2626\"/></svg>";
        let problems = check_gap_honesty("latency", &series, bad);
        assert!(!problems.is_empty());
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("sample marker")),
            "{problems:?}"
        );
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("polyline segment")),
            "{problems:?}"
        );
        // Rebuilt honestly -- a marker per sample and one polyline per run --
        // the same points pass, so the refusal above measures the geometry.
        let mut good = String::from("<svg>");
        for (x, _) in draw::decimate(&points) {
            good.push_str(&format!("<circle class=\"sample\" cx=\"{x}\" cy=\"1\"/>"));
        }
        good.push_str("<polyline points=\"1,1 2,1\" fill=\"none\" stroke=\"#2563eb\"/>");
        good.push_str("<polyline points=\"3,1 4,1\" fill=\"none\" stroke=\"#2563eb\"/>");
        good.push_str("</svg>");
        assert!(check_gap_honesty("latency", &series, &good).is_empty());
    }

    #[test]
    fn a_summary_is_measured_back_out_of_the_drawn_lines() {
        let series = series_of("clean", vec![(1.0, 20.0), (2.0, 30.0)]);
        let bounds = vec![Bound::new(250.0, "M1 ceiling 250 ms".to_string())];
        let extent = (10.0, 260.0);
        // A markup whose drawn bound sits at a pixel the summary will state.
        let markup = "<svg><rect x=\"72\" y=\"24\" width=\"864\" height=\"228\" class=\"plot-bg\"/>\
                      <line class=\"bound\" x1=\"72\" y1=\"30.0\" x2=\"936\" y2=\"30.0\"/>\
                      <text x=\"72.0\" y=\"276\" text-anchor=\"middle\">1.0</text>\
                      <text x=\"936.0\" y=\"276\" text-anchor=\"middle\">2.0</text>\
                      <g class=\"legend\"><text>clean</text></g></svg>";
        let mut document = panel_summary_document(
            "latency",
            Chart::Line,
            "elapsed time (s)",
            "latency (ms)",
            &series,
            &bounds,
            extent,
            markup,
            "",
            228.0,
            None,
            None,
        );
        let with_summary = introduce_panel_summary(markup, &document);
        let problems = check_panel_summary_stated(
            "latency",
            Chart::Line,
            "elapsed time (s)",
            "latency (ms)",
            &series,
            &bounds,
            extent,
            &with_summary,
            228.0,
            "",
            None,
            None,
        );
        assert!(problems.is_empty(), "{problems:?}");
        // Vacuity: the same document with a *different* stated pixel is
        // refused, so the check measures the drawn geometry.
        if let J::Obj(members) = &mut document
            && let Some((_, J::Arr(bounds))) = members.iter_mut().find(|(key, _)| key == "bounds")
            && let J::Obj(entry) = &mut bounds[0]
            && let Some((_, value)) = entry.iter_mut().find(|(key, _)| key == "px")
        {
            *value = J::Float(99.0);
        }
        let tampered = introduce_panel_summary(markup, &document);
        let problems = check_panel_summary_stated(
            "latency",
            Chart::Line,
            "elapsed time (s)",
            "latency (ms)",
            &series,
            &bounds,
            extent,
            &tampered,
            228.0,
            "",
            None,
            None,
        );
        assert!(!problems.is_empty());
        assert!(
            problems.iter().any(|problem| problem.contains("99.0")),
            "{problems:?}"
        );
    }

    #[test]
    fn a_tail_inside_the_axis_owes_no_clip() {
        // A peak the axis can hold, or one that is a *majority* of the
        // samples, is the data's own extent and clips nothing; a lone outlier
        // against a read-at bound is clipped at that bound.
        let inside = series_of("arm", (0..20).map(|i| (i as f64, 200.0)).collect());
        let ceil = vec![Bound::new(250.0, "ceil".to_string())];
        assert_eq!(line_clip_owed(&inside, &ceil), None);
        let majority = series_of(
            "arm",
            (0..100)
                .map(|i| (i as f64, 400.0 + i as f64))
                .chain((0..100).map(|i| (i as f64, 100.0)))
                .collect(),
        );
        assert_eq!(line_clip_owed(&majority, &ceil), None);
        // The clip fixture: three bodies whose own ranges are tens of ms and a
        // single 1400 ms sample on the lone tail.
        let clean: Vec<(f64, f64)> = (0..200)
            .map(|i| (i as f64, 20.0 + (i % 20) as f64))
            .collect();
        let hostile: Vec<(f64, f64)> = (0..200)
            .map(|i| (i as f64, 40.0 + 2.0 * (i % 20) as f64))
            .collect();
        let lone: Vec<(f64, f64)> = (0..200)
            .map(|i| (i as f64, 10.0 + (i % 20) as f64))
            .chain(std::iter::once((199.0, 1400.0)))
            .collect();
        let series: Series = vec![
            ("clean".to_string(), clean),
            ("hostile".to_string(), hostile),
            ("lone_tail".to_string(), lone),
        ];
        let clip_series = series.clone();
        assert_eq!(line_clip_owed(&clip_series, &ceil), Some(250.0));
        // Green: the axis the policy draws is the clipped one, and the body it
        // used to compress is at least twice the height it had before.
        let plot_height = draw::line_plot_height(3, 0);
        let unclipped = draw::extent_including_bounds(
            draw::finite_extent(&clip_series),
            &[(250.0, String::new())],
        );
        let clipped = line_axis_extent(&clip_series, &ceil, None, Some(plot_height));
        let clean_values: Vec<f64> = clip_series[0].1.iter().map(|(_, y)| *y).collect();
        let spread = clean_values.iter().cloned().fold(f64::MIN, f64::max)
            - clean_values.iter().cloned().fold(f64::MAX, f64::min);
        let before = spread / (unclipped.1 - unclipped.0) * plot_height;
        let after = spread / (clipped.1 - clipped.0) * plot_height;
        assert!(clipped.1 < 1400.0, "{clipped:?}");
        assert!(after > 2.0 * before, "{before} -> {after}");
        // The vacuity of the clip's own refusal: the same panel measured
        // against the axis the old policy drew -- the data's own extent,
        // outlier and all, with no clip stated -- is refused by name.
        let problems = check_line_axis_clip_stated(
            "latency",
            Chart::Line,
            &clip_series,
            &ceil,
            unclipped,
            "<svg></svg>",
            None,
        );
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("one outlier set the axis"),
            "{}",
            problems[0]
        );
        assert!(problems[0].contains("1400"), "{}", problems[0]);
    }

    #[test]
    fn a_sliver_statement_is_not_owed_where_no_departure_is_drawn() {
        // The statement is only the honest reading where the panel *shows* a
        // departure: here the bound's furthest bar is below it and a pass, so
        // the panel keeps the refusal.
        let series = series_of("p99_ms", vec![(1.0, 35.3), (2.0, 81.8), (3.0, 98.5)]);
        let mut bounds = vec![Bound::new(
            100.0,
            "M2 non-degrading p99 bound (ms)".to_string(),
        )];
        let axis = draw::bar_axis_extent(&series, &bounds, None, None);
        assert!(
            sliver_bound_statements(
                &series,
                &mut bounds,
                axis,
                draw::bar_plot_height(1) as f64,
                None,
            )
            .is_empty()
        );
    }

    #[test]
    fn a_departure_smaller_than_the_band_is_not_a_statement() {
        // A departure has to be at least as legible as the six pixels the band
        // is measured against, or the panel has nothing to state but the
        // sliver itself. On a `0..5` axis `0.02` is 0.9 px and `0.2` is 9.1 px.
        let bound = Bound::new(0.0, "synthetic bound".to_string());
        let height = draw::bar_plot_height(1) as f64;
        assert_eq!(
            bound_sliver_statement(&bound, &[-0.02, 0.02], 0.005, (0.0, 5.0), height),
            ""
        );
        let statement = bound_sliver_statement(&bound, &[-0.2, 0.2], 0.005, (0.0, 5.0), height);
        assert!(
            statement.contains("furthest -0.2, 9.1 px from the bound"),
            "{statement}"
        );
    }

    #[test]
    fn a_panel_with_no_bound_states_none_by_design() {
        let block = panel_summary_block(
            &pyjson::parse(
                r#"{"panel": "goodput", "chart": "bar", "axis": [0.0, 1.0],
                "x_label": "rep (1..3)", "y_label": "MiB/s",
                "series": [{"name": "goodput", "points": 3, "min": 0.948, "max": 0.953}],
                "bounds": [], "reading": "goodput 0.948..0.953 (3 pts)", "fault": null}"#,
            )
            .expect("parses"),
        );
        assert!(block.contains("bound: none by design"), "{block}");
        assert!(!block.contains("fault:"), "{block}");
    }

    fn panels_of(declaration: &str) -> Vec<Panel> {
        let document = pyjson::parse(declaration).expect("parses");
        validate_declaration(&document, Path::new("M.json"))
            .expect("valid declaration")
            .panels
    }

    fn points_of(rows: &[(&str, &str, &str, &str)]) -> Points {
        let rows: Vec<(usize, String, String, String, String)> = rows
            .iter()
            .enumerate()
            .map(|(index, (panel, series, x, y))| {
                (
                    index + 2,
                    panel.to_string(),
                    series.to_string(),
                    x.to_string(),
                    y.to_string(),
                )
            })
            .collect();
        parse_points(&rows).expect("points parse")
    }

    #[test]
    fn the_guards_of_every_arm_on_a_line_panel_are_tokenised() {
        // The run states a guard per arm and the panel draws the arms, so each
        // guard is read off its own key and the panel's own legend
        // (`lone` -> `lone_tail`); a guard for an arm this panel does not draw
        // is left out rather than named against the wrong series.
        let run = pyjson::parse(
            r#"{"clean_p50": 24.4, "clean_p99": 93.1, "clean_max": 107.7,
                "hostile_p50": 45.8, "hostile_p99": 231.8, "hostile_max": 277.1,
                "lone_p50": 0.2, "lone_p99": 166.4, "lone_p999": 1465.8, "lone_max": 1567.1,
                "ceiling": 250.0, "hostile_p99_guard": 900, "hostile_over250_guard": 8,
                "lone_p99_guard": 3200, "lone_p999_guard": 8000, "lone_over250_guard": 8}"#,
        )
        .expect("parses");
        let series: Series = vec![
            ("clean".to_string(), vec![(1.0, 20.0)]),
            ("hostile".to_string(), vec![(1.0, 100.0)]),
            ("lone_tail".to_string(), vec![(1.0, 1600.0)]),
        ];
        let tokens = arm_guard_tokens(&series, Some(&run));
        let hostile = tokens
            .iter()
            .find(|(arm, _)| arm == "hostile")
            .expect("hostile's guards")
            .clone();
        let lone = tokens
            .iter()
            .find(|(arm, _)| arm == "lone_tail")
            .expect("lone_tail's guards")
            .clone();
        assert_eq!(
            hostile.1,
            vec![
                ("hostile_p99_guard".to_string(), "p99".to_string(), 900.0),
                (
                    "hostile_over250_guard".to_string(),
                    "over250".to_string(),
                    8.0
                ),
            ]
        );
        assert_eq!(
            lone.1,
            vec![
                ("lone_p99_guard".to_string(), "p99".to_string(), 3200.0),
                ("lone_p999_guard".to_string(), "p999".to_string(), 8000.0),
                ("lone_over250_guard".to_string(), "over250".to_string(), 8.0),
            ]
        );
        // Only the arms that have a guard are returned at all, and an arm the
        // panel does not draw is not named against it.
        let arms: Vec<String> = tokens.iter().map(|(arm, _)| arm.clone()).collect();
        assert_eq!(arms, vec!["hostile".to_string(), "lone_tail".to_string()]);
        assert!(
            arm_guard_tokens(
                &vec![("clean".to_string(), Vec::<(f64, f64)>::new())],
                Some(&run)
            )
            .is_empty(),
            "a guard for an arm the panel does not draw is not named against it"
        );
    }

    #[test]
    fn a_per_arm_bound_is_stated_only_where_the_run_restates_it() {
        let panels = panels_of(
            r#"{"mandate": "M2", "title": "t", "x_label": "arm", "y_label": "value",
                "panels": [{"id": "latency", "chart": "bar",
                    "series": [{"name": "p99_ms"}],
                    "bounds": [{"y": 100.0, "label": "M2 non-degrading p99 bound (ms)"}]}]}"#,
        );
        let panel = &panels[0];
        let series: Series = vec![(
            "p99_ms".to_string(),
            vec![(1.0, 26.251), (2.0, 61.5), (3.0, 185.8015)],
        )];
        let bounds = bound_specs(panel);
        let run = pyjson::parse(
            r#"{"clean_p99_ms": 26.251, "hostile_p99_ms": 61.5, "lone_p99_ms": 185.8015,
                "hostile_p99_guard": 200.0, "lone_p99_guard": 400.0}"#,
        )
        .expect("parses");
        let plan = arm_bound_values(panel, &series, &bounds, Some(&run))
            .expect("the run restates two arms");
        assert_eq!(plan.arms, vec!["clean", "hostile", "lone"]);
        let per_arm: Vec<(&str, Vec<f64>)> = plan
            .arms
            .iter()
            .zip(plan.lines.iter())
            .map(|(arm, lines)| (arm.as_str(), lines.iter().map(|line| line.value).collect()))
            .collect();
        assert_eq!(
            per_arm,
            vec![
                ("clean", vec![100.0]),
                ("hostile", vec![200.0]),
                ("lone", vec![400.0]),
            ]
        );
        // Vacuity: the same panel against a run that bears on a different
        // quantity restates nothing, so the declaration's own bound stands.
        let unrelated = pyjson::parse(
            r#"{"flows": 4, "imbalance_bound": 0.01, "fair_share": 0.25,
                "delivery_floor": 0.995, "hostile_p99_guard": 900.0}"#,
        )
        .expect("parses");
        assert!(
            arm_bound_values(panel, &series, &bounds, Some(&unrelated)).is_none(),
            "a run that bears on another quantity owes no restatement"
        );
        let effective = effective_bounds(panel, &series, &bounds, Some(&run));
        assert_eq!(effective.len(), 3, "{effective:?}");
        assert_eq!(effective[0].y, 100.0);
        assert_eq!(effective[0].arms, Some(vec!["clean".to_string()]));
        assert_eq!(effective[0].window, Some(vec![1.0]));
        assert_eq!(effective[0].label, "M2 non-degrading p99 bound (ms)");
        assert_eq!(effective[1].y, 200.0);
        assert_eq!(effective[1].arms, Some(vec!["hostile".to_string()]));
        assert_eq!(effective[1].window, Some(vec![2.0]));
        assert_eq!(effective[1].label, "run hostile_p99_guard=200");
        assert_eq!(effective[2].y, 400.0);
        assert_eq!(effective[2].arms, Some(vec!["lone".to_string()]));
        assert_eq!(effective[2].window, Some(vec![3.0]));
        assert_eq!(effective[2].label, "run lone_p99_guard=400");
    }

    #[test]
    fn a_panel_that_can_show_its_own_failure_owes_no_departure_note() {
        // The check is about a bound the bars *straddle*, not about every bar
        // panel: a floor is a line a bar can fail, so that panel can show its
        // own failure and is left alone -- as is a share panel whose mandate
        // declares no panel carrying the departure for it to name.
        let panels = panels_of(
            r#"{"mandate": "M4", "title": "t", "x_label": "flow", "y_label": "value",
                "panels": [
                    {"id": "delivery", "chart": "bar", "series": [{"name": "clean"}],
                     "bounds": [{"y": 0.995, "label": "M4 per-flow delivery floor 0.995"}]},
                    {"id": "shares", "chart": "bar", "series": [{"name": "clean"}],
                     "bounds": [{"y": 0.25, "label": "fair share 25.0%"}]}]}"#,
        );
        let points = points_of(&[
            ("delivery", "clean", "1.0", "1.0"),
            ("shares", "clean", "1.0", "0.250029"),
            ("shares", "clean", "2.0", "0.249912"),
        ]);
        let delivery = &panels[0];
        let shares = &panels[1];
        let delivery_series = panel_series(delivery, &points);
        let shares_series = panel_series(shares, &points);
        assert!(target_bounds(delivery, &delivery_series).is_empty());
        assert_eq!(target_bounds(shares, &shares_series).len(), 1);
        for panel in [delivery, shares] {
            assert_eq!(departure_view_note(panel, &panels, &points), "");
        }
    }

    #[test]
    fn a_legend_drawing_a_column_name_is_refused() {
        let series = series_of("shaper_forwarded", vec![(1.0, 1.0)]);
        let bad = "<svg><g class=\"legend\"><line/><text x=\"1\" y=\"1\">shaper_forwarded</text></g></svg>";
        let problems = check_series_labels("goodput", bad, &series);
        assert!(!problems.is_empty(), "{problems:?}");
        let good = "<svg><g class=\"legend\"><line/><text x=\"1\" y=\"1\">shaper forwarded</text></g></svg>";
        assert!(check_series_labels("goodput", good, &series).is_empty());
    }

    // -- the checks ported from `tools/test_mandate_plot.py` -----------------
    //
    // Each of these reads the *drawn* artifact rather than trusting the code
    // that wrote it, and each is paired with the markup that is green on the
    // same rule, so the refusal is a measurement and not a predicate that is
    // true of everything.

    fn bars_of(name: &str, points: Vec<(f64, f64)>) -> Series {
        series_of(name, points)
    }

    #[test]
    fn a_bound_label_moved_to_the_canvas_origin_is_refused() {
        let series = bars_of("s", vec![(1.0, 1.0), (2.0, 2.5)]);
        let bounds = vec![Bound::new(2.0, "a bound".to_string())];
        let markup = draw::svg_bar_chart("t", "x", "arm", &series, &bounds, None, None, "");
        assert!(check_label_fit("p", &markup).expect("reads").is_empty());
        // The same markup with its own label moved to the canvas origin: the
        // vertical extent is width-free, so the left edge and the top are both
        // what a misplaced label crosses.
        let found = regex_dotall(r#"<text class="bound-label"[^>]*>"#)
            .search(&markup)
            .expect("the panel draws a bound label")
            .group(0)
            .unwrap_or_default();
        let misplaced = markup.replacen(
            &found,
            "<text class=\"bound-label\" x=\"0.0\" y=\"0.0\">",
            1,
        );
        assert_ne!(misplaced, markup);
        let problems = check_label_fit("p", &misplaced).expect("reads");
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("does not fit the plot area"),
            "{}",
            problems[0]
        );
        assert!(
            problems[0].contains("past its left edge"),
            "{}",
            problems[0]
        );
        assert!(problems[0].contains("above it"), "{}", problems[0]);
    }

    #[test]
    fn a_bound_label_drawn_twice_on_one_anchor_is_refused() {
        // The fair-share panel the preserved run drew, whose label is a real
        // annotation the first time.
        let series = bars_of(
            "clean",
            vec![
                (1.0, 0.250059),
                (2.0, 0.250059),
                (3.0, 0.249941),
                (4.0, 0.249941),
            ],
        );
        let bounds = vec![Bound::new(0.25, "fair share 25.0%".to_string())];
        let markup = draw::svg_bar_chart("t", "x", "share", &series, &bounds, None, None, "");
        assert!(check_label_overlap("shares", &markup).is_empty());
        let element = regex_dotall(r#"<text class="bound-label".*?</text>"#)
            .search(&markup)
            .expect("a bound label")
            .group(0)
            .unwrap_or_default();
        // Red: the artifact with its own label duplicated on the same anchor.
        let duplicated = markup.replacen(&element, &format!("{element}{element}"), 1);
        let problems = check_label_overlap("shares", &duplicated);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("drawn twice on the same anchor"),
            "{}",
            problems[0]
        );
        // Red: two *different* labels over one another are an overlap.
        let other = element.replace("fair share 25.0%", "fair-share bound");
        let overlapping = markup.replacen(&element, &format!("{element}{other}"), 1);
        let problems = check_label_overlap("shares", &overlapping);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("overlap"), "{}", problems[0]);
    }

    #[test]
    fn bars_drawn_flush_are_refused() {
        // The geometry the preserved run drew: three 311 px bars whose
        // rectangles overlapped, which the eye read as one staircase.
        let staircase = r##"<svg viewBox="0 0 960 300">
            <rect x="72.0" y="178.2" width="311.0" height="73.8" fill="#2563eb"/>
            <rect x="331.2" y="90.1" width="311.0" height="161.9" fill="#2563eb"/>
            <rect x="590.4" y="31.3" width="311.0" height="220.7" fill="#2563eb"/>
            </svg>"##;
        let problems = check_bar_separation("latency", staircase);
        assert!(!problems.is_empty());
        assert!(
            problems[0].contains("overlap by 51.8 px"),
            "{}",
            problems[0]
        );
        // Green: a panel the renderer drew has its bars apart.
        let series = bars_of("p99_ms", vec![(1.0, 26.251), (2.0, 61.5), (3.0, 185.8)]);
        let bounds = vec![Bound::new(
            100.0,
            "M2 non-degrading p99 bound (ms)".to_string(),
        )];
        let markup = draw::svg_bar_chart("t", "x", "value", &series, &bounds, None, None, "");
        assert_eq!(bar_boxes(&markup).len(), 3);
        assert!(check_bar_separation("latency", &markup).is_empty());
    }

    #[test]
    fn a_bar_caption_whose_crossing_count_contradicts_its_bars_is_refused() {
        // The M2-latency shape: clean 25.4, hostile 112.9, lone_tail 161.7 read
        // against a 100 ms bound that governs the clean arm only. One bar is
        // under the drawn line and two are beyond it, and the caption the
        // renderer writes states exactly that.
        let series = bars_of(
            "p99_ms",
            vec![(1.0, 25.377), (2.0, 112.855), (3.0, 161.719_958)],
        );
        let mut bound = Bound::new(100.0, "M2 non-degrading p99 bound (ms)".to_string());
        bound.x = Some((1.0, 1.0));
        let markup = draw::svg_bar_chart("t", "x", "value", &series, &[bound], None, None, "");
        assert!(markup.contains("1 of 3 bars under it"), "{markup}");
        assert!(
            check_crossing_count_stated("latency", &markup).is_empty(),
            "{markup}"
        );
        // Red: the caption states a count the geometry contradicts. The check
        // reads both the caption and the geometry back out of the artifact, so
        // it reddens and names what it measured.
        let wrong = markup.replace("1 of 3 bars under it", "2 of 3 bars under it");
        assert!(
            wrong.contains("2 of 3 bars under it"),
            "the mutation applied"
        );
        let problems = check_crossing_count_stated("latency", &wrong);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("2 of 3 bars under it"), "{problems:?}");
        assert!(problems[0].contains("1 under"), "{problems:?}");
        assert!(problems[0].contains("2 beyond"), "{problems:?}");
        // A count of zero is a claim about the drawn geometry too.
        let none = markup.replace("1 of 3 bars under it", "0 of 3 bars under it");
        assert_eq!(check_crossing_count_stated("latency", &none).len(), 1);
    }

    #[test]
    fn a_floor_mark_that_is_not_hollow_or_not_drawn_is_refused() {
        let series = bars_of("clean", vec![(1.0, 0.0), (2.0, 0.0)]);
        let extent = (0.0, 0.021);
        let drawn = draw::svg_bar_chart(
            "t",
            "flow",
            "departure",
            &series,
            &[],
            Some(extent),
            None,
            "",
        );
        assert_eq!(zero_bar_marks(&drawn).len(), 2, "{drawn}");
        // The panel is checked the way the renderer checks it: with its own
        // summary already in place, so the mark has to be both drawn and stated.
        let plot_height = draw::bar_plot_height(1) as f64;
        let summary = panel_summary_document(
            "imbalance",
            Chart::Bar,
            "flow",
            "departure",
            &series,
            &[],
            extent,
            &drawn,
            "",
            plot_height,
            None,
            None,
        );
        let markup = introduce_panel_summary(&drawn, &summary);
        assert!(
            check_zero_bar_marks("imbalance", &series, extent, &markup)
                .expect("reads")
                .is_empty()
        );
        // Red: the panel draws the mark and its summary is silent about it.
        let silent = drawn.clone();
        let problems = check_zero_bar_marks("imbalance", &series, extent, &silent).expect("reads");
        assert!(
            problems.iter().any(|p| p.contains("states none")),
            "{problems:?}"
        );
        // Red: the mark painted solid. A filled rect of the mark's own size is
        // exactly what a small non-zero bar is, so it must be refused.
        let solid = markup.replace("fill=\"none\"", "fill=\"#2563eb\"");
        assert_ne!(solid, markup);
        let problems = check_zero_bar_marks("imbalance", &series, extent, &solid).expect("reads");
        assert!(
            problems.iter().any(|p| p.contains("is painted")),
            "{problems:?}"
        );
        // Red: a mark drawn shorter than the visible floor.
        let short = markup.replace("height=\"4.0\"", "height=\"1.0\"");
        assert_ne!(short, markup);
        let problems = check_zero_bar_marks("imbalance", &series, extent, &short).expect("reads");
        assert!(
            problems.iter().any(|p| p.contains("px tall")),
            "{problems:?}"
        );
        // Red: the mark dropped, which is the defect this whole change is
        // about -- the producer measured 0 and the panel shows nothing.
        let dropped = markup.replace("class=\"zero-bar\"", "class=\"bar\"");
        assert_ne!(dropped, markup);
        let problems = check_zero_bar_marks("imbalance", &series, extent, &dropped).expect("reads");
        assert!(
            problems.iter().any(|p| p.contains("draws 0 floor mark")),
            "{problems:?}"
        );
    }

    #[test]
    fn a_summary_that_states_a_floor_mark_the_panel_dropped_is_refused() {
        let series = bars_of("clean", vec![(1.0, 0.0), (2.0, 0.0)]);
        let extent = (0.0, 0.021);
        let drawn = draw::svg_bar_chart(
            "t",
            "flow",
            "departure",
            &series,
            &[],
            Some(extent),
            None,
            "",
        );
        let plot_height = draw::bar_plot_height(1) as f64;
        let stated = panel_summary_document(
            "imbalance",
            Chart::Bar,
            "flow",
            "departure",
            &series,
            &[],
            extent,
            &drawn,
            "",
            plot_height,
            None,
            None,
        );
        let zero = stated
            .get("zero_bar")
            .expect("the panel summary must state the floor mark it drew");
        assert_eq!(zero.get("bars").and_then(J::as_i64), Some(2));
        assert_eq!(zero.get("values").and_then(J::as_i64), Some(2));
        assert_eq!(zero.get("mark").and_then(J::as_str), Some("hollow-bar"));
        let markup = introduce_panel_summary(&drawn, &stated);
        assert!(
            check_panel_summary_stated(
                "imbalance",
                Chart::Bar,
                "flow",
                "departure",
                &series,
                &[],
                extent,
                &markup,
                plot_height,
                "",
                None,
                None,
            )
            .is_empty()
        );
        // Red: the same stated summary against a panel whose marks are not
        // drawn. The summary has to be the drawn geometry, so a panel that
        // says it drew two floor marks and drew none is refused by name.
        let dropped = drawn.replace("class=\"zero-bar\"", "class=\"bar\"");
        let markup = introduce_panel_summary(&dropped, &stated);
        let problems = check_panel_summary_stated(
            "imbalance",
            Chart::Bar,
            "flow",
            "departure",
            &series,
            &[],
            extent,
            &markup,
            plot_height,
            "",
            None,
            None,
        );
        assert!(
            problems.iter().any(|p| p.contains("zero_bar")),
            "{problems:?}"
        );
    }

    #[test]
    fn a_clipped_axis_label_and_an_empty_placeholder_are_refused() {
        // Red: the y label the old renderer wrote with its band-view note
        // appended is long enough to run off the canvas when it is rotated
        // down the left margin.
        let series = bars_of("delivery", vec![(1.0, 1.0), (2.0, 1.0), (3.0, 1.0)]);
        let long = "delivery (received / offered) [band view 0.979..1.001, not 0-based]";
        let markup = draw::svg_bar_chart("t", "arm", long, &series, &[], None, None, "");
        let problems = check_canvas_text_fit("delivery", &markup);
        assert!(!problems.is_empty(), "{problems:?}");
        assert!(
            problems.iter().any(|p| p.contains("draws it clipped")),
            "{problems:?}"
        );
        // Red: a drawn label carrying the empty template its absent evidence
        // left behind.
        let bounds = vec![Bound::new(1.0, "M2 delivery floor 1.000 []".to_string())];
        let templated =
            draw::svg_bar_chart("t", "arm", "delivery", &series, &bounds, None, None, "");
        let problems = check_canvas_text_fit("delivery", &templated);
        assert!(
            problems.iter().any(|p| p.contains("empty placeholder")),
            "{problems:?}"
        );
        // Green: the same panel with the measurement in place of the template.
        let bounds = vec![Bound::new(1.0, "M2 delivery floor 1.000".to_string())];
        let good = draw::svg_bar_chart("t", "arm", "delivery", &series, &bounds, None, None, "");
        assert!(check_canvas_text_fit("delivery", &good).is_empty());
        assert_eq!(
            draw::band_view_note((0.979, 1.001)),
            "band view 0.979..1.001, not 0-based"
        );
        assert_eq!(draw::band_view_note((0.0, 0.26)), "");
    }

    #[test]
    fn a_named_guard_the_axis_does_not_reach_is_refused() {
        let bounds = vec![Bound::new(
            100.0,
            "M2 non-degrading p99 bound (ms)".to_string(),
        )];
        // Red: an axis that tops out below a guard the panel's own label names.
        let problems = check_named_values_in_axis(
            "latency",
            &bounds,
            &[200.0, 400.0],
            (0.0, 190.0),
            Some(228.0),
        );
        assert!(!problems.is_empty(), "{problems:?}");
        assert!(
            problems.iter().any(|p| p.contains("named guard 200")),
            "{problems:?}"
        );
        assert!(
            problems.iter().any(|p| p.contains("does not resolve")),
            "{problems:?}"
        );
        // Green: the automatic axis carries every guard, with the inset a
        // named value needs.
        assert!(
            check_named_values_in_axis(
                "latency",
                &bounds,
                &[200.0, 400.0],
                (0.0, 450.0),
                Some(228.0)
            )
            .is_empty()
        );
    }

    #[test]
    fn an_axis_with_no_room_over_its_bound_is_refused() {
        let bounds = vec![Bound::new(0.25, "fair share 25.0%".to_string())];
        // Red: the axis the audit found -- 0..0.25, the bound itself, so a bar
        // over the share is clipped at the line the panel exists to watch.
        let problems = check_bound_headroom("shares", &bounds, &[], (0.0, 0.25), Some(228.0), None);
        assert!(!problems.is_empty(), "{problems:?}");
        assert!(
            problems.iter().any(|p| p.contains("over-bound bar")),
            "{problems:?}"
        );
        assert!(
            problems.iter().any(|p| p.contains("same picture")),
            "{problems:?}"
        );
        // Green: the automatic extent keeps the pixel floor above the bound.
        assert!(
            check_bound_headroom("shares", &bounds, &[], (0.0, 0.2601), Some(228.0), None)
                .is_empty()
        );
    }

    #[test]
    fn an_axis_label_carried_from_a_sibling_panel_is_refused() {
        let series = series_of("fraction", vec![(1.0, 0.958217), (2.0, 0.958271)]);
        // Red: the goodput panel's unit carried over the mandate's shared
        // y_label onto a single-series fraction panel.
        let problems = check_axis_label("fraction", "MiB/s", &series, None, "MiB/s");
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("MiB/s"), "{}", problems[0]);
        // Green: the label a single-series panel draws for itself, a panel
        // that states its own label, and a panel with several series.
        assert!(
            check_axis_label("fraction", "fraction of link rate", &series, None, "MiB/s")
                .is_empty()
        );
        assert!(check_axis_label("fraction", "MiB/s", &series, Some("MiB/s"), "MiB/s").is_empty());
        let several: Series = vec![
            ("delivered".to_string(), vec![]),
            ("shaper_forwarded".to_string(), vec![]),
        ];
        assert!(check_axis_label("goodput", "MiB/s", &several, None, "MiB/s").is_empty());
    }

    #[test]
    fn an_x_axis_that_contradicts_the_runs_categories_is_refused() {
        let run = pyjson::parse(r#"{"reps": 3, "measured_s": 18.0}"#).expect("parses");
        // Red: a panel drawing one bar per repetition (x=1..3) labelled `seed`.
        let problems = check_x_axis_label("fraction", "seed", &[1.0, 2.0, 3.0], Some(&run));
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("seed"), "{}", problems[0]);
        assert!(problems[0].contains("reps=3"), "{}", problems[0]);
        assert!(
            check_x_axis_label("fraction", "rep (1..3)", &[1.0, 2.0, 3.0], Some(&run)).is_empty()
        );
        // Green: categories that are not the run's repetitions, or a run with
        // no repetition count, keeps the declaration's label.
        assert!(check_x_axis_label("goodput", "seed", &[11.0, 21.0, 31.0], Some(&run)).is_empty());
        let fewer = pyjson::parse(r#"{"reps": 3}"#).expect("parses");
        assert!(check_x_axis_label("fraction", "seed", &[1.0, 2.0], Some(&fewer)).is_empty());
    }

    #[test]
    fn a_reading_the_panel_does_not_state_is_refused() {
        // A climb the window cut off: the shape the eye cannot tell from a
        // peak that returned, and the shape the run's own detector classified.
        let reading = pyjson::parse(
            r#"{"lone_tail": {"verdict": "Censored", "rungs_at_edge": 1.0, "room": 1200.0}}"#,
        )
        .expect("parses");
        let series = series_of(
            "lone_tail",
            (1..7)
                .map(|index| (index as f64 * 0.25, index as f64 * 300.0))
                .collect(),
        );
        let readings = panel_readings(&series, Some(&reading));
        assert_eq!(readings.len(), 1);
        // Red: a panel drawn without the run's reading is refused by name.
        let problems = check_readings_stated("latency", &series, &readings, "<svg></svg>");
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("Censored"), "{}", problems[0]);
        assert!(
            problems[0].contains("the opposite conclusion"),
            "{}",
            problems[0]
        );
    }

    #[test]
    fn a_reading_band_that_eats_the_plot_is_refused() {
        // Three arms of readings leave most of the plot; a band that leaves too
        // little is refused rather than drawn.
        assert!(check_reading_band("latency", draw::line_plot_height(3, 6)).is_empty());
        let problems = check_reading_band("latency", draw::line_plot_height(3, 20));
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("shape its readings are about"),
            "{}",
            problems[0]
        );
    }

    // -- coverage recovered from the deleted `tools/test_mandate_plot.py` -----
    //
    // The port to Rust left several check-level cases with no counterpart: the
    // CDF reference-reach, per-arm-governance, panel-summary-extent and
    // caption/reading-number families, the render-integration halves of the
    // sliver and clip families, and the two-sided-bound and axis-label cases.
    // Each test below starts from the artifact that satisfies the property and
    // then shows the artifact that violates it being refused, the way the
    // cases removed from the Python suite were written.

    fn run(values: &str) -> J {
        pyjson::parse(values).expect("parses")
    }

    fn panel_of(id: &str, chart: Chart, names: &[&str]) -> Panel {
        Panel {
            id: id.to_string(),
            chart,
            series: names
                .iter()
                .map(|name| SeriesEntry {
                    name: name.to_string(),
                    role: None,
                })
                .collect(),
            x_label: None,
            y_label: None,
            bounds: Vec::new(),
            y_extent: None,
        }
    }

    fn cdf_markup(series: &Series, scale: &'static str, note: &str) -> String {
        draw::svg_cdf_chart(&draw::LineChart {
            title: "M1 [cdf]",
            x_label: "latency (ms)",
            y_label: "percentile (%)",
            series,
            y_extent: None,
            bounds: &[],
            walls: false,
            markers: false,
            readings: &[],
            note,
            x_bounds: &[],
            x_scale: scale,
            y_clip: None,
        })
    }

    fn line_markup(series: &Series, bounds: &[(f64, String)]) -> String {
        draw::svg_line_chart(&draw::LineChart {
            title: "M1 [latency]",
            x_label: "elapsed time (s)",
            y_label: "latency (ms)",
            series,
            y_extent: None,
            bounds,
            walls: true,
            markers: true,
            readings: &[],
            note: "",
            x_bounds: &[],
            x_scale: "linear",
            y_clip: None,
        })
    }

    fn reading_markup(sentences: &[String]) -> String {
        let mut body = String::new();
        let mut row = 0usize;
        for sentence in sentences {
            for line in draw::wrap_label(sentence, draw::READING_PLOT_WIDTH as f64) {
                body.push_str(&format!(
                    "<text class=\"arm-reading\" x=\"76\" y=\"{}\">{}</text>",
                    24 + row * 13,
                    pyjson::escape(&line)
                ));
                row += 1;
            }
        }
        format!("<svg><g class=\"arm-readings\">{body}</g></svg>")
    }

    fn note_markup(note: &str) -> String {
        format!(
            "<svg><rect x=\"72\" y=\"24\" width=\"864\" height=\"228\" class=\"plot-bg\"/>\
             <text class=\"panel-note\" x=\"77\" y=\"60\">{}</text></svg>",
            pyjson::escape(note)
        )
    }

    fn summary_issues(
        markup: &str,
        series: &Series,
        bounds: &[Bound],
        extent: (f64, f64),
        fault: Option<&str>,
    ) -> Vec<String> {
        check_panel_summary_stated(
            "imbalance",
            Chart::Bar,
            "flow (1..4)",
            "departure from the fair share",
            series,
            bounds,
            extent,
            markup,
            draw::bar_plot_height(1) as f64,
            "",
            None,
            fault,
        )
    }

    fn the_m1_arms() -> Series {
        vec![
            (
                "clean".to_string(),
                vec![
                    (20.137, 0.0),
                    (85.316, 98.0),
                    (93.088, 99.0),
                    (107.674, 100.0),
                ],
            ),
            (
                "hostile".to_string(),
                vec![
                    (0.05, 0.0),
                    (213.408, 98.0),
                    (231.837, 99.0),
                    (277.114, 100.0),
                ],
            ),
            (
                "lone_tail".to_string(),
                vec![
                    (0.092417, 0.0),
                    (122.818459, 98.0),
                    (172.260084, 99.0),
                    (1567.110834, 100.0),
                ],
            ),
        ]
    }

    #[test]
    fn a_cdf_reference_arm_squeezed_to_a_sliver_is_refused_unless_the_panel_states_it() {
        // `M1-cdf` is read for where its reference arm's body sits; a linear
        // axis out to the worst arm's tail paints it as a sliver. The axis goes
        // logarithmic, and an axis that cannot is refused unless it says so.
        let series = the_m1_arms();
        let panel = panel_of("cdf", Chart::Cdf, &["clean", "hostile", "lone_tail"]);
        let guards = run(r#"{"hostile_p99_guard": 900.0, "lone_p99_guard": 3200.0,
                "lone_p999_guard": 8000.0}"#);
        let reference = reference_arm_names(&series, Some(&guards));
        assert_eq!(reference, vec!["clean".to_string()]);
        assert_eq!(cdf_x_scale(&series, &reference), "log");
        let linear = cdf_markup(&series, "linear", "");
        assert_eq!(drawn_x_scale(&linear), "linear");
        let problems = check_cdf_reference_reach("cdf", &panel, &series, &reference, &linear);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("6.9% of the width"), "{}", problems[0]);
        assert!(problems[0].contains("clean"), "{}", problems[0]);
        // Green: the logarithmic axis keeps the reference arm legible.
        let log = cdf_markup(&series, "log", "");
        assert_eq!(drawn_x_scale(&log), "log");
        assert!(check_cdf_reference_reach("cdf", &panel, &series, &reference, &log).is_empty());
        // Green: a squeezed axis that *states* the share is accepted, which is
        // the escape a frame that cannot show what it owes is allowed.
        let note = cdf_scale_note(&series, &reference, "linear");
        assert!(note.contains("7% of the width"), "{note}");
        assert!(note.contains("clean"), "{note}");
        let stated = note_markup(&note);
        assert!(check_cdf_reference_reach("cdf", &panel, &series, &reference, &stated).is_empty());
    }

    #[test]
    fn a_bar_bound_with_a_different_floor_per_arm_is_drawn_per_arm_and_named() {
        // The measured defect: `M2-delivery` drew the clean arm's `1.000` line
        // across all three arms while the run guards the other two at `0.995`.
        let panel = panel_of("delivery", Chart::Bar, &["delivery"]);
        let series = series_of("delivery", vec![(1.0, 1.0), (2.0, 1.0), (3.0, 1.0)]);
        let bounds = vec![Bound::new(1.0, "M2 delivery floor 1.000".to_string())];
        let values = run(
            r#"{"clean_delivery": 1.0, "hostile_delivery": 1.0, "lone_delivery": 1.0,
                "hostile_delivery_guard": 0.995, "lone_delivery_guard": 0.995,
                "delivery_floor": 0.995}"#,
        );
        let planned =
            crate::tools::mandate_plot::bar_bound_plan(&panel, &series, &bounds, Some(&values));
        assert!(planned.len() > bounds.len(), "{planned:?}");
        // Red: one line at the declared bound, naming no arm.
        let one_line = draw::svg_bar_chart(
            "M2 [delivery]",
            "arm",
            "delivery (received / offered)",
            &series,
            &bounds,
            None,
            Some(&values),
            "",
        );
        let problems = check_bound_arm_governance(
            "delivery",
            &panel,
            &series,
            &bounds,
            Some(&values),
            &one_line,
        );
        assert_eq!(problems.len(), 2, "{problems:?}");
        let joined = problems.join("\n");
        assert!(joined.contains("would be the floor of neither"), "{joined}");
        assert!(joined.contains("governs clean"), "{joined}");
        assert!(joined.contains("governs hostile lone"), "{joined}");
        // Green: each arm's own line, labelled with the arm it governs.
        let split = draw::svg_bar_chart(
            "M2 [delivery]",
            "arm",
            "delivery (received / offered)",
            &series,
            &planned,
            None,
            Some(&values),
            "",
        );
        assert!(
            check_bound_arm_governance("delivery", &panel, &series, &bounds, Some(&values), &split)
                .is_empty()
        );
    }

    #[test]
    fn a_line_bound_that_names_no_arm_is_refused() {
        // One ceiling across three arms whose own guards differ by more than
        // the ceiling itself: the label has to say which arm it governs.
        let panel = panel_of("latency", Chart::Line, &["clean", "hostile", "lone_tail"]);
        let series: Series = vec![
            ("clean".to_string(), vec![(1.0, 20.0), (2.0, 107.0)]),
            ("hostile".to_string(), vec![(1.0, 0.05), (2.0, 277.0)]),
            ("lone_tail".to_string(), vec![(1.0, 0.09), (2.0, 1567.1)]),
        ];
        let bounds = vec![Bound::new(250.0, "M1 ceiling 250 ms".to_string())];
        let values = run(
            r#"{"hostile_p99_guard": 900.0, "hostile_over250_guard": 8.0,
                "lone_p99_guard": 3200.0, "lone_over250_guard": 8.0}"#,
        );
        let bare = line_markup(&series, &[(250.0, "M1 ceiling 250 ms".to_string())]);
        let problems =
            check_bound_arm_governance("latency", &panel, &series, &bounds, Some(&values), &bare);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("the bound of none of them"),
            "{}",
            problems[0]
        );
        assert!(
            problems[0].contains("lone_p99_guard=3200"),
            "{}",
            problems[0]
        );
        // Green: the same line carrying the arm-guard clause.
        let label = governed_label(&bounds[0], &series, Some(&values), false);
        assert!(label.contains("governs clean"), "{label}");
        assert!(label.contains("lone_p99_guard=3200"), "{label}");
        let named = line_markup(&series, &[(250.0, label)]);
        assert!(
            check_bound_arm_governance("latency", &panel, &series, &bounds, Some(&values), &named)
                .is_empty()
        );
    }

    #[test]
    fn a_run_that_states_a_per_arm_bound_owes_a_line_over_that_arm() {
        // A split offered where the run states no bound of its own for the
        // panel's quantity would be an invention, not a reading.
        let panel = panel_of("delivery", Chart::Bar, &["delivery"]);
        let series = series_of("delivery", vec![(1.0, 1.0), (2.0, 1.0), (3.0, 1.0)]);
        let bounds = vec![Bound::new(1.0, "M2 delivery floor 1.000".to_string())];
        assert!(
            crate::tools::mandate_plot::arm_bound_values(&panel, &series, &bounds, None).is_none()
        );
        let unrelated = run(r#"{"flows": 4, "imbalance_bound": 0.01, "fair_share": 0.25,
                "delivery_floor": 0.995, "hostile_p99_guard": 900.0}"#);
        assert!(
            crate::tools::mandate_plot::arm_bound_values(
                &panel,
                &series,
                &bounds,
                Some(&unrelated)
            )
            .is_none()
        );
        assert_eq!(
            crate::tools::mandate_plot::effective_bounds(
                &panel,
                &series,
                &bounds,
                Some(&unrelated)
            ),
            bounds
        );
        // The run that *does* restate the floor for two of the three arms gets
        // a bound per arm, which is what the panel then has to draw.
        let restated = run(
            r#"{"clean_delivery": 1.0, "hostile_delivery": 1.0, "lone_delivery": 1.0,
                "hostile_delivery_guard": 0.995, "lone_delivery_guard": 0.995,
                "delivery_floor": 0.995}"#,
        );
        let plan =
            crate::tools::mandate_plot::arm_bound_values(&panel, &series, &bounds, Some(&restated))
                .expect("the run restates two arms");
        let per_arm: Vec<(&str, Vec<f64>)> = plan
            .arms
            .iter()
            .zip(plan.lines.iter())
            .map(|(arm, lines)| (arm.as_str(), lines.iter().map(|line| line.value).collect()))
            .collect();
        assert_eq!(
            per_arm,
            vec![
                ("clean", vec![1.0]),
                ("hostile", vec![0.995]),
                ("lone", vec![0.995]),
            ]
        );
    }

    #[test]
    fn a_guard_the_panel_names_without_a_line_is_refused() {
        // A tolerance the panel names and does not draw is a claim the reader
        // has to take on trust.
        let guards = vec![200.0, 400.0];
        let axis = (0.0, 450.0);
        let y_of = |value: f64| 24.0 + (axis.1 - value) / (axis.1 - axis.0) * 228.0;
        let line = |value: f64| {
            format!(
                "<line class=\"bound\" x1=\"72\" y1=\"{}\" x2=\"936\" y2=\"{}\"/>",
                y_of(value),
                y_of(value)
            )
        };
        let one = format!(
            "<svg><rect x=\"72\" y=\"24\" width=\"864\" height=\"228\" class=\"plot-bg\"/>{}</svg>",
            line(100.0)
        );
        let problems = check_named_guards_drawn("latency", &guards, axis, &one, Some(228.0));
        assert_eq!(problems.len(), 2, "{problems:?}");
        for value in ["200", "400"] {
            assert!(
                problems
                    .iter()
                    .any(|problem| problem.contains(&format!("names the guard {value}"))),
                "{problems:?}"
            );
        }
        // Green: the same panel with a line at each named guard.
        let all = format!(
            "<svg><rect x=\"72\" y=\"24\" width=\"864\" height=\"228\" class=\"plot-bg\"/>{}{}{}</svg>",
            line(100.0),
            line(200.0),
            line(400.0)
        );
        assert!(check_named_guards_drawn("latency", &guards, axis, &all, Some(228.0)).is_empty());
    }

    #[test]
    fn a_crossing_whose_arm_is_named_nowhere_is_refused() {
        // The lone tail crosses the ceiling while sitting inside its own arm's
        // guard; with no drawn line saying so, a pass and a breach are the same
        // picture.
        let series: Series = vec![
            (
                "clean".to_string(),
                (0..10).map(|i| (i as f64, 20.0 + i as f64)).collect(),
            ),
            (
                "hostile".to_string(),
                (0..10).map(|i| (i as f64, 100.0 + i as f64)).collect(),
            ),
            (
                "lone_tail".to_string(),
                (0..9)
                    .map(|i| (i as f64, 100.0 + i as f64))
                    .chain(std::iter::once((9.0, 1600.0)))
                    .collect(),
            ),
        ];
        let bounds = vec![Bound::new(250.0, "M1 ceiling 250 ms".to_string())];
        let values = run(
            r#"{"ceiling": 250.0, "clean_p99": 27.0, "hostile_p99_guard": 900.0,
                "hostile_over250_guard": 8.0, "lone_p99_guard": 3200.0,
                "lone_over250_guard": 8.0}"#,
        );
        assert_eq!(
            own_bound_names(Chart::Line, &series, Some(&values)),
            vec!["hostile".to_string(), "lone_tail".to_string()]
        );
        let bare = line_markup(&series, &[(250.0, "M1 ceiling 250 ms".to_string())]);
        let problems = check_crossing_series_governed(
            "latency",
            Chart::Line,
            &series,
            &bounds,
            Some(&values),
            &bare,
        );
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("'lone_tail'"), "{}", problems[0]);
        assert!(problems[0].contains("M1 ceiling 250 ms"), "{}", problems[0]);
        // Green: the clause naming the arm the crossing belongs to.
        let label = governed_label(&bounds[0], &series, Some(&values), false);
        let named = line_markup(&series, &[(250.0, label.clone())]);
        assert!(
            check_crossing_series_governed(
                "latency",
                Chart::Line,
                &series,
                &bounds,
                Some(&values),
                &named
            )
            .is_empty(),
            "{label}"
        );
    }

    #[test]
    fn a_summary_that_is_not_the_drawn_panel_is_refused() {
        // A summary carrying a number the run did not measure is worse than
        // silence, because the reader trusts it instead of the pixels.
        let series = series_of("clean", vec![(1.0, 0.000118), (2.0, 0.000118)]);
        let bounds = vec![Bound::new(0.01, "fair-share bound \u{b1}1.0%".to_string())];
        let extent = (0.0, 0.02);
        let markup = draw::svg_bar_chart(
            "M4 [imbalance]",
            "flow (1..4)",
            "departure from the fair share",
            &series,
            &bounds,
            Some(extent),
            None,
            "",
        );
        let document = panel_summary_document(
            "imbalance",
            Chart::Bar,
            "flow (1..4)",
            "departure from the fair share",
            &series,
            &bounds,
            extent,
            &markup,
            "",
            draw::bar_plot_height(1) as f64,
            None,
            None,
        );
        let with_summary = introduce_panel_summary(&markup, &document);
        assert!(
            summary_issues(&with_summary, &series, &bounds, extent, None).is_empty(),
            "the introduced summary is the drawn panel's"
        );
        // Red: the same panel with its `<desc>` removed.
        let problems = summary_issues(&markup, &series, &bounds, extent, None);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("carries no panel summary"),
            "{}",
            problems[0]
        );
        // Red: the x extent dropped from the summary.
        let mut no_x = document.clone();
        if let J::Obj(members) = &mut no_x {
            members.retain(|(key, _)| key != "x_axis");
        }
        let markup_no_x = introduce_panel_summary(&markup, &no_x);
        let problems = summary_issues(&markup_no_x, &series, &bounds, extent, None);
        assert!(
            problems.iter().any(|problem| problem.contains("'x_axis'")),
            "{problems:?}"
        );
        // Red: a summary that does not state the fault the panel took.
        let problems = summary_issues(&with_summary, &series, &bounds, extent, Some("M4_drop"));
        assert!(
            problems.iter().any(|problem| problem.contains("'fault'")),
            "{problems:?}"
        );
    }

    #[test]
    fn an_x_extent_that_is_not_the_drawn_axis_is_refused() {
        let series: Series = vec![(
            "impaired".to_string(),
            vec![(12.5, 33.3), (31.5, 66.7), (88.25, 100.0)],
        )];
        let markup = cdf_markup(&series, "linear", "");
        assert!(check_x_axis_extent_stated("cdf", Chart::Cdf, (12.5, 88.25), &markup).is_empty());
        let problems = check_x_axis_extent_stated("cdf", Chart::Cdf, (0.0, 100.0), &markup);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("states the x axis 0..100"),
            "{}",
            problems[0]
        );
        assert!(problems[0].contains("12.5"), "{}", problems[0]);
    }

    #[test]
    fn a_panel_that_cannot_draw_the_bound_says_where_it_is_drawn() {
        let mut latency = panel_of("latency", Chart::Line, &["impaired"]);
        latency.bounds = vec![Bound::new(250.0, "M1 ceiling 250 ms".to_string())];
        let cdf = panel_of("cdf", Chart::Cdf, &["impaired"]);
        let panels = vec![latency, cdf.clone()];
        let points = points_of(&[
            ("latency", "impaired", "0.0", "12.5"),
            ("latency", "impaired", "1.0", "31.5"),
            ("cdf", "impaired", "12.5", "33.3"),
            ("cdf", "impaired", "31.5", "66.7"),
        ]);
        let note =
            bound_reference_note(&cdf, &panels, &points, "elapsed time (s)", "RTT (ms)", None);
        assert!(note.contains("drawn on panel 'latency'"), "{note}");
        // Red: the panel drawn with no note at all.
        let bare = cdf_markup(
            &{
                let series: Series =
                    vec![("impaired".to_string(), vec![(12.5, 33.3), (31.5, 66.7)])];
                series
            },
            "linear",
            "",
        );
        let problems = check_departure_view_stated(
            "cdf",
            &cdf,
            &panels,
            &points,
            &bare,
            "elapsed time (s)",
            "RTT (ms)",
            None,
        );
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("draws no bound at all"),
            "{}",
            problems[0]
        );
        assert!(problems[0].contains("'latency'"), "{}", problems[0]);
        // Green: the note drawn, and inside the plot.
        let said = note_markup(&note);
        assert!(
            check_departure_view_stated(
                "cdf",
                &cdf,
                &panels,
                &points,
                &said,
                "elapsed time (s)",
                "RTT (ms)",
                None
            )
            .is_empty()
        );
        assert!(check_note_fit("cdf", &said).expect("reads").is_empty());
        // Red: the same note placed outside the plot area.
        let outside = format!(
            "<svg><rect x=\"72\" y=\"24\" width=\"864\" height=\"228\" class=\"plot-bg\"/>\
             <text class=\"panel-note\" x=\"10\" y=\"10\">{}</text></svg>",
            pyjson::escape(&note)
        );
        assert!(!check_note_fit("cdf", &outside).expect("reads").is_empty());
    }

    #[test]
    fn a_share_panel_without_its_departure_statement_is_refused() {
        let mut shares = panel_of("shares", Chart::Bar, &["clean", "hostile"]);
        shares.bounds = vec![Bound::new(0.25, "fair share 25.0%".to_string())];
        let mut imbalance = panel_of("imbalance", Chart::Bar, &["clean", "hostile"]);
        imbalance.bounds = vec![Bound::new(0.01, "fair-share bound \u{b1}1.0%".to_string())];
        let panels = vec![shares.clone(), imbalance.clone()];
        let points = points_of(&[
            ("shares", "clean", "1.0", "0.250029"),
            ("imbalance", "clean", "1.0", "0.000118"),
            ("shares", "clean", "2.0", "0.250029"),
            ("imbalance", "clean", "2.0", "0.000118"),
            ("shares", "clean", "3.0", "0.250029"),
            ("imbalance", "clean", "3.0", "0.000118"),
            ("shares", "clean", "4.0", "0.249912"),
            ("imbalance", "clean", "4.0", "-0.000353"),
            ("shares", "hostile", "1.0", "0.250029"),
            ("imbalance", "hostile", "1.0", "0.000114"),
            ("shares", "hostile", "2.0", "0.249914"),
            ("imbalance", "hostile", "2.0", "-0.000343"),
            ("shares", "hostile", "3.0", "0.250029"),
            ("imbalance", "hostile", "3.0", "0.000114"),
            ("shares", "hostile", "4.0", "0.250029"),
            ("imbalance", "hostile", "4.0", "0.000114"),
        ]);
        let note = departure_view_note(&shares, &panels, &points);
        assert!(note.contains("drawn on panel 'imbalance'"), "{note}");
        let bare_series = panel_series(&shares, &points);
        let bare = draw::svg_bar_chart(
            "M4 [shares]",
            "flow (1..4)",
            "share of the lane's delivered bytes",
            &bare_series,
            &shares.bounds,
            None,
            None,
            "",
        );
        let problems = check_departure_view_stated(
            "shares",
            &shares,
            &panels,
            &points,
            &bare,
            "flow (1..4)",
            "share of the lane's delivered bytes",
            None,
        );
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("cannot carry the failure its mandate is read for"),
            "{}",
            problems[0]
        );
        assert!(problems[0].contains("'imbalance'"), "{}", problems[0]);
        assert!(
            problems[0].contains("no failure to draw"),
            "{}",
            problems[0]
        );
        // Green: the same panel carrying its own note.
        let said = draw::svg_bar_chart(
            "M4 [shares]",
            "flow (1..4)",
            "share of the lane's delivered bytes",
            &bare_series,
            &shares.bounds,
            None,
            None,
            &note,
        );
        assert!(
            check_departure_view_stated(
                "shares",
                &shares,
                &panels,
                &points,
                &said,
                "flow (1..4)",
                "share of the lane's delivered bytes",
                None
            )
            .is_empty()
        );
    }

    #[test]
    fn a_caption_whose_numbers_come_from_another_source_is_refused() {
        // The caption is what the reader trusts instead of the pixels, so an
        // authoritative and wrong caption is worse than no caption.
        let series = series_of("lone_tail", vec![(0.0, 10.0), (1.0, 1000.0), (6.0, 20.0)]);
        let points = draw::decimate(&series[0].1);
        let drawn = arm_reading("lone_tail", &points, None);
        let good = reading_markup(std::slice::from_ref(&drawn));
        assert!(check_reading_numbers("latency", &series, &good).is_empty());
        // Red: the magnitude taken from somewhere else.
        let wrong = drawn.replace("peak 1000 ms", "peak 1074.1 ms");
        assert_ne!(wrong, drawn);
        let problems = check_reading_numbers("latency", &series, &reading_markup(&[wrong]));
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("'lone_tail'"), "{}", problems[0]);
        assert!(problems[0].contains("1074.1"), "{}", problems[0]);
        assert!(problems[0].contains("1000"), "{}", problems[0]);
        assert!(
            problems[0].contains("worse than no caption"),
            "{}",
            problems[0]
        );
        // The other numbers are pinned too, and a caption that states no
        // maximum at all is refused rather than skipped.
        for (broken, fragment) in [
            (
                drawn.replace("at 1.00 s", "at 9.56 s"),
                "where its maximum is",
            ),
            (
                drawn.replace("last 20 ms", "last 424.3 ms"),
                "its last sample",
            ),
            (
                drawn.replace("peak 1000 ms at 1.00 s", "the series is quiet"),
                "states no maximum",
            ),
        ] {
            let problems = check_reading_numbers(
                "latency",
                &series,
                &reading_markup(std::slice::from_ref(&broken)),
            );
            assert!(
                problems.iter().any(|problem| problem.contains(fragment)),
                "{fragment}: {problems:?} ({broken})"
            );
        }
    }

    #[test]
    fn a_caption_taken_from_another_arm_is_refused() {
        // The band is split by the arm's own marker, so a sentence that names
        // the wrong arm is measured against the wrong points.
        let series: Series = vec![
            (
                "first".to_string(),
                (1..8)
                    .map(|index| (index as f64, 10.0 * index as f64))
                    .collect(),
            ),
            (
                "second".to_string(),
                (1..8)
                    .map(|index| (index as f64, 100.0 * index as f64))
                    .collect(),
            ),
        ];
        let first = arm_reading("first", &series[0].1, None);
        let second = arm_reading("second", &series[1].1, None);
        assert!(
            check_reading_numbers(
                "latency",
                &series,
                &reading_markup(&[first, second.clone()])
            )
            .is_empty()
        );
        let mislabelled = reading_markup(&[second.replace("second - ", "first - ")]);
        let problems = check_reading_numbers("latency", &series, &mislabelled);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("'first'"), "{}", problems[0]);
        assert!(
            problems[0].contains("states its maximum as 700"),
            "{}",
            problems[0]
        );
        assert!(problems[0].contains("is 70"), "{}", problems[0]);
    }

    #[test]
    fn a_clipped_axis_must_state_the_clip_and_an_unclipped_one_is_refused() {
        let series: Series = vec![
            (
                "clean".to_string(),
                (0..200)
                    .map(|i| (i as f64, 20.0 + (i % 20) as f64))
                    .collect(),
            ),
            (
                "hostile".to_string(),
                (0..200)
                    .map(|i| (i as f64, 40.0 + 2.0 * (i % 20) as f64))
                    .collect(),
            ),
            (
                "lone_tail".to_string(),
                (0..200)
                    .map(|i| (i as f64, 10.0 + (i % 20) as f64))
                    .chain(std::iter::once((199.0, 1400.0)))
                    .collect(),
            ),
        ];
        let bounds = vec![Bound::new(250.0, "M1 ceiling 250 ms".to_string())];
        let plot_height = draw::line_plot_height(3, 0);
        let clipped = line_axis_extent(&series, &bounds, None, Some(plot_height));
        assert!(clipped.1 < 1400.0, "{clipped:?}");
        let unclipped =
            draw::extent_including_bounds(draw::finite_extent(&series), &[(250.0, String::new())]);
        let statement = y_clip_statement(&series, Some(250.0));
        assert!(!statement.is_empty());
        let stated = format!(
            "<svg><rect x=\"72\" y=\"24\" width=\"864\" height=\"228\" class=\"plot-bg\"/>\
             <line class=\"y-clip\" x1=\"72\" y1=\"26\" x2=\"936\" y2=\"26\"/>\
             <text class=\"panel-note\" x=\"77\" y=\"60\">{}</text></svg>",
            pyjson::escape(&statement)
        );
        assert!(
            check_line_axis_clip_stated(
                "latency",
                Chart::Line,
                &series,
                &bounds,
                clipped,
                &stated,
                None
            )
            .is_empty()
        );
        // Red: the clipped axis drawn without the sentence that says so.
        let silent = stated
            .replace(
                "<line class=\"y-clip\" x1=\"72\" y1=\"26\" x2=\"936\" y2=\"26\"/>",
                "",
            )
            .replace(
                &format!(
                    "<text class=\"panel-note\" x=\"77\" y=\"60\">{}</text>",
                    pyjson::escape(&statement)
                ),
                "",
            );
        let problems = check_line_axis_clip_stated(
            "latency",
            Chart::Line,
            &series,
            &bounds,
            clipped,
            &silent,
            None,
        );
        assert!(!problems.is_empty(), "{problems:?}");
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("does not state it")),
            "{problems:?}"
        );
        // Red: the axis the old policy drew -- the data's own extent, outlier
        // and all.
        let problems = check_line_axis_clip_stated(
            "latency",
            Chart::Line,
            &series,
            &bounds,
            unclipped,
            &stated,
            None,
        );
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("one outlier set the axis"),
            "{}",
            problems[0]
        );
        assert!(problems[0].contains("1400"), "{}", problems[0]);
    }

    #[test]
    fn a_sliver_bound_must_be_stated_or_it_is_refused() {
        let series: Series = vec![
            (
                "clean".to_string(),
                vec![(1.0, -1.0), (2.0, -1.0), (3.0, -1.0), (4.0, -1.0)],
            ),
            (
                "hostile".to_string(),
                vec![
                    (1.0, 0.000116),
                    (2.0, 0.000116),
                    (3.0, -0.000347),
                    (4.0, 0.000116),
                ],
            ),
        ];
        let bounds = vec![Bound::new(0.01, "fair-share bound \u{b1}1.0%".to_string())];
        let mut drawn = crate::tools::mandate_plot::mirrored_bounds(&bounds);
        let axis = draw::bar_axis_extent(&series, &drawn, None, None);
        let problems = check_panel_axis("imbalance", &series, &drawn, axis, Some(228.0), None, "");
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("sub-pixel"), "{}", problems[0]);
        // Red: the panel that draws the sliver and states nothing.
        let silent = draw::svg_bar_chart(
            "M4 [imbalance]",
            "flow (1..4)",
            "departure from the fair share",
            &series,
            &drawn,
            Some(axis),
            None,
            "",
        );
        let problems = check_sliver_bound_stated(
            "imbalance",
            &series,
            &drawn,
            axis,
            &silent,
            Some(228.0),
            None,
        );
        assert!(!problems.is_empty(), "{problems:?}");
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("states nothing")),
            "{problems:?}"
        );
        // Green: the sentence measured off the same series answers the refusal.
        let statements = sliver_bound_statements(&series, &mut drawn, axis, 228.0, None);
        assert!(!statements.is_empty(), "a departure is drawn");
        let sentence = statements
            .iter()
            .map(|(_, sentence)| sentence.clone())
            .collect::<Vec<String>>()
            .join("; ");
        let stated = note_markup(&sentence);
        assert!(
            check_sliver_bound_stated(
                "imbalance",
                &series,
                &drawn,
                axis,
                &stated,
                Some(228.0),
                None
            )
            .is_empty()
        );
        // Red: a stated distance that is not the run's.
        let number_before = crate::tools::mandate_plot::regex(r"([-0-9.]+) px from the bound");
        let tampered = number_before.replace_all(&sentence, "1.0 px from the bound");
        assert_ne!(tampered, sentence);
        let problems = check_sliver_bound_stated(
            "imbalance",
            &series,
            &drawn,
            axis,
            &note_markup(&tampered),
            Some(228.0),
            None,
        );
        assert!(
            problems.iter().any(|problem| problem.contains("far_px=1")),
            "{problems:?}"
        );
    }

    #[test]
    fn a_band_view_whose_ticks_repeat_is_refused() {
        let repeated: String = ["0.01", "0.01", "0.01", "0.00", "0.00", "0.00"]
            .iter()
            .map(|tick| format!("<text x=\"63\" y=\"0\" text-anchor=\"end\">{tick}</text>"))
            .collect();
        let problems = check_tick_labels_distinct("imbalance", &repeated);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("repeat"), "{}", problems[0]);
        assert!(
            problems[0].contains("cannot carry the quantity"),
            "{}",
            problems[0]
        );
        let distinct: String = ["-0.01", "-0.005", "0.00", "0.005", "0.01", "0.015"]
            .iter()
            .map(|tick| format!("<text x=\"63\" y=\"0\" text-anchor=\"end\">{tick}</text>"))
            .collect();
        assert!(check_tick_labels_distinct("imbalance", &distinct).is_empty());
    }

    #[test]
    fn a_two_sided_bound_is_drawn_on_both_of_its_sides() {
        let series: Series = vec![
            (
                "clean".to_string(),
                vec![
                    (1.0, 0.000118),
                    (2.0, 0.000118),
                    (3.0, 0.000118),
                    (4.0, -0.000353),
                ],
            ),
            (
                "hostile".to_string(),
                vec![
                    (1.0, 0.000114),
                    (2.0, -0.000343),
                    (3.0, 0.000114),
                    (4.0, 0.000114),
                ],
            ),
        ];
        let bounds = vec![Bound::new(0.01, "fair-share bound \u{b1}1.0%".to_string())];
        let mirrored = crate::tools::mandate_plot::mirrored_bounds(&bounds);
        assert_eq!(mirrored.len(), 2, "{mirrored:?}");
        let axis = draw::bar_axis_extent(&series, &mirrored, None, None);
        let markup = draw::svg_bar_chart(
            "M4 [imbalance]",
            "flow (1..4)",
            "departure from the fair share",
            &series,
            &mirrored,
            Some(axis),
            None,
            "",
        );
        assert!(
            check_two_sided_bound_drawn("imbalance", &mirrored, axis, &markup, Some(228.0))
                .is_empty()
        );
        // Green: the lower arm keeps its own pixel of headroom below it.
        assert!(
            check_bound_headroom(
                "imbalance",
                &mirrored,
                &[],
                axis,
                Some(228.0),
                Some(&series)
            )
            .is_empty()
        );
        let below = (-0.01 - axis.0) / (axis.1 - axis.0) * 228.0;
        assert!(below >= MIN_HEADROOM_PIXELS, "{below}");
        // Red: the axis flush with the lower arm draws breach and arm alike.
        let flush = (-0.01, axis.1);
        let problems = check_bound_headroom(
            "imbalance",
            &mirrored,
            &[],
            flush,
            Some(228.0),
            Some(&series),
        );
        assert!(!problems.is_empty(), "{problems:?}");
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("below the bound")),
            "{problems:?}"
        );
        // Red: the one-sided artifact the run actually drew, with only the
        // upper arm of the band drawn.
        let upper_only = vec![mirrored[0].clone()];
        let one_sided = draw::svg_bar_chart(
            "M4 [imbalance]",
            "flow (1..4)",
            "departure from the fair share",
            &series,
            &upper_only,
            Some(axis),
            None,
            "",
        );
        let problems =
            check_two_sided_bound_drawn("imbalance", &mirrored, axis, &one_sided, Some(228.0));
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("draws no line at -0.01"),
            "{}",
            problems[0]
        );
        assert!(problems[0].contains("crossing nothing"), "{}", problems[0]);
        // Red: a declaration whose band is drawn as a single line is refused.
        let one_sided_bounds = vec![bounds[0].clone()];
        let single = draw::svg_bar_chart(
            "M4 [imbalance]",
            "flow (1..4)",
            "departure from the fair share",
            &series,
            &one_sided_bounds,
            Some(axis),
            None,
            "",
        );
        let problems =
            check_two_sided_bound_drawn("imbalance", &one_sided_bounds, axis, &single, Some(228.0));
        assert!(!problems.is_empty(), "{problems:?}");
    }

    #[test]
    fn every_drawn_panel_keeps_its_bound_labels_inside_the_plot() {
        let series = bars_of("delivery", vec![(1.0, 1.0), (2.0, 1.0), (3.0, 1.0)]);
        let bounds = vec![Bound::new(1.0, "M2 delivery floor 1.000".to_string())];
        let markup = draw::svg_bar_chart(
            "M2 [delivery]",
            "arm",
            "delivery",
            &series,
            &bounds,
            None,
            None,
            "",
        );
        assert!(
            check_label_fit("delivery", &markup)
                .expect("reads")
                .is_empty()
        );
        let (left, top, right, bottom) = panel_plot_rect("delivery", &markup).expect("reads");
        let boxes = label_boxes(&markup);
        assert!(!boxes.is_empty(), "the panel drew no bound label");
        for (declared, _, (x0, y0, x1, y1)) in &boxes {
            assert!(*x0 >= left, "{declared}");
            assert!(*x1 <= right, "{declared}");
            assert!(*y0 >= top, "{declared}");
            assert!(*y1 <= bottom, "{declared}");
        }
        // A bound at the top of its axis is labelled *below* its line rather
        // than escaping into the legend.
        let bound_y = drawn_bound_lines(&markup)[0];
        for (declared, _, (_, y0, _, _)) in &boxes {
            assert!(*y0 > bound_y, "{declared} is drawn above its own bound");
        }
        // Red: the same markup with its own label moved to the canvas origin.
        let found = crate::tools::mandate_plot::regex_dotall(r#"<text class="bound-label"[^>]*>"#)
            .search(&markup)
            .expect("the panel draws a bound label")
            .group(0)
            .unwrap_or_default();
        let misplaced = markup.replacen(
            &found,
            "<text class=\"bound-label\" x=\"0.0\" y=\"0.0\">",
            1,
        );
        assert_ne!(misplaced, markup);
        assert!(
            !check_label_fit("delivery", &misplaced)
                .expect("reads")
                .is_empty()
        );
    }

    #[test]
    fn the_axis_policy_spends_the_pixel_floor_and_keeps_a_zero_baseline() {
        // The span's own 5% of headroom is a third of a pixel on a fair share
        // pinned at 25%, so the pixel floor is what keeps an over-share bar
        // drawable.
        let (_, high) = draw::axis_with_headroom(0.0, 0.250029, 0.25, 228.0, None);
        assert!(
            (high - 0.25) / high * 228.0 >= MIN_HEADROOM_PIXELS,
            "{high}"
        );
        assert!(high > 0.25 + FRAME_HEADROOM * 0.25, "{high}");
        // The downward-failing side of the same rule.
        let (low, high) = draw::axis_with_headroom(-0.01, 0.01, 0.01, 228.0, Some(-0.01));
        assert!(
            (-0.01 - low) / (high - low) * 228.0 >= MIN_HEADROOM_PIXELS,
            "{low}"
        );
        // A floor far below the data is not the scale, so the axis keeps the
        // zero baseline it had.
        let series = series_of("fraction", vec![(1.0, 0.958217), (2.0, 0.958271)]);
        let bounds = vec![Bound::new(0.35, "M3 floor 0.35x link rate".to_string())];
        assert_eq!(draw::bar_axis_extent(&series, &bounds, None, None).0, 0.0);
    }

    #[test]
    fn the_label_width_model_does_not_underestimate_the_rendered_text() {
        // The widths are the labels headless Chrome measured on the panels a
        // recorded run drew; the model is a *model*, so the only thing that
        // keeps it honest is a check that fails when it underestimates.
        for (label, measured) in [
            ("M4 per-flow delivery floor 0.995", 144.94),
            ("M1 ceiling 250 ms", 82.77),
            ("fair-share bound \u{b1}1.0%", 103.94),
            ("fair share 25.0%", 72.36),
        ] {
            assert!(
                draw::label_text_width(label) >= measured,
                "{label}: {} < {measured}",
                draw::label_text_width(label)
            );
        }
        // Vacuity: a model narrowed below the measured widths fails the same
        // assertion, so the calibration is about the model and not a tautology.
        let narrowed = |text: &str| -> f64 { draw::label_text_width(text) * 0.5 };
        assert!(narrowed("M4 per-flow delivery floor 0.995") < 144.94);
        assert_eq!(draw::label_text_width(""), 0.0);
    }

    #[test]
    fn an_x_bound_without_its_reading_is_refused() {
        // A mandate bound carried to a panel whose own x axis is that quantity
        // has to say what each curve reads there; a mark with no reading is a
        // claim with no value.
        let mut latency = panel_of("latency", Chart::Line, &["clean", "lone_tail"]);
        latency.y_label = Some("latency (ms)".to_string());
        latency.bounds = vec![Bound::new(250.0, "M1 ceiling 250 ms".to_string())];
        let mut cdf = panel_of("cdf", Chart::Cdf, &["clean", "lone_tail"]);
        cdf.x_label = Some("latency (ms)".to_string());
        cdf.y_label = Some("percentile (%)".to_string());
        let panels = vec![latency, cdf.clone()];
        let points = points_of(&[
            ("latency", "clean", "1.0", "20.0"),
            ("latency", "lone_tail", "1.0", "1567.1"),
            ("cdf", "clean", "107.674", "100.0"),
            ("cdf", "lone_tail", "1567.110834", "100.0"),
            ("cdf", "clean", "20.137", "0.0"),
            ("cdf", "lone_tail", "0.092417", "0.0"),
        ]);
        let series = panel_series(&cdf, &points);
        let derived =
            derived_x_bounds(&cdf, &panels, &points, "elapsed time (s)", "RTT (ms)", None);
        assert_eq!(derived.len(), 1, "{derived:?}");
        let reading = x_bound_label(&derived[0], &series, "ms", "%", None);
        assert!(reading.contains("at 250 ms:"), "{reading}");
        let draw_cdf = |label: &str| {
            draw::svg_cdf_chart(&draw::LineChart {
                title: "M1 [cdf]",
                x_label: "latency (ms)",
                y_label: "percentile (%)",
                series: &series,
                y_extent: None,
                bounds: &[],
                walls: false,
                markers: false,
                readings: &[],
                note: "",
                x_bounds: &[(250.0, label.to_string())],
                x_scale: "log",
                y_clip: None,
            })
        };
        let green = check_x_bound_drawn(
            "cdf",
            &cdf,
            &panels,
            &points,
            "elapsed time (s)",
            "RTT (ms)",
            None,
            &draw_cdf(&reading),
        )
        .expect("reads");
        assert!(green.is_empty(), "{green:?} reading={reading}");
        // Red: the mark drawn, its label carrying no reading.
        let problems = check_x_bound_drawn(
            "cdf",
            &cdf,
            &panels,
            &points,
            "elapsed time (s)",
            "RTT (ms)",
            None,
            &draw_cdf("M1 ceiling 250 ms"),
        )
        .expect("reads");
        assert!(!problems.is_empty(), "{problems:?}");
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("has to say what the bound reads there")),
            "{problems:?}"
        );
    }

    #[test]
    fn a_bound_the_bars_split_around_is_a_target_and_a_minority_beyond_one_is_a_crossing() {
        let shares = series_of(
            "clean",
            vec![
                (1.0, 0.250059),
                (2.0, 0.250059),
                (3.0, 0.249941),
                (4.0, 0.249941),
                (5.0, 0.250173),
                (6.0, 0.249365),
                (7.0, 0.250289),
                (8.0, 0.250173),
            ],
        );
        let mut panel = panel_of("shares", Chart::Bar, &["clean"]);
        panel.bounds = vec![Bound::new(0.25, "fair share 25.0%".to_string())];
        assert_eq!(target_bounds(&panel, &shares).len(), 1);
        let latency = series_of("p99_ms", vec![(1.0, 26.251), (2.0, 61.5), (3.0, 185.8015)]);
        let mut latency_panel = panel_of("latency", Chart::Bar, &["p99_ms"]);
        latency_panel.bounds = vec![Bound::new(
            100.0,
            "M2 non-degrading p99 bound (ms)".to_string(),
        )];
        assert!(target_bounds(&latency_panel, &latency).is_empty());
        assert_eq!(
            crate::tools::mandate_plot::crossing_values(&[26.251, 61.5, 185.8015], 100.0),
            vec![185.8015]
        );
    }
}
