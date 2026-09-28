//! Building one mandate's panels, writing them, and the command line.
//!
//! This is the half of the ported tool that owns the *output*: it renders each
//! declared panel, refuses it if any check fires, writes the standalone SVG
//! with the panel's summary inside it and beside it, and (unless told not to)
//! rasterizes it through a headless browser -- the one step that needs a
//! browser, and the one step whose absence is an error rather than an empty
//! file to skim past.
//!
//! The SVG extraction and validation, and the browser step, are ported from
//! `tools/render_graph.py`; what is not ported is that module's own `main` and
//! its `render_panels` entry point, which serve the perf loop's comparison HTML
//! rather than the mandate plotter.

use std::path::{Path, PathBuf};

use super::checks;
use super::checks::*;
use super::draw;
use super::*;

// -- the SVG panels of a written document ------------------------------------

/// The text of every `<svg ...>` panel of a document, in order.
pub fn extract_svg_panels(html: &str) -> Vec<String> {
    let chars: Vec<char> = html.chars().collect();
    let open = regex(r"<svg\b[^>]*>");
    let close_tag = "</svg>";
    let mut panels = Vec::new();
    let mut position = 0usize;
    while let Some((start, end_open)) = open.search_span_in(&chars, position) {
        let Some(close) = find_needle(&chars, close_tag, start) else {
            panels.push(chars[start..].iter().collect());
            break;
        };
        let next_open = open.search_span_in(&chars, end_open);
        if let Some((next_start, _)) = next_open
            && next_start < close
        {
            panels.push(chars[start..next_start].iter().collect());
            position = next_start;
            continue;
        }
        panels.push(
            chars[start..close + close_tag.chars().count()]
                .iter()
                .collect(),
        );
        position = close + close_tag.chars().count();
    }
    panels
}

fn find_needle(chars: &[char], needle: &str, from: usize) -> Option<usize> {
    let needle: Vec<char> = needle.chars().collect();
    if needle.is_empty() || chars.len() < needle.len() {
        return None;
    }
    (from..=chars.len() - needle.len())
        .find(|start| chars[*start..*start + needle.len()] == needle[..])
}

fn polyline_point_count(points: &str) -> usize {
    points
        .split_whitespace()
        .filter(|point| !point.is_empty())
        .count()
}

/// Whether this `<rect>` is a bar that would paint something.
fn rect_is_drawable(rect: &str) -> bool {
    if rect.contains("plot-bg") {
        return false;
    }
    // `render_graph`'s pattern uses a lookbehind; requiring the whitespace as
    // part of the match is the same set of matches, and the port's engine has
    // no lookbehind to offer.
    let dimensions: Vec<(String, String)> = regex(r#"\s(width|height)="([^"]*)""#)
        .find_all(rect)
        .iter()
        .map(|pair| {
            (
                pair[0].clone().unwrap_or_default(),
                pair[1].clone().unwrap_or_default(),
            )
        })
        .collect();
    let lookup = |key: &str| {
        dimensions
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    };
    let (Some(width), Some(height)) = (lookup("width"), lookup("height")) else {
        return false;
    };
    let (Ok(width), Ok(height)) = (width.parse::<f64>(), height.parse::<f64>()) else {
        return false;
    };
    width.is_finite() && height.is_finite() && width > 0.0 && height > 0.0
}

/// Count drawable data series in one SVG panel.
pub fn panel_series_count(panel: &str) -> usize {
    let mut series = 0usize;
    for groups in regex(r#"<polyline\b[^>]*\bpoints="([^"]*)""#).find_all(panel) {
        if polyline_point_count(&groups[0].clone().unwrap_or_default()) >= MIN_POLYLINE_POINTS {
            series += 1;
        }
    }
    for groups in regex(r#"<path\b[^>]*\bd="([^"]*)""#).find_all(panel) {
        if !groups[0].clone().unwrap_or_default().trim().is_empty() {
            series += 1;
        }
    }
    for groups in regex(r"<rect\b[^>]*>").find_all_whole(panel) {
        if rect_is_drawable(&groups) {
            series += 1;
        }
    }
    series
}

/// Why a panel that draws no series geometry is empty, read out of the panel's
/// own summary rather than assumed: a producer that emitted no rows and one
/// that emitted a measured zero are different failures and must not be told
/// apart by the same sentence.
fn seriesless_reason(panel: &str) -> String {
    let Some(summary) = checks::read_panel_summary(panel) else {
        return "the panel carries no panel summary, so nothing states what the \
                producer emitted for it"
            .to_string();
    };
    let entries = summary.get("series").and_then(J::as_arr).unwrap_or(&[]);
    let points: i64 = entries
        .iter()
        .map(|entry| entry.get("points").and_then(J::as_i64).unwrap_or(0))
        .sum();
    if entries.is_empty() || points == 0 {
        return format!(
            "the producer emitted no rows for it at all ({} series declared, {points} \
             points drawn): there is nothing to plot",
            entries.len()
        );
    }
    let all_zero = entries.iter().all(|entry| {
        entry
            .get("min")
            .and_then(J::as_f64)
            .map(|value| value == 0.0)
            .unwrap_or(false)
            && entry
                .get("max")
                .and_then(J::as_f64)
                .map(|value| value == 0.0)
                .unwrap_or(false)
    });
    if all_zero {
        return format!(
            "the producer emitted rows and every one of the {points} points is 0, a \
             measured zero (perfect fairness on this quantity, not an absence), and \
             the panel drew no mark for it: a value at the baseline is drawn as a \
             hollow floor mark there, so a panel without one is a rendering failure"
        );
    }
    format!(
        "the producer emitted {points} point(s) and the panel drew no geometry for any \
         of them"
    )
}

/// The problems that make this panel unusable as evidence.
pub fn validate_panel(index: usize, panel: &str) -> Vec<String> {
    let mut problems = Vec::new();
    let openings = regex(r"<svg\b[^>]*>").find_all(panel).len();
    if !panel.contains("</svg>") {
        problems.push(format!(
            "panel {index}: missing </svg> close tag (truncated panel)"
        ));
    } else if openings != 1 {
        problems.push(format!(
            "panel {index}: {openings} <svg> opening tags; a panel must be one SVG \
             document, so a missing close concatenated the next panel"
        ));
    }
    if panel_series_count(panel) == 0 {
        problems.push(format!(
            "panel {index}: no drawn series geometry ({}), and an empty chart is not a \
             graph",
            seriesless_reason(panel)
        ));
    }
    problems
}

/// Make one embedded panel a self-contained SVG document.
pub fn standalone_panel(panel: &str, width: i64, height: i64) -> String {
    let Some((_, end)) = regex(r"<svg\b[^>]*>").match_span(panel) else {
        return panel.to_string();
    };
    let chars: Vec<char> = panel.chars().collect();
    let mut open_tag: String = chars[..end].iter().collect();
    if !open_tag.contains("xmlns") {
        open_tag = open_tag.replacen("<svg ", "<svg xmlns=\"http://www.w3.org/2000/svg\" ", 1);
    }
    open_tag = regex(r#"\s+width="[^"]*""#).replace_all(&open_tag, "");
    open_tag = regex(r#"\s+height="[^"]*""#).replace_all(&open_tag, "");
    open_tag = open_tag.replacen(
        "<svg ",
        &format!("<svg width=\"{width}\" height=\"{height}\" "),
        1,
    );
    let mut out = open_tag;
    out.push_str(STANDALONE_STYLE);
    out.extend(chars[end..].iter());
    out
}

/// A self-contained SVG document carrying its own title.
fn standalone(markup: &str, title: &str) -> String {
    let document = standalone_panel(markup, draw::WIDTH, draw::HEIGHT);
    let marker = "</style>";
    let Some(index) = document.find(marker) else {
        return document;
    };
    let cut = index + marker.len();
    format!(
        "{}{}<title>{}</title>{}",
        &document[..cut],
        "",
        pyjson::escape(title),
        &document[cut..]
    )
}

// -- the browser step ---------------------------------------------------------

const BROWSER_ENV: &str = "NETEM_RENDER_BROWSER";
const BROWSER_APP_PATHS: [&str; 2] = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
];
const BROWSER_NAMES: [&str; 4] = ["google-chrome", "chromium", "chromium-browser", "chrome"];
const PNG_SIGNATURE: [u8; 8] = [0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n'];

/// Resolve a headless browser executable, or `None`.
fn find_browser(explicit: Option<&str>) -> Option<String> {
    if let Some(explicit) = explicit {
        let candidate = Path::new(explicit);
        if candidate.is_file() {
            return Some(candidate.display().to_string());
        }
        return which(explicit);
    }
    if let Ok(env) = std::env::var(BROWSER_ENV)
        && !env.is_empty()
    {
        return find_browser(Some(&env));
    }
    for path in BROWSER_APP_PATHS {
        if Path::new(path).is_file() {
            return Some(path.to_string());
        }
    }
    BROWSER_NAMES.iter().find_map(|name| which(name))
}

/// `shutil.which`, over `PATH`.
fn which(name: &str) -> Option<String> {
    if name.contains('/') {
        return Path::new(name).is_file().then(|| name.to_string());
    }
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .map(|candidate| candidate.display().to_string())
}

/// `(width, height)` for a PNG byte string, else `None`.
fn png_dimensions(data: &[u8]) -> Option<(u32, u32)> {
    if data.len() < 24 || !data.starts_with(&PNG_SIGNATURE) {
        return None;
    }
    let width = u32::from_be_bytes([data[16], data[17], data[18], data[19]]);
    let height = u32::from_be_bytes([data[20], data[21], data[22], data[23]]);
    Some((width, height))
}

/// Best-effort headless screenshot of one standalone SVG file.
fn rasterize_svg(
    browser: &str,
    svg_path: &Path,
    png_path: &Path,
    width: i64,
    height: i64,
    timeout: std::time::Duration,
) -> Option<String> {
    let uri = absolute_uri(svg_path);
    let mut command = std::process::Command::new(browser);
    command
        .arg("--headless")
        .arg("--disable-gpu")
        .arg("--hide-scrollbars")
        .arg(format!("--screenshot={}", png_path.display()))
        .arg(format!("--window-size={width},{height}"))
        .arg(uri)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let child = match command.spawn() {
        Ok(child) => child,
        Err(error) => return Some(format!("the browser could not be executed: {error}")),
    };
    let deadline = std::time::Instant::now() + timeout;
    let mut child = child;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return None,
            Ok(None) => {}
            Err(error) => return Some(format!("the browser could not be executed: {error}")),
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Some(format!(
                "the browser did not finish within {}s",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

/// A `file://` URI for a path, without the `percent-encoding` the standard
/// library does not need to supply for the paths this tool writes.
fn absolute_uri(path: &Path) -> String {
    let absolute = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    format!("file://{}", absolute.display())
}

/// Rasterize already-written, already-verified standalone SVG panels.
fn rasterize_panels(
    svg_paths: &[String],
    browser: Option<&str>,
    width: i64,
    height: i64,
) -> PlotResult<J> {
    let resolved = find_browser(browser);
    let Some(resolved) = resolved else {
        let directory = svg_paths
            .first()
            .map(|path| {
                Path::new(path)
                    .parent()
                    .map(|parent| parent.display().to_string())
                    .unwrap_or_else(|| ".".to_string())
            })
            .unwrap_or_else(|| ".".to_string());
        return fail(format!(
            "cannot rasterize: no headless browser found, so the PNG step cannot run. \
             The {} SVG panel(s) were written and verified under {directory}. Set \
             {BROWSER_ENV} or pass --browser, or pass --no-rasterize to accept \
             SVG-only evidence explicitly.",
            svg_paths.len()
        ));
    };
    let mut png: Vec<J> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for svg_path in svg_paths {
        let svg = PathBuf::from(svg_path);
        let png_path = svg.with_extension("png");
        if let Some(problem) = rasterize_svg(
            &resolved,
            &svg,
            &png_path,
            width,
            height,
            std::time::Duration::from_secs(120),
        ) {
            failures.push(format!("{}: {problem}", png_path.display()));
            continue;
        }
        let data = std::fs::read(&png_path).unwrap_or_default();
        match png_dimensions(&data) {
            None => failures.push(format!(
                "{}: the browser did not produce a valid PNG ({} bytes)",
                png_path.display(),
                data.len()
            )),
            Some((0, _)) | Some((_, 0)) => failures.push(format!(
                "{}: the browser produced a degenerate PNG",
                png_path.display()
            )),
            Some(_) => png.push(J::Str(png_path.display().to_string())),
        }
    }
    if !failures.is_empty() {
        return fail(format!(
            "rasterization failed; the SVGs were verified but the PNG step cannot be \
             trusted:\n  {}",
            failures.join("\n  ")
        ));
    }
    Ok(J::Obj(vec![
        ("browser".to_string(), J::Str(resolved)),
        ("png".to_string(), J::Arr(png)),
    ]))
}

// -- one panel's markup -------------------------------------------------------

/// The fault selector that belongs to this mandate, or `None`.
fn mandate_fault(mandate: &str, fault: Option<&str>) -> Option<String> {
    let fault = fault?;
    let value = fault.trim();
    if value.is_empty() {
        return None;
    }
    if value == mandate || value.starts_with(&format!("{mandate}_")) {
        return Some(value.to_string());
    }
    None
}

/// Markup for one declared panel, as exactly one `<svg>` document span.
#[allow(clippy::too_many_arguments)]
pub fn panel_markup(
    title: &str,
    x_label: &str,
    y_label: &str,
    panel: &Panel,
    points: &Points,
    run_values: Option<&J>,
    run_censoring: Option<&J>,
    panels: &[Panel],
    fault: Option<&str>,
) -> PlotResult<String> {
    let chart = panel.chart;
    let chart_title = format!("{} [{}]", title, panel.id);
    let series = panel_series(panel, points);
    let panel_x_label = panel_x_label_for(
        panel,
        x_label,
        &series
            .iter()
            .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
            .collect::<Vec<f64>>(),
        run_values,
    );
    let panel_y_label = panel_y_label_for(panel, y_label, &series);
    let bounds = bound_specs(panel);
    let mut drawn_bounds = drawable_bounds(panel, &series, &bounds, run_values);
    let pinned = panel.y_extent;
    let y_clip = if chart == Chart::Line {
        line_axis_clip(&series, &drawn_bounds, pinned)
    } else {
        None
    };
    let readings = if chart == Chart::Line {
        panel_readings(&series, run_censoring)
    } else {
        Vec::new()
    };
    let reading_rows: usize = draw::reading_lines(&readings, None)
        .iter()
        .map(Vec::len)
        .sum();
    let x_bounds = derived_x_bounds(panel, panels, points, x_label, y_label, run_values);
    let mut note = composition_note(panel, panels, points, x_label, y_label, run_values);
    let reference_arms = if chart == Chart::Cdf {
        reference_arm_names(&series, run_values)
    } else {
        Vec::new()
    };
    let x_scale = if reference_arms.is_empty() {
        "linear"
    } else {
        cdf_x_scale(&series, &reference_arms)
    };
    if !reference_arms.is_empty() {
        let scale_note = cdf_scale_note(&series, &reference_arms, x_scale);
        if !scale_note.is_empty() {
            note = if note.is_empty() {
                scale_note
            } else {
                format!("{note}; {scale_note}")
            };
        }
    }
    if y_clip.is_some() {
        let clip_note = y_clip_statement(&series, y_clip);
        note = if note.is_empty() {
            clip_note
        } else {
            format!("{note}; {clip_note}")
        };
    }
    let note_rows = if note.is_empty() {
        0
    } else {
        draw::wrap_label(&note, draw::READING_PLOT_WIDTH as f64).len()
    };
    // A line panel's plot height is a float (its reading band's line height is
    // one); a bar panel's is an integer. Python's f-strings spell the two
    // differently, so the type says which chart the height belongs to.
    let plot_height = if chart != Chart::Bar {
        draw::line_plot_height(series.len(), reading_rows + note_rows)
    } else {
        draw::bar_plot_height(series.len()) as f64
    };
    let axis = panel_axis_extent(panel, &series, &drawn_bounds, run_values, plot_height)?;
    let sliver_statements = if chart == Chart::Bar {
        sliver_bound_statements(&series, &mut drawn_bounds, axis, plot_height, run_values)
    } else {
        Vec::new()
    };
    let stated_sliver_labels: Vec<String> = sliver_statements
        .iter()
        .map(|(index, _)| drawn_bounds[*index].label.clone())
        .collect();
    let stated_slivers: String = if sliver_statements.is_empty() {
        String::new()
    } else {
        sliver_statements
            .iter()
            .map(|(_, sentence)| sentence.clone())
            .collect::<Vec<String>>()
            .join("; ")
    };
    if !stated_slivers.is_empty() {
        note = if note.is_empty() {
            stated_slivers.clone()
        } else {
            format!("{note}; {stated_slivers}")
        };
    }
    let guards = named_guard_values(&series, &drawn_bounds, run_values, chart == Chart::Bar);
    let mut problems = check_bound_governance(&panel.id, &series, &bounds, run_values);
    problems.extend(check_bound_x_categories(&panel.id, &series, &bounds));
    problems.extend(check_reading_band(&panel.id, plot_height));
    problems.extend(check_axis_label(
        &panel.id,
        &panel_y_label,
        &series,
        panel.y_label.as_deref(),
        y_label,
    ));
    problems.extend(check_x_axis_label(
        &panel.id,
        &panel_x_label,
        &series
            .iter()
            .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
            .collect::<Vec<f64>>(),
        run_values,
    ));
    if chart == Chart::Bar {
        problems.extend(check_panel_axis(
            &panel.id,
            &series,
            &drawn_bounds,
            axis,
            Some(plot_height),
            run_values,
            &stated_sliver_labels.join(", "),
        ));
    }
    problems.extend(check_named_values_in_axis(
        &panel.id,
        &drawn_bounds,
        &guards,
        axis,
        Some(plot_height),
    ));
    problems.extend(check_bound_headroom(
        &panel.id,
        &drawn_bounds,
        &guards,
        axis,
        Some(plot_height),
        Some(&series),
    ));
    if !problems.is_empty() {
        return fail(problems.join("\n  "));
    }
    let drawn: Series = series
        .iter()
        .map(|(name, points)| (series_label(name), points.clone()))
        .collect();
    let plotted: Series = series
        .iter()
        .map(|(name, points)| (name.clone(), draw::decimate(points)))
        .collect();
    let plotted_range = if plotted.is_empty() {
        None
    } else {
        Some((
            plotted
                .iter()
                .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
                .fold(f64::INFINITY, f64::min),
            plotted
                .iter()
                .flat_map(|(_, points)| points.iter().map(|(x, _)| *x))
                .fold(f64::NEG_INFINITY, f64::max),
        ))
    };
    let drawn_x_bounds: Vec<(f64, String)> = x_bounds
        .iter()
        .map(|bound| {
            (
                bound.0,
                x_bound_label(
                    bound,
                    &plotted,
                    &panel_unit(&panel_x_label),
                    &panel_unit(&panel_y_label),
                    plotted_range,
                ),
            )
        })
        .collect();
    let markup = match chart {
        Chart::Bar => draw::svg_bar_chart(
            &chart_title,
            &panel_x_label,
            &panel_y_label,
            &series,
            &drawn_bounds,
            Some(axis),
            run_values,
            &note,
        ),
        Chart::Line | Chart::Cdf => {
            let labelled: Vec<(f64, String)> = drawn_bounds
                .iter()
                .map(|bound| (bound.y, governed_label(bound, &series, run_values, false)))
                .collect();
            let chart_doc = draw::LineChart {
                title: &chart_title,
                x_label: &panel_x_label,
                y_label: &panel_y_label,
                series: &drawn,
                y_extent: Some(axis),
                bounds: &labelled,
                walls: true,
                markers: true,
                readings: &readings,
                note: &note,
                x_bounds: &drawn_x_bounds,
                x_scale,
                y_clip,
            };
            if chart == Chart::Line {
                draw::svg_line_chart(&chart_doc)
            } else {
                draw::svg_cdf_chart(&chart_doc)
            }
        }
    };
    let summary_document = checks::panel_summary_document(
        &panel.id,
        chart,
        &panel_x_label,
        &panel_y_label,
        &series,
        &drawn_bounds,
        axis,
        &markup,
        &stated_slivers,
        plot_height,
        run_values,
        fault,
    );
    let markup = checks::introduce_panel_summary(&markup, &summary_document);
    let mut problems = checks::check_panel_summary_stated(
        &panel.id,
        chart,
        &panel_x_label,
        &panel_y_label,
        &series,
        &drawn_bounds,
        axis,
        &markup,
        plot_height,
        &stated_slivers,
        run_values,
        fault,
    );
    problems.extend(checks::check_label_fit(&panel.id, &markup)?);
    problems.extend(checks::check_label_overlap(&panel.id, &markup));
    problems.extend(checks::check_series_labels(&panel.id, &markup, &series));
    problems.extend(checks::check_canvas_text_fit(&panel.id, &markup));
    problems.extend(checks::check_tick_labels_distinct(&panel.id, &markup));
    problems.extend(checks::check_note_fit(&panel.id, &markup)?);
    problems.extend(checks::check_two_sided_bound_drawn(
        &panel.id,
        &drawn_bounds,
        axis,
        &markup,
        Some(plot_height),
    ));
    problems.extend(checks::check_named_guards_drawn(
        &panel.id,
        &guards,
        axis,
        &markup,
        Some(plot_height),
    ));
    problems.extend(checks::check_cdf_reference_reach(
        &panel.id,
        panel,
        &series,
        &reference_arms,
        &markup,
    ));
    problems.extend(checks::check_departure_view_stated(
        &panel.id, panel, panels, points, &markup, x_label, y_label, run_values,
    ));
    problems.extend(checks::check_bound_arm_governance(
        &panel.id, panel, &series, &bounds, run_values, &markup,
    ));
    problems.extend(checks::check_crossing_series_governed(
        &panel.id,
        chart,
        &series,
        &drawn_bounds,
        run_values,
        &markup,
    ));
    if chart == Chart::Bar {
        problems.extend(checks::check_bar_separation(&panel.id, &markup));
        problems.extend(checks::check_zero_bar_marks(
            &panel.id, &series, axis, &markup,
        )?);
        problems.extend(checks::check_sliver_bound_stated(
            &panel.id,
            &series,
            &drawn_bounds,
            axis,
            &markup,
            Some(plot_height),
            run_values,
        ));
    } else {
        problems.extend(checks::check_line_axis_clip_stated(
            &panel.id,
            chart,
            &series,
            &drawn_bounds,
            axis,
            &markup,
            panel.y_extent,
        ));
        problems.extend(checks::check_x_bound_drawn(
            &panel.id, panel, panels, points, x_label, y_label, run_values, &markup,
        )?);
    }
    if chart == Chart::Line {
        problems.extend(checks::check_gap_honesty(&panel.id, &series, &markup));
        problems.extend(checks::check_readings_stated(
            &panel.id, &series, &readings, &markup,
        ));
        problems.extend(checks::check_reading_numbers(&panel.id, &series, &markup));
    }
    if !problems.is_empty() {
        return fail(problems.join("\n  "));
    }
    let panel_documents = extract_svg_panels(&markup);
    if panel_documents.len() != 1 {
        return fail(format!(
            "panel {} rendered {} SVG documents; a {} panel must render exactly one",
            pyjson::repr_str(&panel.id),
            panel_documents.len(),
            pyjson::repr_str(chart.name())
        ));
    }
    Ok(panel_documents[0].clone())
}

// -- one mandate --------------------------------------------------------------

/// The flags `mandate-plot` takes, mirroring the Python tool's command line.
#[derive(Debug, Clone, Default)]
pub struct Args {
    /// The mandate's `.json` panel declaration; its sibling `.csv` carries the data.
    pub declaration: PathBuf,
    /// The directory the panel SVGs (and PNGs) are written into.
    pub out: PathBuf,
    /// Rasterize the panels to PNG (the default).
    pub rasterize: bool,
    /// A headless browser executable or name.
    pub browser: Option<String>,
    /// This run's `MANDATE` measurements, as a JSON object or a file of one.
    pub run_values: Option<String>,
    /// This run's per-arm censoring readings, as a JSON object or a file of one.
    pub run_censoring: Option<String>,
    /// This run's `MANDATE_SMOKE_FAULT` selector, when the run took one.
    pub fault: Option<String>,
    /// Print a JSON summary of the produced panels instead of the text one.
    pub json: bool,
}

/// The run values / censoring, from a JSON object or a file of one.
pub fn load_run_values(path_or_json: Option<&str>) -> PlotResult<Option<J>> {
    let Some(path_or_json) = path_or_json else {
        return Ok(None);
    };
    let candidate = Path::new(path_or_json);
    let text = if candidate.is_file() {
        std::fs::read_to_string(candidate).map_err(|error| {
            MandatePlotError(format!(
                "run values unreadable: {}: {error}",
                candidate.display()
            ))
        })?
    } else {
        path_or_json.to_string()
    };
    let values = pyjson::parse(&text).map_err(|error| {
        MandatePlotError(format!(
            "run values are neither a JSON object nor a file of one: {error}"
        ))
    })?;
    if values.as_obj().is_none() {
        return fail(format!(
            "run values must be a JSON object, got {}",
            values.type_name()
        ));
    }
    Ok(Some(values))
}

/// The run's per-arm censoring readings, from a JSON object or a file of one.
pub fn load_censoring(path_or_json: Option<&str>) -> PlotResult<Option<J>> {
    let Some(readings) = load_run_values(path_or_json)? else {
        return Ok(None);
    };
    for (arm, reading) in readings.as_obj().unwrap_or_default() {
        if arm.trim().is_empty() {
            return fail(format!(
                "run censoring keys must be arm names, got {}",
                pyjson::repr_str(arm)
            ));
        }
        let empty = reading
            .as_obj()
            .map(|members| members.is_empty())
            .unwrap_or(true);
        if empty {
            return fail(format!(
                "the run's reading for arm {} must be a non-empty object of its \
                 measured tokens, got {}",
                pyjson::repr_str(arm),
                reading.repr()
            ));
        }
    }
    Ok(Some(readings))
}

/// Validate one mandate, write and verify its panels, and rasterize them.
///
/// The work runs on a thread with a large stack because the ported regex
/// engine's backtracking matcher recurses once per repetition, and a panel's
/// markup is tens of kilobytes: a `.*?` over a bound label's `<title>`, or a
/// sample marker scan over five thousand points, nests thousands of frames. The
/// depth is bounded by the markup rather than by the pattern, so the fix is
/// room rather than a rewrite; a matcher that recursed without bound would turn
/// a render into a crash, which is not a refusal a caller can read.
pub fn render_mandate(args: &Args) -> PlotResult<J> {
    let args = args.clone();
    let worker = std::thread::Builder::new()
        .name("mandate-plot".to_string())
        .stack_size(512 * 1024 * 1024)
        .spawn(move || render_mandate_on_this_thread(&args))
        .map_err(|error| MandatePlotError(format!("cannot start the plot worker: {error}")))?;
    match worker.join() {
        Ok(result) => result,
        Err(_) => fail("the plot worker panicked; the render is not evidence"),
    }
}

/// The body of [`render_mandate`], on the worker thread's own stack.
fn render_mandate_on_this_thread(args: &Args) -> PlotResult<J> {
    let declaration_path = args.declaration.clone();
    let document = load_declaration(&declaration_path)?;
    let declaration = validate_declaration(&document, &declaration_path)?;
    let data_path = declaration_path.with_extension("csv");
    let points = parse_points(&load_rows(&data_path)?)?;
    reconcile(&declaration.panels, &points, &data_path)?;
    for panel in &declaration.panels {
        check_chart_domain(panel, &points)?;
    }
    let run_values = load_run_values(args.run_values.as_deref())?;
    let run_censoring = load_censoring(args.run_censoring.as_deref())?;
    let censoring_problems =
        check_censoring_drawn(&declaration.panels, &points, run_censoring.as_ref());
    if !censoring_problems.is_empty() {
        return fail(censoring_problems.join("\n  "));
    }

    let out_dir = args.out.clone();
    if out_dir.exists() && !out_dir.is_dir() {
        return fail(format!("--out is not a directory: {}", out_dir.display()));
    }
    std::fs::create_dir_all(&out_dir).map_err(|error| {
        MandatePlotError(format!(
            "--out cannot be created: {}: {error}",
            out_dir.display()
        ))
    })?;

    let mut series_counts: Vec<J> = Vec::new();
    let mut summaries: Vec<J> = Vec::new();
    let mut svg_paths: Vec<String> = Vec::new();
    let mut censoring_arms: Vec<String> = run_censoring
        .as_ref()
        .and_then(J::as_obj)
        .map(|members| members.iter().map(|(arm, _)| arm.clone()).collect())
        .unwrap_or_default();
    censoring_arms.sort();

    for panel in &declaration.panels {
        let panel_id = panel.id.clone();
        let panel_fault = mandate_fault(&declaration.mandate, args.fault.as_deref());
        let markup = panel_markup(
            &declaration.title,
            &declaration.x_label,
            &declaration.y_label,
            panel,
            &points,
            run_values.as_ref(),
            run_censoring.as_ref(),
            &declaration.panels,
            panel_fault.as_deref(),
        )?;
        let problems = validate_panel(0, &markup);
        if !problems.is_empty() {
            return fail(format!(
                "panel {} cannot be used as evidence: {}",
                pyjson::repr_str(&panel_id),
                problems.join("; ")
            ));
        }
        let svg_path = out_dir.join(format!("{}-{panel_id}.svg", declaration.mandate));
        let document = standalone(&markup, &format!("{} [{panel_id}]", declaration.title));
        std::fs::write(&svg_path, &document).map_err(|error| {
            MandatePlotError(format!(
                "panel {} could not be written to {}: {error}",
                pyjson::repr_str(&panel_id),
                svg_path.display()
            ))
        })?;
        let written = std::fs::read_to_string(&svg_path).map_err(|error| {
            MandatePlotError(format!(
                "panel {} could not be written to {}: {error}",
                pyjson::repr_str(&panel_id),
                svg_path.display()
            ))
        })?;
        let count = panel_series_count(&written);
        if count == 0 {
            return fail(format!(
                "{} was written without series geometry ({}); an empty chart is not a \
                 graph",
                svg_path.display(),
                seriesless_reason(&written)
            ));
        }
        let bounds = bound_specs(panel);
        let planned = drawable_bounds(
            panel,
            &panel_series(panel, &points),
            &bounds,
            run_values.as_ref(),
        );
        let drawn = written.matches("class=\"bound\"").count();
        if drawn != planned.len() {
            return fail(format!(
                "{} draws {drawn} bound line(s) for the {} bound(s) the declaration and \
                 the run's own per-arm bounds call for; a bound that is not in the \
                 panel is not a bound",
                svg_path.display(),
                planned.len()
            ));
        }
        let titles: Vec<String> = bound_label_title_re()
            .find_all_whole(&written)
            .iter()
            .map(|title| pyjson::unescape(&title_tag_re().replace_all(title, "")))
            .collect();
        let missing: Vec<String> = bounds
            .iter()
            .filter(|bound| !titles.iter().any(|title| title.starts_with(&bound.label)))
            .map(|bound| bound.label.clone())
            .collect();
        let missing_py = pyjson::py_list(&missing);
        if !missing.is_empty() {
            return fail(format!(
                "{} does not label every declared bound; missing {missing_py}",
                svg_path.display()
            ));
        }
        let Some(mut summary) = checks::read_panel_summary(&written) else {
            return fail(format!(
                "{} carries no panel summary, so the run cannot state what it drew; a \
                 panel is written with its summary or it is not written",
                svg_path.display()
            ));
        };
        let block = checks::panel_summary_block(&summary);
        if let J::Obj(members) = &mut summary {
            members.push(("block".to_string(), J::Str(block.clone())));
        }
        let summary_path = out_dir.join(format!("{}-{panel_id}.summary.txt", declaration.mandate));
        std::fs::write(&summary_path, format!("{block}\n")).map_err(|error| {
            MandatePlotError(format!(
                "panel {} wrote its SVG but not its summary to {}: {error}",
                pyjson::repr_str(&panel_id),
                summary_path.display()
            ))
        })?;
        summaries.push(summary);
        series_counts.push(J::Int(count as i64));
        svg_paths.push(svg_path.display().to_string());
    }

    let mut summary = J::Obj(vec![
        (
            "declaration".to_string(),
            J::Str(declaration_path.display().to_string()),
        ),
        ("data".to_string(), J::Str(data_path.display().to_string())),
        ("mandate".to_string(), J::Str(declaration.mandate.clone())),
        (
            "panels".to_string(),
            J::Int(declaration.panels.len() as i64),
        ),
        ("series_counts".to_string(), J::Arr(series_counts)),
        ("summaries".to_string(), J::Arr(summaries)),
        (
            "svg".to_string(),
            J::Arr(svg_paths.iter().cloned().map(J::Str).collect()),
        ),
        ("png".to_string(), J::Arr(Vec::new())),
        ("browser".to_string(), J::Null),
        ("rasterized".to_string(), J::Bool(false)),
        (
            "censoring".to_string(),
            J::Arr(censoring_arms.into_iter().map(J::Str).collect()),
        ),
    ]);
    if !args.rasterize {
        return Ok(summary);
    }
    let rasterization = rasterize_panels(
        &svg_paths,
        args.browser.as_deref(),
        draw::WIDTH,
        draw::HEIGHT,
    )?;
    if let J::Obj(members) = &mut summary {
        for (key, value) in rasterization.as_obj().unwrap_or_default() {
            if let Some(entry) = members.iter_mut().find(|(name, _)| name == key) {
                entry.1 = value.clone();
            }
        }
        if let Some(entry) = members.iter_mut().find(|(name, _)| name == "rasterized") {
            entry.1 = J::Bool(true);
        }
    }
    Ok(summary)
}

/// The `mandate-plot` subcommand's output, exactly as the Python tool prints it:
/// `panels:`, the mandate, every SVG (and PNG) path, then each panel's own
/// summary block, so the run's own output carries every panel's reading.
pub fn print_summary(summary: &J, json: bool) {
    if json {
        println!("{}", pyjson::dumps(summary, true, Some(2)));
        return;
    }
    println!(
        "panels: {}",
        summary.get("panels").and_then(J::as_i64).unwrap_or(0)
    );
    println!(
        "mandate: {}",
        summary.get("mandate").and_then(J::as_str).unwrap_or("")
    );
    for path in summary.get("svg").and_then(J::as_arr).unwrap_or(&[]) {
        println!("svg: {}", path.as_str().unwrap_or(""));
    }
    if summary
        .get("rasterized")
        .and_then(|value| match value {
            J::Bool(flag) => Some(*flag),
            _ => None,
        })
        .unwrap_or(false)
    {
        for path in summary.get("png").and_then(J::as_arr).unwrap_or(&[]) {
            println!("png: {}", path.as_str().unwrap_or(""));
        }
    }
    for document in summary.get("summaries").and_then(J::as_arr).unwrap_or(&[]) {
        println!(
            "{}",
            document.get("block").and_then(J::as_str).unwrap_or("")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panel_is_extracted_to_its_own_close_before_the_next_opening() {
        let html = "<svg><polyline points=\"1,1 2,2\"/></svg>middle<svg>\
                    <polyline points=\"1,1 2,2\"/></svg>";
        let panels = extract_svg_panels(html);
        assert_eq!(panels.len(), 2);
        assert!(panels[0].ends_with("</svg>"));
        // Vacuity: a panel whose close tag was removed is emitted without one
        // rather than swallowing the next panel, which is what lets
        // `validate_panel` refuse it by name.
        let broken = extract_svg_panels(
            "<svg><polyline points=\"1,1 2,2\"/><svg><polyline points=\"1,1 2,2\"/></svg>",
        );
        assert_eq!(broken.len(), 2);
        assert!(!broken[0].contains("</svg>"));
        assert!(
            validate_panel(0, &broken[0])
                .iter()
                .any(|problem| problem.contains("truncated"))
        );
    }

    #[test]
    fn a_rect_with_no_geometry_is_not_a_series() {
        assert_eq!(panel_series_count("<svg><rect/></svg>"), 0);
        assert_eq!(
            panel_series_count(
                "<svg><rect x=\"1\" y=\"2\" width=\"3\" height=\"4\" class=\"plot-bg\"/></svg>"
            ),
            0
        );
        assert_eq!(
            panel_series_count("<svg><rect x=\"1\" y=\"2\" width=\"3\" height=\"4\"/></svg>"),
            1
        );
        // A rect with a declared but zero size paints nothing, so it is not a
        // series either. This is the property the zero-height floor mark must
        // not be confused with: the mark is *drawn* with a positive height, and
        // this rule is what still tells a label -- or any geometry-less rect --
        // from a series.
        assert_eq!(
            panel_series_count("<svg><rect x=\"1\" y=\"2\" width=\"3\" height=\"0\"/></svg>"),
            0
        );
        assert_eq!(
            panel_series_count("<svg><rect x=\"1\" y=\"2\" width=\"0\" height=\"4\"/></svg>"),
            0
        );
    }

    #[test]
    fn a_bar_at_the_baseline_is_drawn_rather_than_dropped() {
        let series: Series = vec![
            ("clean".to_string(), vec![(1.0, 0.0), (2.0, 0.0)]),
            ("hostile".to_string(), vec![(1.0, 0.0), (2.0, 0.0)]),
        ];
        let markup = draw::svg_bar_chart(
            "t",
            "flow",
            "departure",
            &series,
            &[],
            Some((0.0, 0.021)),
            None,
            "",
        );
        let marks = checks::zero_bar_marks(&markup);
        assert_eq!(marks.len(), 4, "{markup}");
        for mark in &marks {
            // Hollow: a filled rect of any height is what a non-zero bar is.
            assert_eq!(mark.fill, "none", "{markup}");
            assert!(!mark.stroke.is_empty(), "{markup}");
            let height = mark.box_.3 - mark.box_.1;
            assert!(
                (height - ZERO_BAR_MARK_HEIGHT_PX).abs() < 1e-9,
                "the floor mark is {height} px, not the visible {ZERO_BAR_MARK_HEIGHT_PX}"
            );
        }
        // The panel the producer measured is no longer read as empty: the four
        // zero values are four drawn marks.
        assert_eq!(panel_series_count(&markup), 4, "{markup}");
        // The same four values with the mark removed leave a zero-height rect
        // that paints nothing, which is the defect this test exists to catch.
        let dropped = markup
            .replace("class=\"zero-bar\" ", "")
            .replace(" height=\"4.0\" ", " height=\"0.0\" ");
        assert_eq!(checks::zero_bar_marks(&dropped).len(), 0);
        assert_eq!(panel_series_count(&dropped), 0, "{dropped}");
    }

    #[test]
    fn a_seriesless_panel_names_no_rows_and_a_measured_zero_differently() {
        let panel = |series: &str| {
            format!("<svg><desc class=\"panel-summary\">{{\"series\": [{series}]}}</desc></svg>")
        };
        let no_rows = panel(r#"{"name":"clean","points":0,"min":null,"max":null}"#);
        let all_zero = panel(r#"{"name":"clean","points":4,"min":0.0,"max":0.0}"#);
        let empty = validate_panel(0, &no_rows);
        let zero = validate_panel(0, &all_zero);
        assert_eq!(empty.len(), 1, "{empty:?}");
        assert_eq!(zero.len(), 1, "{zero:?}");
        assert!(empty[0].contains("emitted no rows"), "{}", empty[0]);
        assert!(
            zero[0].contains("every one of the 4 points is 0"),
            "{}",
            zero[0]
        );
        assert!(zero[0].contains("measured zero"), "{}", zero[0]);
        assert_ne!(empty[0], zero[0]);
    }

    #[test]
    fn a_standalone_panel_carries_its_style_and_its_size() {
        let panel = "<svg viewBox=\"0 0 960 300\" role=\"img\"><rect/></svg>";
        let document = standalone_panel(panel, 960, 300);
        // The attribute order is the Python tool's: the namespace is added
        // first and the size is then written in front of it.
        assert!(
            document.starts_with(
                "<svg width=\"960\" height=\"300\" xmlns=\"http://www.w3.org/2000/svg\" "
            ),
            "{document}"
        );
        assert!(document.contains(STANDALONE_STYLE));
        assert!(document.ends_with("</svg>"));
        // The title is inserted after the style block, before any content.
        let titled = standalone(panel, "the title");
        assert!(titled.contains("</style><title>the title</title>"));
    }

    #[test]
    fn a_png_is_recognised_by_its_signature_and_size() {
        let mut data = PNG_SIGNATURE.to_vec();
        data.extend(std::iter::repeat_n(0u8, 8));
        data.extend(960u32.to_be_bytes());
        data.extend(300u32.to_be_bytes());
        assert_eq!(png_dimensions(&data), Some((960, 300)));
        assert_eq!(png_dimensions(b"not a png"), None);
        // Vacuity: the size is read from the header rather than assumed.
        let mut other = data.clone();
        other[16..20].copy_from_slice(&640u32.to_be_bytes());
        assert_eq!(png_dimensions(&other), Some((640, 300)));
    }
}
