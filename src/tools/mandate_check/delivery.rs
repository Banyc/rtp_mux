//! A delivery floor read as the count of units it tolerates.
//!
//! The delivery mandates assert a *ratio*, but the ratio is made of a count of
//! units the arm itself offers and receives, so the floor has a size in units
//! -- `floor(offered x (1 - floor))` of slack and the next unit fails -- and a
//! breach has an event size. A ratio printed to three decimals cannot state
//! either, so an arm that reports a `delivery` under a mandate that declares a
//! `delivery_floor`, and does not report the counts the ratio is their
//! quotient, is refused here.

use std::collections::BTreeMap;

use crate::tools::json::Json;
use crate::tools::pyjson::repr_float;

use super::lines::Arm;
use super::value::{Ordered, delivery_count, py_round, py_str};
use super::{DELIVERY_FLOOR_KEY, DELIVERY_KEY, MandateReport};

/// One arm's counts under a floor.
#[derive(Debug, Clone, PartialEq)]
pub struct DeliveryArm {
    pub id: String,
    pub offered: i64,
    pub received: i64,
    pub units_short: i64,
    pub budget_units: i64,
    pub window_seconds: Option<f64>,
}

/// One mandate's floor, read in the units of the smallest offer under it.
#[derive(Debug, Clone, PartialEq)]
pub struct DeliveryReading {
    pub floor: Json,
    pub offered_min: i64,
    pub budget_units: i64,
    pub min_failing_units: i64,
    pub units_short_max: i64,
    pub block_ms: Option<f64>,
    pub arms: Vec<DeliveryArm>,
}

/// Python's `repr()` of a value as a message names it.
fn repr_value(value: &Json) -> String {
    match value {
        Json::Float(number) => repr_float(*number),
        Json::Int(number) => number.to_string(),
        Json::Str(text) => crate::tools::pyjson::repr_str(text),
        Json::Bool(flag) => (if *flag { "True" } else { "False" }).to_string(),
        Json::Null => "None".to_string(),
        other => crate::tools::json::to_string(other),
    }
}

/// The refusal an arm that reports a ratio without the counts owes.
fn no_counts_message(id: &str, mandate: &str, printed: &Json, floor: &Json) -> String {
    format!(
        "{id} reports {DELIVERY_KEY}={} under {mandate}'s {DELIVERY_FLOOR_KEY} {} \
         without the counts that ratio is the quotient of (the arm's {}/{} fields), \
         so the number of units the floor tolerates cannot be stated and a breach \
         of it cannot be attributed to an event size",
        py_str(printed),
        py_str(floor),
        super::DELIVERY_OFFERED_COUNTER,
        super::DELIVERY_RECEIVED_COUNTER
    )
}

/// Every declared delivery floor as the count of units it tolerates.
pub fn check_delivery_granularity(
    arms: &[Arm],
    mandate_order: &[String],
    mandates: &BTreeMap<String, MandateReport>,
) -> (BTreeMap<String, DeliveryReading>, Vec<String>) {
    let mut problems = Vec::new();
    let mut recorded = BTreeMap::new();
    for mandate in mandate_order {
        let section = match mandates.get(mandate) {
            Some(section) => section.clone(),
            None => continue,
        };
        let values = &section.values;
        let floor = values.get(DELIVERY_FLOOR_KEY).cloned();
        let delivery_arms: Vec<&Arm> = arms
            .iter()
            .filter(|arm| {
                arm.mandate.as_deref() == Some(mandate.as_str())
                    && arm.values.contains_key(DELIVERY_KEY)
            })
            .collect();
        // A mandate that printed no line has no floor to read, and its absent
        // line is already a problem of its own.
        if section.raw_line.is_none() {
            continue;
        }
        let mut delivery_values: Vec<String> = values
            .keys()
            .filter(|key| key.contains(DELIVERY_KEY))
            .cloned()
            .collect();
        delivery_values.sort();
        let Some(floor) = floor else {
            if !delivery_values.is_empty() {
                problems.push(format!(
                    "{mandate}: the line reports {} but declares no \
                     {DELIVERY_FLOOR_KEY}, so the delivery it reports is read \
                     against no bound and the units it tolerates cannot be stated",
                    delivery_values.join(", ")
                ));
            }
            continue;
        };
        let fraction = match &floor {
            Json::Bool(_) | Json::Str(_) | Json::Null | Json::Array(_) | Json::Object(_) => None,
            Json::Int(number) => Some(*number as f64),
            Json::Float(number) => Some(*number),
        };
        let Some(fraction) = fraction.filter(|value| *value > 0.0 && *value <= 1.0) else {
            problems.push(format!(
                "{mandate}: the declared {DELIVERY_FLOOR_KEY} {} is not a \
                 fraction in (0, 1], so it names no bound a delivery can be read \
                 against",
                repr_value(&floor)
            ));
            continue;
        };
        if delivery_arms.is_empty() {
            problems.push(format!(
                "{mandate}: the line declares a {DELIVERY_FLOOR_KEY} of {} but no \
                 arm of this mandate reports a delivery value, so the floor is \
                 declared over no measurement and its unit budget cannot be stated",
                py_str(&floor)
            ));
            continue;
        }
        let mut entries: Vec<DeliveryArm> = Vec::new();
        for arm in delivery_arms {
            let id = arm.id.clone().unwrap_or_default();
            let offered = arm
                .counters
                .get(super::DELIVERY_OFFERED_COUNTER)
                .and_then(delivery_count);
            let received = arm
                .counters
                .get(super::DELIVERY_RECEIVED_COUNTER)
                .and_then(delivery_count);
            let printed = arm.values.get(DELIVERY_KEY).cloned().unwrap_or(Json::Null);
            let (Some(offered), Some(received)) = (offered, received) else {
                problems.push(no_counts_message(&id, mandate, &printed, &floor));
                continue;
            };
            if offered == 0 {
                problems.push(no_counts_message(&id, mandate, &printed, &floor));
                continue;
            }
            if received > offered {
                problems.push(format!(
                    "{id} reports {DELIVERY_RECEIVED_COUNTER}={received} against \
                     {DELIVERY_OFFERED_COUNTER}={offered}, so its delivery ratio is \
                     above a whole: a flow cannot deliver more units than it offered",
                    DELIVERY_RECEIVED_COUNTER = super::DELIVERY_RECEIVED_COUNTER,
                    DELIVERY_OFFERED_COUNTER = super::DELIVERY_OFFERED_COUNTER
                ));
                continue;
            }
            let ratio = received as f64 / offered as f64;
            let printed_matches = printed
                .as_f64()
                .is_some_and(|number| (number - ratio).abs() > super::DELIVERY_PRINT_STEP / 2.0);
            if printed_matches {
                problems.push(format!(
                    "{id} reports {DELIVERY_KEY}={} for \
                     {DELIVERY_RECEIVED_COUNTER}={received} of \
                     {DELIVERY_OFFERED_COUNTER}={offered}, which is {ratio:.6}: \
                     the ratio is not the quotient of the counts recorded \
                     beside it, so neither the ratio nor those units can be read \
                     as this arm's delivery",
                    py_str(&printed),
                    DELIVERY_RECEIVED_COUNTER = super::DELIVERY_RECEIVED_COUNTER,
                    DELIVERY_OFFERED_COUNTER = super::DELIVERY_OFFERED_COUNTER
                ));
            }
            let budget_units = (offered as f64 * (1.0 - fraction)).floor() as i64;
            entries.push(DeliveryArm {
                id,
                offered,
                received,
                units_short: offered - received,
                budget_units,
                window_seconds: arm_window_seconds(arm, values),
            });
        }
        if entries.is_empty() {
            continue;
        }
        // The tightest budget is the smallest offer's, and the breach is timed
        // against that same arm's window.
        let smallest = entries
            .iter()
            .min_by(|left, right| (left.offered, &left.id).cmp(&(right.offered, &right.id)))
            .expect("entries is non-empty");
        let offered_min = smallest.offered;
        let budget_units = (offered_min as f64 * (1.0 - fraction)).floor() as i64;
        let min_failing_units = budget_units + 1;
        let window = smallest.window_seconds;
        let block_ms = window.filter(|value| *value > 0.0).map(|value| {
            py_round(
                min_failing_units as f64 * value * 1000.0 / offered_min as f64,
                1,
            )
        });
        let mut sorted = entries;
        sorted.sort_by(|left, right| left.id.cmp(&right.id));
        recorded.insert(
            mandate.clone(),
            DeliveryReading {
                floor,
                offered_min,
                budget_units,
                min_failing_units,
                units_short_max: sorted
                    .iter()
                    .map(|entry| entry.units_short)
                    .max()
                    .unwrap_or(0),
                block_ms,
                arms: sorted,
            },
        );
    }
    (recorded, problems)
}

/// The window the arm ran for, from its own line or from its mandate's.
pub fn arm_window_seconds(arm: &Arm, mandate_values: &Ordered) -> Option<f64> {
    let candidates = [
        arm.windows.get("window_seconds").cloned(),
        mandate_values.get("window_s").cloned(),
    ];
    for candidate in candidates.into_iter().flatten() {
        let Some(number) = candidate.as_f64() else {
            continue;
        };
        if number.is_finite() && number > 0.0 {
            return Some(number);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::super::value::Ordered;
    use super::*;
    use crate::tools::json::Json;

    fn arm(id: &str, mandate: &str, values: &[(&str, Json)], counters: &[(&str, Json)]) -> Arm {
        let mut record =
            super::super::lines::parse_arm_line("[mandate-smoke x] recv=1").expect("an arm line");
        record.id = Some(id.to_string());
        record.mandate = Some(mandate.to_string());
        for (key, value) in values {
            record.values.insert(key.to_string(), value.clone());
        }
        for (key, value) in counters {
            record.counters.insert(key.to_string(), value.clone());
        }
        record
    }

    fn mandate(values: &[(&str, Json)], raw: bool) -> MandateReport {
        let mut record = super::super::MandateReport {
            producer: "rtp_mux".to_string(),
            declared: true,
            verdict: Some("PASS".to_string()),
            values: Ordered::default(),
            raw_line: None,
            plots: Vec::new(),
            series_counts: Vec::new(),
            panels: 0,
            panel_summaries: Vec::new(),
            censoring_arms: Vec::new(),
            finished_at_seconds: None,
            duration_seconds: None,
            duration_source: None,
            duration_note: None,
        };
        if raw {
            record.raw_line = Some("MANDATE M2 PASS".to_string());
        }
        for (key, value) in values {
            record.values.insert(key.to_string(), value.clone());
        }
        record
    }

    #[test]
    fn the_floor_is_read_in_the_units_of_the_smallest_offer() {
        let arms = vec![
            arm(
                "M2/clean",
                "M2",
                &[("delivery", Json::Float(1.0))],
                &[("sent", Json::Int(1200)), ("received", Json::Int(1200))],
            ),
            arm(
                "M2/hostile",
                "M2",
                &[("delivery", Json::Float(0.998))],
                &[("sent", Json::Int(1200)), ("received", Json::Int(1198))],
            ),
        ];
        let mut mandates = BTreeMap::new();
        mandates.insert(
            "M2".to_string(),
            mandate(&[("delivery_floor", Json::Float(0.995))], true),
        );
        let (recorded, problems) =
            check_delivery_granularity(&arms, &["M2".to_string()], &mandates);
        assert!(problems.is_empty(), "{problems:?}");
        let reading = recorded.get("M2").expect("a reading");
        assert_eq!(reading.offered_min, 1200);
        assert_eq!(reading.budget_units, 6);
        assert_eq!(reading.min_failing_units, 7);
        assert_eq!(reading.units_short_max, 2);
    }

    #[test]
    fn an_arm_without_its_counts_is_refused() {
        let arms = vec![arm(
            "M2/hostile",
            "M2",
            &[("delivery", Json::Float(0.998))],
            &[],
        )];
        let mut mandates = BTreeMap::new();
        mandates.insert(
            "M2".to_string(),
            mandate(&[("delivery_floor", Json::Float(0.995))], true),
        );
        let (recorded, problems) =
            check_delivery_granularity(&arms, &["M2".to_string()], &mandates);
        assert!(recorded.is_empty());
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("without the counts that ratio is the quotient of"));
    }

    #[test]
    fn a_ratio_its_counts_cannot_produce_is_refused() {
        let arms = vec![arm(
            "M2/clean",
            "M2",
            &[("delivery", Json::Float(0.995))],
            &[("sent", Json::Int(1200)), ("received", Json::Int(1200))],
        )];
        let mut mandates = BTreeMap::new();
        mandates.insert(
            "M2".to_string(),
            mandate(&[("delivery_floor", Json::Float(0.995))], true),
        );
        let (_recorded, problems) =
            check_delivery_granularity(&arms, &["M2".to_string()], &mandates);
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("the ratio is not the quotient of the counts"));
    }

    #[test]
    fn a_delivery_reported_without_a_floor_is_refused() {
        let arms = vec![arm(
            "M2/clean",
            "M2",
            &[("delivery", Json::Float(1.0))],
            &[("sent", Json::Int(1200)), ("received", Json::Int(1200))],
        )];
        let mut mandates = BTreeMap::new();
        // The obligation is the mandate's own *line*: M1 reports a delivery
        // none of its own arms owns, and declares no floor to read it against.
        mandates.insert(
            "M2".to_string(),
            mandate(&[("clean_delivery", Json::Float(1.0))], true),
        );
        let (_recorded, problems) =
            check_delivery_granularity(&arms, &["M2".to_string()], &mandates);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("but declares no delivery_floor"));
    }

    #[test]
    fn a_floor_with_no_delivery_arm_is_refused() {
        let arms: Vec<Arm> = Vec::new();
        let mut mandates = BTreeMap::new();
        mandates.insert(
            "M2".to_string(),
            mandate(&[("delivery_floor", Json::Float(0.995))], true),
        );
        let (_recorded, problems) =
            check_delivery_granularity(&arms, &["M2".to_string()], &mandates);
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("no arm of this mandate reports a delivery value"));
    }

    #[test]
    fn a_mandate_that_printed_no_line_adds_no_second_complaint() {
        let arms: Vec<Arm> = Vec::new();
        let mut mandates = BTreeMap::new();
        mandates.insert("M2".to_string(), mandate(&[], false));
        let (_recorded, problems) =
            check_delivery_granularity(&arms, &["M2".to_string()], &mandates);
        assert!(problems.is_empty(), "{problems:?}");
    }

    #[test]
    fn the_breach_is_timed_against_the_offering_arms_own_window() {
        let mut offering = arm(
            "M2/hostile",
            "M2",
            &[("delivery", Json::Float(0.998))],
            &[("sent", Json::Int(1200)), ("received", Json::Int(1198))],
        );
        offering
            .windows
            .insert("window_seconds".to_string(), Json::Int(12));
        let arms = vec![offering];
        let mut mandates = BTreeMap::new();
        mandates.insert(
            "M2".to_string(),
            mandate(&[("delivery_floor", Json::Float(0.995))], true),
        );
        let (recorded, problems) =
            check_delivery_granularity(&arms, &["M2".to_string()], &mandates);
        assert!(problems.is_empty(), "{problems:?}");
        // 7 units of 1200 over a 12 s window: 70.0 ms.
        assert_eq!(recorded["M2"].block_ms, Some(70.0));
    }
}
