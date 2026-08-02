use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use metrics::counter;
use mux::LaneClass;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LaneRejectionClass {
    Capacity,
    HelloTimeout,
    HelloParse,
    ClassMismatch,
    Admission,
    GroupFull,
    PairingTimeout,
    BirthHeartbeat,
    ReservationLost,
}
impl LaneRejectionClass {
    #[cfg(test)]
    pub(crate) const ALL: [Self; 9] = [
        Self::Capacity,
        Self::HelloTimeout,
        Self::HelloParse,
        Self::ClassMismatch,
        Self::Admission,
        Self::GroupFull,
        Self::PairingTimeout,
        Self::BirthHeartbeat,
        Self::ReservationLost,
    ];
    pub(crate) fn metric_name(self) -> &'static str {
        match self {
            Self::Capacity => "stream.rtp_mux.capacity_rejected",
            Self::HelloTimeout => "stream.rtp_mux.hello_timeout",
            Self::HelloParse => "stream.rtp_mux.hello_parse_error",
            Self::ClassMismatch => "stream.rtp_mux.class_mismatch",
            Self::Admission => "stream.rtp_mux.admission_rejected",
            Self::GroupFull => "stream.rtp_mux.group_full",
            Self::PairingTimeout => "stream.rtp_mux.pairing_timeout",
            Self::BirthHeartbeat => "stream.rtp_mux.birth_heartbeat_error",
            Self::ReservationLost => "stream.rtp_mux.reservation_lost",
        }
    }
}
#[derive(Debug, Clone)]
pub(crate) struct RejectedLaneContext {
    pub(crate) class: LaneRejectionClass,
    pub(crate) peer: SocketAddr,
    pub(crate) local_addr: SocketAddr,
    pub(crate) expected_class: Option<LaneClass>,
    pub(crate) reason: String,
}
#[derive(Debug, Default)]
struct LaneRejectionSummary {
    total: u64,
    by_class: HashMap<LaneRejectionClass, u64>,
    first: Option<RejectedLaneContext>,
    last: Option<RejectedLaneContext>,
}
#[derive(Debug, Default)]
struct LaneRejectionLogInner {
    summary: Mutex<LaneRejectionSummary>,
}
#[derive(Debug, Clone, Default)]
pub(crate) struct LaneRejectionLog {
    inner: Arc<LaneRejectionLogInner>,
}
impl LaneRejectionLog {
    pub(crate) fn record(&self, context: RejectedLaneContext) {
        counter!(context.class.metric_name()).increment(1);
        let mut summary = self.inner.summary.lock().unwrap();
        summary.total = summary.total.saturating_add(1);
        *summary.by_class.entry(context.class).or_default() = summary
            .by_class
            .get(&context.class)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        summary.first.get_or_insert_with(|| context.clone());
        summary.last = Some(context);
    }
    #[cfg(test)]
    pub(crate) fn recorded(&self, class: LaneRejectionClass) -> u64 {
        let summary = self.inner.summary.lock().unwrap();
        summary.by_class.get(&class).copied().unwrap_or_default()
    }
    pub(crate) fn flush(&self) {
        let summary = {
            let mut summary = self.inner.summary.lock().unwrap();
            if summary.total == 0 {
                return;
            }
            std::mem::take(&mut *summary)
        };
        let first = summary.first.unwrap();
        let last = summary.last.unwrap();
        warn!(event = "rtp_mux_lane_rejected", rejected = summary.total, rejection_classes = ?summary.by_class, first_class = ?first.class, first_dn = ?first.peer, first_dn_local = ?first.local_addr, first_expected_class = ?first.expected_class, first_reason = %first.reason, last_class = ?last.class, last_dn = ?last.peer, last_dn_local = ?last.local_addr, last_expected_class = ?last.expected_class, last_reason = %last.reason, "Rejected RTP mux lanes");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_lane_rejection_class_has_its_own_metric() {
        let mut seen: HashMap<&'static str, LaneRejectionClass> = HashMap::new();
        for class in LaneRejectionClass::ALL {
            if let Some(other) = seen.insert(class.metric_name(), class) {
                panic!(
                    "{:?} and {:?} both count into {:?}, so the counter cannot say which one fired",
                    class,
                    other,
                    class.metric_name(),
                );
            }
        }
    }
    #[test]
    fn lane_rejection_log_aggregates_across_classes_peers_and_lanes() {
        let log = LaneRejectionLog::default();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        log.record(RejectedLaneContext {
            class: LaneRejectionClass::HelloTimeout,
            peer,
            local_addr: local,
            expected_class: Some(LaneClass::Interactive),
            reason: "test".to_string(),
        });
        log.record(RejectedLaneContext {
            class: LaneRejectionClass::HelloTimeout,
            peer,
            local_addr: local,
            expected_class: Some(LaneClass::Interactive),
            reason: "test".to_string(),
        });
        log.record(RejectedLaneContext {
            class: LaneRejectionClass::HelloParse,
            peer,
            local_addr: local,
            expected_class: Some(LaneClass::Bulk),
            reason: "test".to_string(),
        });
        let summary = log.inner.summary.lock().unwrap();
        assert_eq!(summary.total, 3);
        assert_eq!(
            summary.by_class.get(&LaneRejectionClass::HelloTimeout),
            Some(&2)
        );
        assert_eq!(
            summary.by_class.get(&LaneRejectionClass::HelloParse),
            Some(&1)
        );
    }
}
