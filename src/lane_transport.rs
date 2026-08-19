//! Lane-aware RTP transport configuration.
//!
//! RTP-mux owns a fixed, lane-aware FEC policy: the interactive lane always
//! enables FEC (optionally carrying explicit interactive tuning and in-stream
//! group FEC), while the bulk lane always disables ordinary FEC and in-stream
//! FEC — regardless of any interactive settings. Each lane keeps its own
//! independent typed metrics observer, and the connect config preserves the
//! per-connector handshake toggle.

use mux::LaneClass;

fn fec_enabled(lane: LaneClass) -> bool {
    matches!(lane, LaneClass::Interactive)
}

pub(crate) fn connect_config(
    interactive_fec_tuning: rtp::FecTuning,
    interactive_instream_group_fec: bool,
    handshake: bool,
    lane: LaneClass,
    metrics_observer: Option<rtp::metrics::MetricsObserver>,
) -> rtp::udp::ConnectConfig<'static> {
    let defaults = rtp::udp::ConnectConfig::default();
    let fec = fec_enabled(lane);
    rtp::udp::ConnectConfig {
        handshake,
        fec,
        fec_tuning: if fec {
            interactive_fec_tuning
        } else {
            rtp::FecTuning::default()
        },
        instream_group_fec: fec && interactive_instream_group_fec,
        metrics_observer,
        ..defaults
    }
}

pub(crate) fn accept_config(
    interactive_fec_tuning: rtp::FecTuning,
    interactive_instream_group_fec: bool,
    lane: LaneClass,
    metrics_observer: Option<rtp::metrics::MetricsObserver>,
) -> rtp::udp::AcceptConfig {
    let defaults = rtp::udp::AcceptConfig::default();
    let fec = fec_enabled(lane);
    rtp::udp::AcceptConfig {
        fec,
        fec_tuning: if fec {
            interactive_fec_tuning
        } else {
            rtp::FecTuning::default()
        },
        instream_group_fec: fec && interactive_instream_group_fec,
        metrics_observer,
        ..defaults
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observer() -> Option<rtp::metrics::MetricsObserver> {
        Some(rtp::metrics::MetricsObserver::new(|_| {}))
    }

    #[test]
    fn interactive_lane_always_enables_fec() {
        let tuning = rtp::FecTuning::max_diversity();
        let connect = connect_config(tuning, true, true, LaneClass::Interactive, observer());
        assert!(connect.fec, "interactive connect config must enable FEC");
        assert_eq!(connect.fec_tuning, tuning);
        assert!(connect.instream_group_fec);
        let accept = accept_config(tuning, true, LaneClass::Interactive, observer());
        assert!(accept.fec, "interactive accept config must enable FEC");
        assert_eq!(accept.fec_tuning, tuning);
        assert!(accept.instream_group_fec);
    }

    #[test]
    fn bulk_lane_disables_every_fec_mode() {
        let tuning = rtp::FecTuning::max_diversity();
        let connect = connect_config(tuning, true, true, LaneClass::Bulk, None);
        assert!(!connect.fec, "bulk connect config must disable FEC");
        assert_eq!(connect.fec_tuning, rtp::FecTuning::default());
        assert!(!connect.instream_group_fec);
        let accept = accept_config(tuning, true, LaneClass::Bulk, None);
        assert!(!accept.fec, "bulk accept config must disable FEC");
        assert_eq!(accept.fec_tuning, rtp::FecTuning::default());
        assert!(!accept.instream_group_fec);
    }

    #[test]
    fn connect_config_preserves_requested_handshake_mode() {
        let tuning = rtp::FecTuning::default();
        let protected = connect_config(tuning, false, true, LaneClass::Interactive, None);
        assert!(protected.handshake);
        let unprotected = connect_config(tuning, false, false, LaneClass::Interactive, None);
        assert!(!unprotected.handshake);
    }

    #[test]
    fn per_lane_observers_survive_transport_configuration() {
        let tuning = rtp::FecTuning::default();
        let interactive = observer();
        let bulk = observer();
        let connect = connect_config(tuning, false, true, LaneClass::Interactive, interactive);
        assert!(
            connect.metrics_observer.is_some(),
            "interactive connect config lost its observer"
        );
        let bulk_connect = connect_config(tuning, false, true, LaneClass::Bulk, bulk);
        assert!(
            bulk_connect.metrics_observer.is_some(),
            "bulk connect config lost its observer"
        );
        let accept = accept_config(tuning, false, LaneClass::Interactive, observer());
        assert!(
            accept.metrics_observer.is_some(),
            "interactive accept config lost its observer"
        );
        let bulk_accept = accept_config(tuning, false, LaneClass::Bulk, observer());
        assert!(
            bulk_accept.metrics_observer.is_some(),
            "bulk accept config lost its observer"
        );
    }
}
