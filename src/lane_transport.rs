//! Lane-aware RTP transport configuration.
//!
//! RTP-mux owns a fixed, lane-aware FEC policy: the interactive lane always
//! enables FEC (optionally carrying explicit interactive tuning and in-stream
//! group FEC), while the bulk lane always disables ordinary FEC and in-stream
//! FEC — regardless of any interactive settings. Each lane keeps its own
//! independent typed metrics observer, and the connect config preserves the
//! per-connector handshake toggle.
//!
//! The lane-aware frame-delivery policy is likewise owned here: the
//! interactive lane enables the receiver-side fast-forward so a complete frame
//! past an unrepaired hole is handed up immediately (mux's per-stream reorder
//! buffer restores ordering), while the bulk lane stays strictly ordered so
//! its throughput and ordering contract are unchanged.

//! The lane-aware congestion intent is owned here too: the bulk lane runs over
//! its own dedicated RTP connection with no competing traffic, so it declares
//! [`CongestionLane::Dedicated`] (the shallower drain / gentler probe tuning);
//! the interactive lane shares the host's bottleneck with whatever else is on
//! the wire, so it declares [`CongestionLane::Shared`] and keeps the
//! conservative cross-traffic-protecting tuning.

use mux::LaneClass;
use rtp::CongestionLane;

/// The connect-path transport policy rtp-mux owns: the lane-aware FEC knobs,
/// the per-lane metrics observer, the RTP opening-handshake toggle, and the
/// datagram-obfuscation key. The lane itself is the data argument to
/// [`connect_config`]; everything here is caller policy.
#[derive(Clone)]
pub(crate) struct ConnectSettings {
    pub interactive_fec_tuning: rtp::FecTuning,
    pub interactive_instream_group_fec: bool,
    pub handshake: bool,
    pub metrics_observer: Option<rtp::metrics::MetricsObserver>,
    pub obfuscation_key: Option<crate::ObfuscationKey>,
}

/// The accept-path transport policy rtp-mux owns: the lane-aware FEC knobs
/// and the per-lane metrics observer. The lane itself is the data argument
/// to [`accept_config`]; everything here is caller policy. (The
/// opening-handshake toggle is not part of the rtp accept config; the
/// server passes it separately to the accept call. Datagram obfuscation is
/// not part of the accept policy either: the single-path rtp listener takes
/// its key at `Listener::bind_with_key`.)
#[derive(Clone)]
pub(crate) struct AcceptSettings {
    pub interactive_fec_tuning: rtp::FecTuning,
    pub interactive_instream_group_fec: bool,
    pub metrics_observer: Option<rtp::metrics::MetricsObserver>,
}

fn fec_enabled(lane: LaneClass) -> bool {
    matches!(lane, LaneClass::Interactive)
}

/// The lane's receiver-side frame-delivery policy. The interactive lane opts
/// into the fast-forward (mux reassembles each stream in order); the bulk lane
/// stays strict. Frame delivery itself is forced on by the frame-delivery
/// entry points on both lanes; this only selects the ordering policy.
fn frame_delivery(lane: LaneClass) -> rtp::FrameMode {
    if matches!(lane, LaneClass::Interactive) {
        rtp::FrameMode::enabled_reordering()
    } else {
        rtp::FrameMode::default()
    }
}

/// The lane's congestion-controller intent.  The bulk lane is the dedicated
/// pipe (no competing traffic over its connection's queue); the interactive
/// lane shares the host's bottleneck and keeps the conservative tuning.  This
/// is deliberately independent of [`frame_delivery`]: the bulk lane keeps
/// frame delivery while still declaring a dedicated congestion lane.
///
/// This is the one authority for the mapping: the layer-testing kit reads it
/// (via [`LaneRtpConfig::production_bulk`](crate::testkit::dual::LaneRtpConfig::production_bulk))
/// so a scenario's arm declares the same congestion lane the deployment does,
/// and the two cannot drift.
pub(crate) fn congestion_lane(lane: LaneClass) -> CongestionLane {
    match lane {
        LaneClass::Bulk => CongestionLane::Dedicated,
        LaneClass::Interactive => CongestionLane::Shared,
    }
}

pub(crate) fn connect_config(
    lane: LaneClass,
    settings: ConnectSettings,
) -> rtp::udp::ConnectConfig<'static> {
    let defaults = rtp::udp::ConnectConfig::default();
    let fec = fec_enabled(lane);
    rtp::udp::ConnectConfig {
        handshake: settings.handshake,
        fec,
        fec_tuning: if fec {
            settings.interactive_fec_tuning
        } else {
            rtp::FecTuning::default()
        },
        instream_group_fec: fec && settings.interactive_instream_group_fec,
        metrics_observer: settings.metrics_observer,
        obfuscation_key: settings
            .obfuscation_key
            .map(crate::ObfuscationKey::into_bytes),
        frame_delivery: frame_delivery(lane),
        congestion_lane: congestion_lane(lane),
        ..defaults
    }
}

pub(crate) fn accept_config(lane: LaneClass, settings: AcceptSettings) -> rtp::udp::AcceptConfig {
    let defaults = rtp::udp::AcceptConfig::default();
    let fec = fec_enabled(lane);
    rtp::udp::AcceptConfig {
        fec,
        fec_tuning: if fec {
            settings.interactive_fec_tuning
        } else {
            rtp::FecTuning::default()
        },
        instream_group_fec: fec && settings.interactive_instream_group_fec,
        metrics_observer: settings.metrics_observer,
        frame_delivery: frame_delivery(lane),
        congestion_lane: congestion_lane(lane),
        ..defaults
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observer() -> Option<rtp::metrics::MetricsObserver> {
        Some(rtp::metrics::MetricsObserver::new(|_| {}))
    }

    fn connect_settings(
        tuning: rtp::FecTuning,
        instream_group_fec: bool,
        handshake: bool,
        metrics_observer: Option<rtp::metrics::MetricsObserver>,
    ) -> ConnectSettings {
        ConnectSettings {
            interactive_fec_tuning: tuning,
            interactive_instream_group_fec: instream_group_fec,
            handshake,
            metrics_observer,
            obfuscation_key: None,
        }
    }

    fn accept_settings(
        tuning: rtp::FecTuning,
        instream_group_fec: bool,
        metrics_observer: Option<rtp::metrics::MetricsObserver>,
    ) -> AcceptSettings {
        AcceptSettings {
            interactive_fec_tuning: tuning,
            interactive_instream_group_fec: instream_group_fec,
            metrics_observer,
        }
    }

    #[test]
    fn interactive_lane_always_enables_fec() {
        let tuning = rtp::FecTuning::max_diversity();
        let connect = connect_config(
            LaneClass::Interactive,
            connect_settings(tuning, true, true, observer()),
        );
        assert!(connect.fec, "interactive connect config must enable FEC");
        assert_eq!(connect.fec_tuning, tuning);
        assert!(connect.instream_group_fec);
        let accept = accept_config(
            LaneClass::Interactive,
            accept_settings(tuning, true, observer()),
        );
        assert!(accept.fec, "interactive accept config must enable FEC");
        assert_eq!(accept.fec_tuning, tuning);
        assert!(accept.instream_group_fec);
    }

    #[test]
    fn bulk_lane_disables_every_fec_mode() {
        let tuning = rtp::FecTuning::max_diversity();
        let connect = connect_config(LaneClass::Bulk, connect_settings(tuning, true, true, None));
        assert!(!connect.fec, "bulk connect config must disable FEC");
        assert_eq!(connect.fec_tuning, rtp::FecTuning::default());
        assert!(!connect.instream_group_fec);
        let accept = accept_config(LaneClass::Bulk, accept_settings(tuning, true, None));
        assert!(!accept.fec, "bulk accept config must disable FEC");
        assert_eq!(accept.fec_tuning, rtp::FecTuning::default());
        assert!(!accept.instream_group_fec);
    }

    #[test]
    fn interactive_lane_enables_frame_delivery_fast_forward() {
        let connect = connect_config(
            LaneClass::Interactive,
            connect_settings(rtp::FecTuning::default(), false, true, None),
        );
        assert!(
            connect.frame_delivery.enabled && connect.frame_delivery.allow_reorder,
            "interactive lane must opt into receiver-side frame fast-forward"
        );
        let accept = accept_config(
            LaneClass::Interactive,
            accept_settings(rtp::FecTuning::default(), false, None),
        );
        assert!(
            accept.frame_delivery.enabled && accept.frame_delivery.allow_reorder,
            "interactive lane must opt into receiver-side frame fast-forward"
        );
    }

    #[test]
    fn bulk_lane_stays_strictly_ordered() {
        let connect = connect_config(
            LaneClass::Bulk,
            connect_settings(rtp::FecTuning::default(), false, true, None),
        );
        assert!(
            !connect.frame_delivery.allow_reorder,
            "bulk lane must keep strict frame ordering"
        );
        let accept = accept_config(
            LaneClass::Bulk,
            accept_settings(rtp::FecTuning::default(), false, None),
        );
        assert!(
            !accept.frame_delivery.allow_reorder,
            "bulk lane must keep strict frame ordering"
        );
    }

    /// The bulk lane is the dedicated pipe: it declares a `Dedicated`
    /// congestion lane even though it keeps frame delivery.  The interactive
    /// lane shares the host bottleneck and stays `Shared`.
    #[test]
    fn bulk_lane_declares_a_dedicated_congestion_lane() {
        let connect = connect_config(
            LaneClass::Bulk,
            connect_settings(rtp::FecTuning::default(), false, true, None),
        );
        assert_eq!(
            connect.congestion_lane,
            CongestionLane::Dedicated,
            "bulk lane must declare a dedicated congestion lane"
        );
        assert_eq!(
            connect.frame_delivery,
            rtp::FrameMode::default(),
            "the bulk config's congestion intent is independent of its frame-delivery bit"
        );
        let accept = accept_config(
            LaneClass::Bulk,
            accept_settings(rtp::FecTuning::default(), false, None),
        );
        assert_eq!(accept.congestion_lane, CongestionLane::Dedicated);
    }

    #[test]
    fn interactive_lane_declares_a_shared_congestion_lane() {
        let connect = connect_config(
            LaneClass::Interactive,
            connect_settings(rtp::FecTuning::default(), false, true, None),
        );
        assert_eq!(
            connect.congestion_lane,
            CongestionLane::Shared,
            "the interactive lane must keep the conservative shared tuning"
        );
        let accept = accept_config(
            LaneClass::Interactive,
            accept_settings(rtp::FecTuning::default(), false, None),
        );
        assert_eq!(accept.congestion_lane, CongestionLane::Shared);
    }

    #[test]
    fn connect_config_preserves_requested_handshake_mode() {
        let tuning = rtp::FecTuning::default();
        let protected = connect_config(
            LaneClass::Interactive,
            connect_settings(tuning, false, true, None),
        );
        assert!(protected.handshake);
        let unprotected = connect_config(
            LaneClass::Interactive,
            connect_settings(tuning, false, false, None),
        );
        assert!(!unprotected.handshake);
    }

    #[test]
    fn per_lane_observers_survive_transport_configuration() {
        let tuning = rtp::FecTuning::default();
        let interactive = observer();
        let bulk = observer();
        let connect = connect_config(
            LaneClass::Interactive,
            connect_settings(tuning, false, true, interactive),
        );
        assert!(
            connect.metrics_observer.is_some(),
            "interactive connect config lost its observer"
        );
        let bulk_connect =
            connect_config(LaneClass::Bulk, connect_settings(tuning, false, true, bulk));
        assert!(
            bulk_connect.metrics_observer.is_some(),
            "bulk connect config lost its observer"
        );
        let accept = accept_config(
            LaneClass::Interactive,
            accept_settings(tuning, false, observer()),
        );
        assert!(
            accept.metrics_observer.is_some(),
            "interactive accept config lost its observer"
        );
        let bulk_accept =
            accept_config(LaneClass::Bulk, accept_settings(tuning, false, observer()));
        assert!(
            bulk_accept.metrics_observer.is_some(),
            "bulk accept config lost its observer"
        );
    }

    #[test]
    fn obfuscation_key_is_passed_through_to_the_rtp_connect_config() {
        let tuning = rtp::FecTuning::default();
        let key = crate::ObfuscationKey::from_bytes([7; 32]);
        let connect = connect_config(
            LaneClass::Interactive,
            ConnectSettings {
                obfuscation_key: Some(key),
                ..connect_settings(tuning, false, true, None)
            },
        );
        assert_eq!(
            connect.obfuscation_key,
            Some([7; 32]),
            "the wrapped obfuscation key must reach the rtp connect config"
        );
        let plain = connect_config(
            LaneClass::Interactive,
            connect_settings(tuning, false, true, None),
        );
        assert_eq!(
            plain.obfuscation_key, None,
            "the default (no key) must stay in the clear"
        );
    }
}
