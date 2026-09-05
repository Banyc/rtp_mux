//! Lane-aware RTP transport configuration.
//!
//! RTP-mux owns a fixed, lane-aware FEC policy: the interactive lane always
//! enables FEC (optionally carrying explicit interactive tuning and in-stream
//! group FEC), while the bulk lane always disables ordinary FEC and in-stream
//! FEC — regardless of any interactive settings. Each lane keeps its own
//! independent typed metrics observer, and the connect config preserves the
//! per-connector handshake toggle.

use mux::LaneClass;

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

/// The accept-path transport policy rtp-mux owns: the lane-aware FEC knobs,
/// the per-lane metrics observer, and the datagram-obfuscation key. The lane
/// itself is the data argument to [`accept_config`]; everything here is
/// caller policy. (The opening-handshake toggle is not part of the rtp
/// accept config; the server passes it separately to the accept call.)
#[derive(Clone)]
pub(crate) struct AcceptSettings {
    pub interactive_fec_tuning: rtp::FecTuning,
    pub interactive_instream_group_fec: bool,
    pub metrics_observer: Option<rtp::metrics::MetricsObserver>,
    pub obfuscation_key: Option<crate::ObfuscationKey>,
}

fn fec_enabled(lane: LaneClass) -> bool {
    matches!(lane, LaneClass::Interactive)
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
        obfuscation_key: settings
            .obfuscation_key
            .map(crate::ObfuscationKey::into_bytes),
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
            obfuscation_key: None,
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
    fn obfuscation_key_is_passed_through_to_the_rtp_config() {
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
        let accept = accept_config(
            LaneClass::Interactive,
            AcceptSettings {
                obfuscation_key: Some(key),
                ..accept_settings(tuning, false, None)
            },
        );
        assert_eq!(
            accept.obfuscation_key,
            Some([7; 32]),
            "the wrapped obfuscation key must reach the rtp accept config"
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
