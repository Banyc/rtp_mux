//! The shared byte-sink payload helper.
//!
//! Every arm that measures a delivered-byte total writes a cyclic payload into
//! one of `rtp`'s byte-counting sinks, and every one of those sinks verifies
//! the received stream against a fixed `% 251` pattern.  The sink advances its
//! offset only after a whole read matches, so a producer that wraps its buffer
//! at a length that is **not** a whole multiple of the pattern period emits a
//! discontinuous stream: the first read that straddles the wrap fails the
//! check, the sink stops advancing its offset *and* stops counting, and the
//! delivered total freezes at whatever was written before the wrap — silently,
//! because the read did not error.
//!
//! This module is the single authority for the period, for a period-aligned
//! chunk length, and for the saturating writer those arms share, so no arm can
//! reintroduce a discontinuous wrap by choosing its own chunk.

use std::time::{Duration, Instant};

use tokio::io::{AsyncWrite, AsyncWriteExt};

/// The byte sinks' payload period: `expected = (offset + j) % 251`.
pub const BYTE_SINK_PAYLOAD_PERIOD: usize = 251;

/// The canonical bulk chunk length: a whole multiple of
/// [`BYTE_SINK_PAYLOAD_PERIOD`], so [`saturate`]'s wrap is seamless.
///
/// `64 KiB` is **not** a multiple (`65536 mod 251 = 25`), which is the defect
/// this constant exists to prevent.
pub const BYTE_SINK_BULK_CHUNK_BYTES: usize = BYTE_SINK_PAYLOAD_PERIOD * 256;

/// A deterministic payload of `chunk_bytes` bytes, trimmed down to a whole
/// number of [`BYTE_SINK_PAYLOAD_PERIOD`] periods so a cyclic writer's wrap
/// keeps the stream continuous.
pub fn byte_sink_payload(chunk_bytes: usize) -> Vec<u8> {
    let len = chunk_bytes - (chunk_bytes % BYTE_SINK_PAYLOAD_PERIOD);
    (0..len)
        .map(|i| (i % BYTE_SINK_PAYLOAD_PERIOD) as u8)
        .collect()
}

/// Write `payload` back to back, wrapping at its length, until `run_for`
/// elapses.  Advance the offset by exactly the bytes each write commits, so the
/// byte stream's own pattern is continuous across writes as long as
/// `payload.len()` is a whole number of periods (see [`byte_sink_payload`]).
pub async fn saturate(write: &mut (impl AsyncWrite + Unpin), payload: &[u8], run_for: Duration) {
    let deadline = Instant::now() + run_for;
    let mut offset = 0usize;
    while Instant::now() < deadline {
        match write.write(&payload[offset..]).await {
            Ok(0) | Err(_) => break,
            Ok(n) => offset = (offset + n) % payload.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical chunk is exactly what the sinks' `% 251` verifier needs:
    /// a whole number of periods, and *not* the 64 KiB length whose wrap the
    /// defect was about.
    #[test]
    fn the_canonical_chunk_is_a_whole_number_of_periods() {
        assert_eq!(BYTE_SINK_BULK_CHUNK_BYTES % BYTE_SINK_PAYLOAD_PERIOD, 0);
        assert_eq!(64 * 1024 % BYTE_SINK_PAYLOAD_PERIOD, 25);
        assert_ne!(BYTE_SINK_BULK_CHUNK_BYTES, 64 * 1024);
    }

    /// The generator trims a misaligned request to the nearest whole period
    /// and leaves an aligned one untouched, so both forms are seamless.
    #[test]
    fn the_generator_trims_to_a_whole_number_of_periods() {
        for requested in [1, 250, 251, 252, 64 * 1024, BYTE_SINK_BULK_CHUNK_BYTES] {
            let payload = byte_sink_payload(requested);
            assert_eq!(payload.len() % BYTE_SINK_PAYLOAD_PERIOD, 0);
            assert_eq!(
                payload.len(),
                requested - (requested % BYTE_SINK_PAYLOAD_PERIOD)
            );
        }
    }

    /// The reader's byte stream is continuous across the writer's wrap: a
    /// write that straddles the boundary equals the single `(offset % 251)`
    /// stream the sink checks, which is exactly `saturate`'s `%` wrap.
    #[test]
    fn the_stream_is_continuous_across_the_wrap() {
        let payload = byte_sink_payload(BYTE_SINK_BULK_CHUNK_BYTES);
        let len = payload.len();
        let mut sink = Vec::new();
        // Reproduce `saturate`'s offset arithmetic over the wrap: short writes
        // that straddle the boundary, starting mid-period.
        let mut offset = len - 7;
        for _ in 0..10 {
            let n = 11.min(len - offset);
            sink.extend_from_slice(&payload[offset..offset + n]);
            offset = (offset + n) % len;
        }
        for (i, &byte) in sink.iter().enumerate() {
            assert_eq!(
                byte,
                ((len - 7 + i) % BYTE_SINK_PAYLOAD_PERIOD) as u8,
                "byte {i} broke the cyclic pattern across the wrap"
            );
        }
    }
}
