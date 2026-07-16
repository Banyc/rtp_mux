# `rtp_mux`

Reusable composition of two RTP transports into one dual-lane mux session.

## Boundary

This crate owns RTP connection establishment, lane admission and pairing, dual-lane liveness supervision, connector session reuse, and logical stream creation. It does not own proxy configuration, routing, hot reload, or application connection handlers.

`RtpMuxConnector` returns cancellation-safe `ClientStream` values implementing `AsyncRead + AsyncWrite`. Its standard configuration derives the bulk endpoint as interactive port + 1; tests or embedding transports can supply a separate bulk-address selector without reimplementing lane setup.

Keeping this composition outside both `rtp` and `mux` preserves their intended separation: RTP remains a reliable transport, and mux remains usable over RTP, TCP, or any compatible `AsyncRead + AsyncWrite` transport.
