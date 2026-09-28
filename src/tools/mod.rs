#![forbid(unsafe_code)]

//! The crate's own performance tooling, as library modules.
//!
//! The tooling lives here rather than in a single binary because two binaries
//! share it: `netem-tools` is the command-line face (`mandate-compare` and
//! `mandate-plot` today) and `perf-history` reuses the same comparison for the
//! coverage/claim axes of `vs-prev.md`. One implementation, so two callers
//! cannot disagree about the semantics.
//!
//! `pyformat`, `pyjson` and `pyre` are the port's own substrate: Python's
//! `format()` for floats, ints and strings, Python's `json` plus `repr` and the
//! `html` escapes, and the subset of `re` the ported patterns are written in.
//! They exist so a ported tool keeps its twin's *messages* and *spelling*, not
//! merely its decisions.

pub mod check_gate;
pub mod json;
pub mod mandate_check;
pub mod mandate_compare;
pub mod mandate_plot;
pub mod pyformat;
pub mod pyjson;
pub mod pyre;
