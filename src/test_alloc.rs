//! Test-only allocation instrumentation for `rtp_mux` unit tests.
//!
//! A binary may install only one `#[global_allocator]`, so the unit-test
//! binary installs it here and every allocation-sensitive test observes the
//! same counter. `thread_alloc_count` is thread-local so a tight measurement
//! is not perturbed by unrelated tests allocating concurrently on other
//! threads, and the `#[global_allocator]` is `#[cfg(test)]` so dependency
//! builds (integration tests, `netem_test`, `proxy`) never install it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// Number of allocating calls made on the current thread. A `const`
    /// initializer keeps the instrumentation from recursively allocating.
    static THREAD_ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
}

/// Allocation calls (`alloc` / `alloc_zeroed` / `realloc`) observed on the
/// current thread.
pub(crate) fn thread_alloc_count() -> usize {
    THREAD_ALLOC_COUNT.with(Cell::get)
}

/// The process-wide allocator that feeds the counter above.
struct CountingAllocator;

impl CountingAllocator {
    fn note(layout: Layout) {
        let _ = layout;
        THREAD_ALLOC_COUNT.with(|count| count.set(count.get().saturating_add(1)));
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::note(layout);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::note(layout);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_layout = unsafe { Layout::from_size_align_unchecked(new_size, layout.align()) };
        Self::note(new_layout);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static COUNTING_ALLOCATOR: CountingAllocator = CountingAllocator;
