#[cfg(not(target_pointer_width = "64"))]
compile_error!("Only 64-bit is supported");

mod circular_buffer;
mod memory;
mod queue;
mod util;
mod sem;
