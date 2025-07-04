use crate::memory::ViewOptions;

use super::MemoryFileFactory;

pub struct WindowsMemoryFileFactory;

impl MemoryFileFactory for WindowsMemoryFileFactory {
    fn create(options: impl ViewOptions) -> memmap2::MmapRaw {
        memmap2::MmapOptions::new().len(options.get_actual_storage_size());

        todo!()

    }
}
