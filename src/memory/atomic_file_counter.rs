use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
    sync::atomic::{AtomicU32, Ordering},
};

use memmap2::MmapRaw;

// Probably do not use this as it's quite unsafe and doesn't have parity.
pub struct AtomicFileCounter {
    file: File,
    mem_map: memmap2::MmapRaw,
}

impl AtomicFileCounter {
    pub fn new(path: PathBuf) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        file.set_len(4).unwrap();
        let mem_map = MmapRaw::map_raw(&file).unwrap();

        let s = Self { file, mem_map };
        s.as_atomic().fetch_add(1, Ordering::Relaxed);
        s
    }

    fn as_atomic<'a>(&'a self) -> &'a AtomicU32 {
        unsafe { AtomicU32::from_ptr(self.mem_map.as_mut_ptr() as *mut u32) }
    }

    pub fn file_value(&self) -> u32 {
        self.as_atomic().load(Ordering::Relaxed)
    }
}
