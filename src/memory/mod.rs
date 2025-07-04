use std::{borrow::Cow, path::PathBuf};

use thiserror::Error;

mod windows;
mod atomic_file_counter;
 
trait MemoryFileFactory {
    fn create(options: impl ViewOptions) -> memmap2::MmapRaw;
}

pub fn create_memory_view(options: impl ViewOptions) -> memmap2::MmapRaw {
    windows::WindowsMemoryFileFactory::create(options)
}

pub struct MemoryViewOptions {
    pub memory_view_name: Cow<'static, str>,
    pub path: PathBuf,
    pub capacity: u64,
}

#[derive(Error, Debug)]
pub enum MemoryViewOptionError {
    #[error("view name empty")]
    EmptyViewName,
    #[error("View must be a multiple of 8 bytes, and be at least 16 bytes long")]
    InvalidSize(u64),
}

impl MemoryViewOptions {
    pub fn new_temp(
        memory_view_name: impl Into<Cow<'static, str>>,
        capacity: u64,
    ) -> Result<Self, MemoryViewOptionError> {
        unsafe { Self::new(memory_view_name, std::env::temp_dir(), capacity) }
    }

    pub unsafe fn new(
        memory_view_name: impl Into<Cow<'static, str>>,
        path: PathBuf,
        capacity: u64,
    ) -> Result<Self, MemoryViewOptionError> {
        let memory_view_name = memory_view_name.into();

        if memory_view_name.is_empty() {
            Err(MemoryViewOptionError::EmptyViewName)
        } else if capacity < 16 || capacity % 8 != 0 {
            Err(MemoryViewOptionError::InvalidSize(capacity))
        } else {
            Ok(Self {
                memory_view_name,
                path: path,
                capacity,
            })
        }
    }
}

pub trait ViewOptions: Into<MemoryViewOptions> {
    fn get_actual_storage_size(&self) -> usize;
}

impl ViewOptions for MemoryViewOptions {
    fn get_actual_storage_size(&self) -> usize {
        self.capacity as usize
    }
}
