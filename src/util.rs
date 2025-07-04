pub unsafe fn unsafe_to_slice<T>(val: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts(val as *const T as *const u8, size_of::<T>()) }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;

    #[repr(C)]
    struct TestStruct {
        val1: u64,
        val2: u64,
    }

    #[repr(C)]
    struct AtomicStruct {
        val1: AtomicU64,
        val2: AtomicU64,
    }

    #[test]
    fn format_struct() {
        let data = TestStruct { val1: 1, val2: 1 };

        let slice = unsafe { unsafe_to_slice(&data) };

        assert_eq!(slice, &[1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn format_atomic_struct() {
        let data = AtomicStruct {
            val1: AtomicU64::new(1),
            val2: AtomicU64::new(1),
        };

        let slice = unsafe { unsafe_to_slice(&data) };

        assert_eq!(slice, &[1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
    }
}
