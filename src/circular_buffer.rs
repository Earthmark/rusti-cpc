pub struct CircularBuffer<'a> {
    buffer: &'a mut [u8],
}

impl<'a> CircularBuffer<'a> {
    pub fn new(buffer: &'a mut [u8]) -> Self {
        Self { buffer }
    }

    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    pub fn read_raw(&self, offset: usize, result_buffer: &mut [u8]) {
        let offset = offset % self.buffer.len();

        // The size from the middle to the end of the buffer space.
        let end_read_length = result_buffer.len().min(self.buffer.len() - offset);
        // the size from the start of the buffer space to the end of the read.
        let start_read_length = result_buffer.len() - end_read_length;

        // Direct offset read
        result_buffer[..end_read_length]
            .copy_from_slice(&self.buffer[offset..offset + end_read_length]);

        if start_read_length > 0 {
            result_buffer[end_read_length..].copy_from_slice(&self.buffer[..start_read_length]);
        }
    }

    pub fn write<T: Sized>(&mut self, offset: usize, value: &T) {
        self.write_raw(offset, unsafe { crate::util::unsafe_to_slice(value) });
    }

    pub fn write_raw(&mut self, offset: usize, src_buffer: &[u8]) {
        let offset = offset % self.buffer.len();

        let end_write_length = src_buffer.len().min(self.buffer.len() - offset);
        let start_write_length = src_buffer.len() - end_write_length;

        self.buffer[offset..offset + end_write_length]
            .copy_from_slice(&src_buffer[..end_write_length]);

        if start_write_length > 0 {
            self.buffer[..start_write_length].copy_from_slice(&src_buffer[end_write_length..]);
        }
    }

    pub fn clear(&mut self, offset: usize, size: usize) {
        let offset = offset % self.buffer.len();

        let end_write_length = size.min(self.buffer.len() - offset);
        let start_write_length = size - end_write_length;

        self.buffer[offset..offset + end_write_length].fill(0);

        if start_write_length > 0 {
            self.buffer[..start_write_length].fill(0);
        }
    }

    pub fn raw_ptr<T>(&self, offset: usize) -> &T {
        self.raw_mut_ptr(offset)
    }

    pub fn raw_mut_ptr<T>(&self, offset: usize) -> &mut T {
        assert!(
            size_of::<T>() <= 8,
            "Raw pointers to objects larger than the 8 byte boundary are not safe."
        );
        assert!(
            offset % 8 == 0,
            "Raw pointers can only be used to access 8-byte aligned objects"
        );

        let offset = offset % self.buffer.len();

        unsafe { &mut *(self.buffer[offset..size_of::<T>()].as_ptr() as *mut T) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_contiguous() {
        let mut slice = [0; 128];

        let mut read_head = [0; 64];
        {
            let mut index = 0;
            for target in slice.as_mut_slice() {
                *target = index;
                index = index + 1;
            }

            let buffer = CircularBuffer::new(slice.as_mut_slice());

            buffer.read_raw(0, read_head.as_mut_slice());
        }

        assert_eq!(read_head.as_slice(), &slice[0..64]);
    }

    #[test]
    fn read_contiguous_wrapped() {
        let mut slice = [0; 128];

        let mut read_head = [0; 64];
        {
            let mut index = 0;
            for target in slice.as_mut_slice() {
                *target = index;
                index = index + 1;
            }

            let buffer = CircularBuffer::new(slice.as_mut_slice());

            buffer.read_raw(128, read_head.as_mut_slice());
        }

        assert_eq!(read_head.as_slice(), &slice[0..64]);
    }

    #[test]
    fn read_wrapped() {
        let mut slice = [0; 128];

        let mut read_head = [0; 64];
        {
            let mut index = 0;
            for target in slice.as_mut_slice() {
                *target = index;
                index = index + 1;
            }

            let buffer = CircularBuffer::new(slice.as_mut_slice());

            buffer.read_raw(100, read_head.as_mut_slice());
        }

        assert_eq!(
            read_head.as_slice(),
            slice[100..]
                .iter()
                .chain(slice[..36].iter())
                .cloned()
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn read_write_contiguous() {
        let mut slice = [0; 128];
        let mut buffer = CircularBuffer::new(slice.as_mut_slice());

        let mut write_content = [0; 64];
        let mut index = 0;
        for target in write_content.as_mut_slice() {
            *target = index;
            index = index + 1;
        }

        buffer.write_raw(0, write_content.as_slice());

        let mut read_content = [0; 64];
        buffer.read_raw(0, read_content.as_mut_slice());

        assert_eq!(write_content, read_content);
    }

    #[test]
    fn read_write_contiguous_over_split() {
        let mut slice = [0; 128];
        let mut buffer = CircularBuffer::new(slice.as_mut_slice());

        let mut write_content = [0; 64];
        let mut index = 0;
        for target in write_content.as_mut_slice() {
            *target = index;
            index = index + 1;
        }

        buffer.write_raw(100, write_content.as_slice());

        let mut read_content = [0; 64];
        buffer.read_raw(100, &mut read_content);

        assert_eq!(write_content, read_content);
    }
}
