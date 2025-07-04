use std::{
    sync::atomic::{self, AtomicU32, AtomicU64},
    time::Duration,
};

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::{circular_buffer::CircularBuffer, memory::{MemoryViewOptions, ViewOptions}};

pub struct QueueOptions(MemoryViewOptions);

impl From<MemoryViewOptions> for QueueOptions {
    fn from(value: MemoryViewOptions) -> Self {
        Self(value)
    }
}

impl ViewOptions for QueueOptions {
    fn get_actual_storage_size(&self) -> usize {
        size_of::<QueueHeader>() + self.0.get_actual_storage_size()
    }
}

impl From<QueueOptions> for MemoryViewOptions {
    fn from(value: QueueOptions) -> Self {
        value.0
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum PublisherError {
    #[error("buffer is too small to ever fit the queued message.")]
    BufferCapacityTooLow,
}

pub trait Publisher {
    fn enqueue(&mut self, message: &[u8]) -> Result<(), PublisherError>;
}

#[derive(Error, Debug, PartialEq)]
pub enum SubscriberError {
    #[error("there are no pending messages")]
    NoMessages,
    #[error("a different reader tool longer than 10 seconds, the buffer is likely corrupted")]
    OtherReaderTimedOut,
    #[error("the writer took longer than 10 seconds to write a buffer, it likely crashed")]
    WriterTimedOut,
}

pub trait Subscriber {
    fn try_dequeue(&mut self, result_buffer: &mut [u8]) -> Result<usize, SubscriberError>;

    fn dequeue(&mut self, result_buffer: &mut [u8]) -> Result<usize, SubscriberError>;
}

#[repr(C)]
struct QueueHeader {
    read_offset: AtomicU64,
    write_offset: AtomicU64,
    read_lock_timestamp: AtomicU64,
    _reserved: u64,
}

impl QueueHeader {
    fn is_empty(&self) -> bool {
        let read = self.read_offset.load(atomic::Ordering::Relaxed);
        let write = self.write_offset.load(atomic::Ordering::Relaxed);
        read == write
    }
}

struct QueueReadLock<'a>(&'a QueueHeader);

impl<'a> QueueReadLock<'a> {
    fn try_claim(header: &'a QueueHeader, start: u64) -> Result<Self, u64> {
        header
            .read_lock_timestamp
            .compare_exchange(
                0,
                start,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            )
            .map(|_| Self(header))
    }
}

impl<'a> Drop for QueueReadLock<'a> {
    fn drop(&mut self) {
        self.0
            .read_lock_timestamp
            .store(0, atomic::Ordering::Release);
    }
}

#[repr(C)]
struct MessageHeader {
    pub state: AtomicU32,
    pub body_length: AtomicU32,
}

#[repr(u32)]
enum MessageState {
    Writing = 0,
    LockedToBeConsumed = 1,
    ReadyToBeConsumed = 2,
}

impl From<u32> for MessageState {
    fn from(value: u32) -> Self {
        match value {
            0 => MessageState::Writing,
            1 => MessageState::LockedToBeConsumed,
            2 => MessageState::ReadyToBeConsumed,
            _ => panic!("Unknown message state"),
        }
    }
}

impl From<MessageState> for u32 {
    fn from(value: MessageState) -> Self {
        value as u32
    }
}

pub struct Queue<'a> {
    header: &'a QueueHeader,
    buffer: CircularBuffer<'a>,
    signal: named_sem::NamedSemaphore,
}

unsafe impl Sync for Queue<'_> {}

unsafe impl Send for Queue<'_> {}

impl<'a> Queue<'a> {
    pub fn new(memory: &'a mut [u8], memory_name: &str) -> Result<Self, ()> {
        // This is safe as the circular buffer is the first chunk of memory, and the buffer is the rest.
        let header = unsafe { &mut *(memory.as_mut_ptr() as *mut QueueHeader) };
        let buffer = CircularBuffer::new(&mut memory[size_of::<QueueHeader>()..]);

        Ok(Self {
            header,
            buffer,
            signal: crate::sem::create_sem(memory_name),
        })
    }

    fn capacity(&self) -> u64 {
        self.buffer.capacity() as u64
    }

    fn get_message_body_offset(start_offset: usize) -> usize {
        start_offset + size_of::<MessageHeader>()
    }

    fn padded_message_length(body_length: usize) -> usize {
        let length = body_length + size_of::<MessageHeader>();
        if length % 8 != 0 {
            (length / 8) * 8 + 1
        } else {
            length
        }
    }

    fn safe_increment_message_offset(&self, offset: usize, increment: usize) -> usize {
        (offset + increment) % (self.buffer.capacity() * 2)
    }

    fn check_capacity(&self, header: &QueueHeader, message_length: u64) -> bool {
        if message_length > self.capacity() {
            false
        } else if header.is_empty() {
            true
        } else {
            let read_offset =
                (header.read_offset.load(atomic::Ordering::Relaxed)) % self.capacity();
            let write_offset =
                (header.write_offset.load(atomic::Ordering::Relaxed)) % self.capacity();

            // Queue is full
            if read_offset == write_offset {
                false
            } else if read_offset < write_offset {
                if message_length > self.capacity() + read_offset - write_offset {
                    false
                } else {
                    true
                }
            } else if message_length > read_offset - write_offset {
                false
            } else {
                true
            }
        }
    }

    fn try_dequeue_internal(&mut self, result_buffer: &mut [u8]) -> Result<usize, SubscriberError> {
        let start = current_ticks();
        let _lock = match QueueReadLock::try_claim(self.header, start) {
            Ok(lock) => lock,
            Err(read_lock_timestamp) => {
                if start - read_lock_timestamp < TEN_SECONDS_TICKS {
                    return Err(SubscriberError::OtherReaderTimedOut);
                } else {
                    return Err(SubscriberError::NoMessages);
                }
            }
        };

        // Only continue if there is a waiting message, else release the lock and spin.
        if self.header.is_empty() {
            return Err(SubscriberError::NoMessages);
        }

        let read_offset = self.header.read_offset.load(atomic::Ordering::Acquire);
        let write_offset = self.header.write_offset.load(atomic::Ordering::Acquire);
        let header = self.buffer.raw_ptr::<MessageHeader>(read_offset as usize);

        while let Err(_) = header.state.compare_exchange(
            MessageState::ReadyToBeConsumed.into(),
            MessageState::LockedToBeConsumed.into(),
            atomic::Ordering::SeqCst,
            atomic::Ordering::Acquire,
        ) {
            if current_ticks() - start > TEN_SECONDS_TICKS {
                self.header
                    .read_offset
                    .store(write_offset, atomic::Ordering::Release);
                return Err(SubscriberError::WriterTimedOut);
            }

            std::thread::yield_now();
        }

        let body_length = header.body_length.load(atomic::Ordering::Relaxed) as usize;

        self.buffer.read_raw(
            Self::get_message_body_offset(read_offset as usize),
            &mut result_buffer[..body_length],
        );

        let padded_length = Self::padded_message_length(body_length);
        self.buffer.clear(read_offset as usize, padded_length);

        let new_read_offset =
            self.safe_increment_message_offset(read_offset as usize, padded_length);
        self.header
            .read_offset
            .store(new_read_offset as u64, atomic::Ordering::Release);

        Ok(body_length)
    }
}

impl<'a> Publisher for Queue<'a> {
    fn enqueue(&mut self, message: &[u8]) -> Result<(), PublisherError> {
        let message_length = Self::padded_message_length(message.len()) as u64;

        loop {
            if !self.check_capacity(self.header, message_length) {
                return Err(PublisherError::BufferCapacityTooLow);
            }

            // TODO: Make these orderings actually valid, the CPU can totally re-order this into chaos right now.
            let write_offset = self.header.write_offset.load(atomic::Ordering::Acquire);
            let new_write_offset =
                self.safe_increment_message_offset(write_offset as usize, message_length as usize);

            if let Ok(_) = self.header.write_offset.compare_exchange(
                write_offset,
                new_write_offset as u64,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            ) {
                self.buffer
                    .raw_mut_ptr::<MessageHeader>(write_offset as usize)
                    .body_length
                    .store(message.len() as u32, atomic::Ordering::Relaxed);

                self.buffer.write_raw(
                    Self::get_message_body_offset(write_offset as usize),
                    message,
                );

                self.buffer
                    .raw_mut_ptr::<MessageHeader>(write_offset as usize)
                    .state
                    .store(
                        MessageState::ReadyToBeConsumed.into(),
                        atomic::Ordering::Release,
                    );

                return Ok(());
            }
        }
    }
}

const CSHARP_EPOCH: DateTime<Utc> = chrono::NaiveDateTime::new(
    chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap(),
    chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
)
.and_utc();

const TEN_SECONDS_TICKS: u64 = 100_000_000;

fn current_ticks() -> u64 {
    let duration = Utc::now().signed_duration_since(&CSHARP_EPOCH);
    duration.num_seconds() as u64 * 10_000_000 + (duration.subsec_nanos() as u64) / 100
}

impl<'a> Subscriber for Queue<'a> {
    fn dequeue(&mut self, result_buffer: &mut [u8]) -> Result<usize, SubscriberError> {
        let mut loop_count = 0;
        loop {
            match self.try_dequeue(result_buffer) {
                Err(SubscriberError::NoMessages) => {}
                ret => return ret,
            };

            if loop_count > 10 {
                let _ = self.signal.timed_wait(Duration::from_millis(10));
            } else if loop_count > 0 {
                let _ = self.signal.timed_wait(Duration::from_millis(loop_count));
            } else {
                std::thread::yield_now();
            }
            loop_count += 1;
        }
    }

    fn try_dequeue(&mut self, result_buffer: &mut [u8]) -> Result<usize, SubscriberError> {
        let result = self.try_dequeue_internal(result_buffer);
        let _ = self.signal.post();
        result
    }
}

#[cfg(test)]
mod tests {
    use std::{alloc::Layout, slice};

    use super::*;

    #[test]
    #[ignore = "hangs forever waiting for a message"]
    fn read_empty() {
        let memory = unsafe {
            slice::from_raw_parts_mut(
                std::alloc::alloc_zeroed(Layout::from_size_align(128, 8).unwrap()),
                128,
            )
        };
        let mut queue = Queue::new(memory, "TEST1").unwrap();

        let mut output = [0; 64];
        let result = queue.dequeue(&mut output);

        assert_eq!(result, Err(SubscriberError::OtherReaderTimedOut));
    }

    #[test]
    fn read_queued_message() {
        let memory = unsafe {
            slice::from_raw_parts_mut(
                std::alloc::alloc_zeroed(Layout::from_size_align(128, 8).unwrap()),
                128,
            )
        };
        let message = {
            let mut message = [0; 32];
            let mut index = 0;
            for target in message.as_mut_slice() {
                *target = index;
                index = index + 1;
            }
            message
        };

        let mut queue = Queue::new(memory, "TEST2").unwrap();

        let _ = queue.enqueue(&message);

        let mut output = [0; 64];
        let result = queue.dequeue(&mut output);

        assert_eq!(result.map(|size| &output[..size]), Ok(message.as_slice()));
    }
}
