use named_sem::NamedSemaphore;

#[cfg(target_os = "windows")]
fn sem_name_prefix() -> &'static str {
    r"Global\CT.IP."
}

#[cfg(target_os = "linux")]
fn sem_name_prefix() {
    r"/ct.ip."
}

pub fn create_sem(name: &str) -> NamedSemaphore {
    let name = format!("{}{}", sem_name_prefix(), name);
    NamedSemaphore::create_with_max(name, 0, i32::MAX as u32).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_basic() {
        create_sem("taco");
    }
}
