//! Utility functions for SmithyQ.
//!
//! This module contains helper functions used throughout SmithyQ for
//! backoff calculations, timing, and other common operations.

use crate::error::SmithyResult;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Calculate exponential backoff with jitter for worker polling.
///
/// This function implements an exponential backoff algorithm with a maximum cap
/// and worker-specific staggering to prevent thundering herd problems.
///
/// # Arguments
///
/// * `empty_count` - Number of consecutive empty responses (increases backoff)
/// * `worker_index` - Worker identifier for staggering (prevents all workers polling at once)
///
/// # Returns
///
/// Backoff time in milliseconds
///
/// # Examples
///
/// ```rust
/// use smithyq::utils::calculate_backoff;
///
/// // First empty response for worker 0
/// let backoff = calculate_backoff(1, 0);
/// assert_eq!(backoff, 200); // 100 * 2^1 + 0 * 50
///
/// // Third empty response for worker 2  
/// let backoff = calculate_backoff(3, 2);
/// assert_eq!(backoff, 900); // 100 * 2^3 + 2 * 50 = 800 + 100
///
/// // High empty count hits the cap
/// let backoff = calculate_backoff(10, 5);
/// assert_eq!(backoff, 5000); // Capped at max_backoff
/// ```
pub fn calculate_backoff(empty_count: u32, worker_index: u64) -> u64 {
    let base: u64 = 100; // Base backoff in milliseconds
    let max_backoff: u64 = 5000; // Maximum backoff (5 seconds)
    let stagger = worker_index * 50; // Worker-specific stagger

    // Exponential backoff with cap at 2^6 to prevent overflow
    let exponential = base * (2u64.pow(empty_count.min(6)));

    // Add stagger and apply maximum cap
    (exponential + stagger).min(max_backoff)
}

/// Calculate backoff with custom parameters.
///
/// This is a more flexible version of `calculate_backoff` that allows
/// customization of all parameters.
pub fn calculate_custom_backoff(
    empty_count: u32,
    worker_index: u64,
    base_ms: u64,
    max_backoff_ms: u64,
    stagger_ms: u64,
    max_exponent: u32,
) -> u64 {
    let stagger = worker_index * stagger_ms;
    let exponential = base_ms * (2u64.pow(empty_count.min(max_exponent)));
    (exponential + stagger).min(max_backoff_ms)
}

/// Calculate linear backoff (alternative to exponential).
///
/// Useful for scenarios where exponential backoff is too aggressive.
pub fn calculate_linear_backoff(
    empty_count: u32,
    worker_index: u64,
    base_ms: u64,
    increment_ms: u64,
    max_backoff_ms: u64,
) -> u64 {
    let stagger = worker_index * 50;
    let linear = base_ms + (empty_count as u64 * increment_ms);
    (linear + stagger).min(max_backoff_ms)
}

/// Generate a unique task ID.
///
/// Creates a UUID v4 string suitable for use as a task identifier.
pub fn generate_task_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Generate a unique task ID with a custom prefix.
///
/// Useful for creating identifiable task IDs in different contexts.
///
/// # Examples
///
/// ```rust
/// use smithyq::utils::generate_task_id_with_prefix;
///
/// let task_id = generate_task_id_with_prefix("email");
/// // Returns something like: "email_f47ac10b-58cc-4372-a567-0e02b2c3d479"
/// ```
pub fn generate_task_id_with_prefix(prefix: &str) -> String {
    format!("{}_{}", prefix, uuid::Uuid::new_v4())
}

/// Get current timestamp as milliseconds since Unix epoch.
///
/// Useful for timing and duration calculations.
pub fn current_timestamp_ms() -> SmithyResult<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .map_err(|e| crate::error::SmithyError::config(format!("Time error: {}", e)))
}

/// Get current timestamp as seconds since Unix epoch.
pub fn current_timestamp_secs() -> SmithyResult<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|e| crate::error::SmithyError::config(format!("Time error: {}", e)))
}

/// Calculate duration between two timestamps.
///
/// Returns None if the end time is before the start time.
pub fn duration_between(start: SystemTime, end: SystemTime) -> Option<Duration> {
    end.duration_since(start).ok()
}

/// Format duration in a human-readable way.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
/// use smithyq::utils::format_duration;
///
/// let duration = Duration::from_secs(125);
/// assert_eq!(format_duration(duration), "2m 5s");
///
/// let duration = Duration::from_millis(1500);
/// assert_eq!(format_duration(duration), "1.5s");
/// ```
pub fn format_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let millis = duration.subsec_millis();

    if total_secs >= 3600 {
        // Hours, minutes, seconds
        let hours = total_secs / 3600;
        let minutes = (total_secs % 3600) / 60;
        let seconds = total_secs % 60;
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if total_secs >= 60 {
        // Minutes and seconds
        let minutes = total_secs / 60;
        let seconds = total_secs % 60;
        format!("{}m {}s", minutes, seconds)
    } else if total_secs > 0 {
        // Seconds and milliseconds
        if millis > 0 {
            format!("{}.{}s", total_secs, millis / 100)
        } else {
            format!("{}s", total_secs)
        }
    } else {
        // Just milliseconds
        format!("{}ms", millis)
    }
}

/// Calculate task processing rate (tasks per second).
///
/// Useful for performance monitoring and metrics.
pub fn calculate_rate(task_count: u64, duration: Duration) -> f64 {
    if duration.is_zero() {
        0.0
    } else {
        task_count as f64 / duration.as_secs_f64()
    }
}

/// Retry configuration for operations.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Base delay between retries
    pub base_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff
    pub multiplier: f64,
    /// Whether to add jitter to delays
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

impl RetryConfig {
    /// Create a new retry configuration.
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            ..Default::default()
        }
    }

    /// Set the base delay.
    pub fn with_base_delay(mut self, delay: Duration) -> Self {
        self.base_delay = delay;
        self
    }

    /// Set the maximum delay.
    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Set the backoff multiplier.
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Enable or disable jitter.
    pub fn with_jitter(mut self, enabled: bool) -> Self {
        self.jitter = enabled;
        self
    }

    /// Calculate delay for a specific attempt.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        let delay_ms =
            (self.base_delay.as_millis() as f64) * self.multiplier.powi((attempt - 1) as i32);

        let mut delay = Duration::from_millis(delay_ms as u64);
        delay = delay.min(self.max_delay);

        if self.jitter {
            // Add up to 25% jitter
            let jitter_ms = (delay.as_millis() as f64 * 0.25 * rand::random::<f64>()) as u64;
            delay += Duration::from_millis(jitter_ms);
        }

        delay
    }
}

/// Execute a function with retries.
///
/// # Examples
///
/// ```rust,no_run
/// use smithyq::utils::{retry_with_config, RetryConfig};
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = RetryConfig::new(3)
///     .with_base_delay(Duration::from_millis(100))
///     .with_max_delay(Duration::from_secs(5));
///
/// let mut counter = 0;
/// let result = retry_with_config(config, || async {
///     counter += 1;
///     // Your fallible operation here
///     if counter >= 2 {
///         Ok("Success!")
///     } else {
///         Err("Simulated failure")
///     }
/// }).await;
/// # Ok(())
/// # }
/// ```
pub async fn retry_with_config<F, Fut, T, E>(config: RetryConfig, mut operation: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut last_error = None;

    for attempt in 0..=config.max_attempts {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(error) => {
                last_error = Some(error);

                if attempt < config.max_attempts {
                    let delay = config.delay_for_attempt(attempt + 1);
                    tracing::debug!(
                        "🔄 Retry attempt {} failed, retrying in {:?}",
                        attempt + 1,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    Err(last_error.unwrap())
}

/// Simple retry with exponential backoff.
pub async fn retry<F, Fut, T, E>(max_attempts: u32, operation: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    retry_with_config(RetryConfig::new(max_attempts), operation).await
}

/// Memory usage utilities
pub mod memory {
    /// Get current memory usage of the process in bytes.
    ///
    /// Returns None if memory information is not available.
    #[cfg(target_os = "linux")]
    pub fn current_memory_usage() -> Option<usize> {
        use std::fs;

        let status = fs::read_to_string("/proc/self/status").ok()?;
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<usize>() {
                        return Some(kb * 1024); // Convert KB to bytes
                    }
                }
            }
        }
        None
    }

    #[cfg(not(target_os = "linux"))]
    pub fn current_memory_usage() -> Option<usize> {
        // Not implemented for other platforms
        None
    }

    /// Format memory size in human-readable format.
    pub fn format_memory_size(bytes: usize) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
        const THRESHOLD: f64 = 1024.0;

        let mut size = bytes as f64;
        let mut unit_index = 0;

        while size >= THRESHOLD && unit_index < UNITS.len() - 1 {
            size /= THRESHOLD;
            unit_index += 1;
        }

        if unit_index == 0 {
            format!("{} {}", bytes, UNITS[unit_index])
        } else {
            format!("{:.1} {}", size, UNITS[unit_index])
        }
    }
}

/// Task naming utilities
pub mod naming {
    /// Generate a human-readable task name from a task type.
    ///
    /// Converts snake_case and PascalCase to space-separated words.
    pub fn humanize_task_type(task_type: &str) -> String {
        let with_spaces = task_type.replace('_', " ");

        let mut result = String::new();
        let mut prev_char: Option<char> = None;

        for c in with_spaces.chars() {
            if c.is_uppercase()
                && !result.is_empty()
                && !result.ends_with(' ')
                && prev_char.map_or(false, |prev| prev.is_lowercase())
            {
                result.push(' ');
            }
            result.push(c);
            prev_char = Some(c);
        }

        result
            .split_whitespace()
            .map(|word| {
                let mut chars = word.chars();
                match chars.next() {
                    None => String::new(),
                    Some(first) => {
                        first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase()
                    }
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_calculate_backoff() {
        // First empty response for worker 0
        assert_eq!(calculate_backoff(1, 0), 200); // 100 * 2^1 + 0 * 50

        // Second empty response for worker 1
        assert_eq!(calculate_backoff(2, 1), 450); // 100 * 2^2 + 1 * 50 = 400 + 50

        // Third empty response for worker 2
        assert_eq!(calculate_backoff(3, 2), 900); // 100 * 2^3 + 2 * 50 = 800 + 100

        // High empty count should hit the cap
        assert_eq!(calculate_backoff(10, 5), 5000); // Capped at max_backoff
    }

    #[test]
    fn test_custom_backoff() {
        let backoff = calculate_custom_backoff(2, 1, 50, 1000, 25, 4);
        assert_eq!(backoff, 225); // 50 * 2^2 + 1 * 25 = 200 + 25
    }

    #[test]
    fn test_linear_backoff() {
        let backoff = calculate_linear_backoff(3, 2, 100, 50, 1000);
        assert_eq!(backoff, 350); // 100 + 3 * 50 + 2 * 50 = 100 + 150 + 100
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_millis(500)), "500ms");
        assert_eq!(format_duration(Duration::from_secs(5)), "5s");
        assert_eq!(format_duration(Duration::from_secs(65)), "1m 5s");
        assert_eq!(format_duration(Duration::from_secs(3665)), "1h 1m 5s");
    }

    #[test]
    fn test_calculate_rate() {
        let rate = calculate_rate(100, Duration::from_secs(10));
        assert_eq!(rate, 10.0);

        let rate = calculate_rate(0, Duration::from_secs(10));
        assert_eq!(rate, 0.0);

        let rate = calculate_rate(100, Duration::ZERO);
        assert_eq!(rate, 0.0);
    }

    #[test]
    fn test_retry_config() {
        let config = RetryConfig::new(5)
            .with_base_delay(Duration::from_millis(100))
            .with_multiplier(2.0)
            .with_jitter(false);

        assert_eq!(config.delay_for_attempt(0), Duration::ZERO);
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(200));
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(400));
    }

    #[test]
    fn test_memory_format() {
        use memory::format_memory_size;

        assert_eq!(format_memory_size(512), "512 B");
        assert_eq!(format_memory_size(1536), "1.5 KB"); // 1.5 * 1024
        assert_eq!(format_memory_size(2_097_152), "2.0 MB"); // 2 * 1024 * 1024
    }

    #[test]
    fn test_humanize_task_type() {
        use naming::humanize_task_type;

        assert_eq!(humanize_task_type("send_email"), "Send Email");
        assert_eq!(humanize_task_type("ProcessImageTask"), "Process Image Task");
        assert_eq!(humanize_task_type("simple"), "Simple");
        assert_eq!(humanize_task_type("UPPER_CASE"), "Upper Case");
    }

    #[test]
    fn test_task_id_generation() {
        let id1 = generate_task_id();
        let id2 = generate_task_id();

        assert_ne!(id1, id2);
        assert!(id1.len() > 0);

        let prefixed_id = generate_task_id_with_prefix("test");
        assert!(prefixed_id.starts_with("test_"));
    }

    #[tokio::test]
    async fn test_retry_success() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let attempts = Rc::new(RefCell::new(0));
        let attempts_clone = attempts.clone();

        let result = retry(3, move || {
            let attempts = attempts_clone.clone();
            async move {
                *attempts.borrow_mut() += 1;
                if *attempts.borrow() >= 2 {
                    Ok("success")
                } else {
                    Err("failure")
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), "success");
        assert_eq!(*attempts.borrow(), 2);
    }

    #[tokio::test]
    async fn test_retry_failure() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let attempts = Rc::new(RefCell::new(0));
        let attempts_clone = attempts.clone();

        let result: Result<(), &str> = retry(2, move || {
            let attempts = attempts_clone.clone();
            async move {
                *attempts.borrow_mut() += 1;
                Err("always fails")
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(*attempts.borrow(), 3); // Original attempt + 2 retries
    }
}

// Add required dependency for random jitter
#[cfg(feature = "jitter")]
mod rand {
    /// Simple linear congruential generator for jitter
    /// This avoids adding the full rand crate dependency
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEED: AtomicU64 = AtomicU64::new(1);

    pub fn random<T: From<f64>>() -> T {
        let prev = SEED.load(Ordering::Relaxed);
        let next = prev.wrapping_mul(1103515245).wrapping_add(12345);
        SEED.store(next, Ordering::Relaxed);

        // Convert to 0.0..1.0 range
        let normalized = (next as f64) / (u64::MAX as f64);
        T::from(normalized)
    }
}

// Fallback for when jitter feature is not enabled
#[cfg(not(feature = "jitter"))]
mod rand {
    pub fn random<T: From<f64>>() -> T {
        T::from(0.5) // Fixed value when jitter is disabled
    }
}
