use std::time::Duration;

use convoyx::{BackoffStrategy, RetryPolicy};

#[test]
fn fixed_backoff_returns_constant_delay_when_under_max_attempts() {
    let policy = RetryPolicy {
        max_attempts: 3,
        backoff: BackoffStrategy::Fixed(Duration::from_secs(5)),
    };

    assert_eq!(policy.next_delay(0), Some(Duration::from_secs(5)));
    assert_eq!(policy.next_delay(1), Some(Duration::from_secs(5)));
    assert_eq!(policy.next_delay(2), Some(Duration::from_secs(5)));
    assert_eq!(policy.next_delay(3), None);
}

#[test]
fn exponential_backoff_grows_and_clamps() {
    let base = Duration::from_secs(1);
    let max = Duration::from_secs(10);
    let policy = RetryPolicy {
        max_attempts: 5,
        backoff: BackoffStrategy::Exponential { base, max },
    };

    // attempt = 0 -> 2^0 * base = 1s
    assert_eq!(policy.next_delay(0), Some(Duration::from_secs(1)));
    // attempt = 1 -> 2^1 * base = 2s
    assert_eq!(policy.next_delay(1), Some(Duration::from_secs(2)));
    // attempt = 2 -> 2^2 * base = 4s
    assert_eq!(policy.next_delay(2), Some(Duration::from_secs(4)));
    // attempt = 3 -> 2^3 * base = 8s (still under max)
    assert_eq!(policy.next_delay(3), Some(Duration::from_secs(8)));
    // attempt = 4 -> 2^4 * base = 16s, but clamped to 10s max
    assert_eq!(policy.next_delay(4), Some(Duration::from_secs(10)));
    // attempt >= max_attempts -> None
    assert_eq!(policy.next_delay(5), None);
}
