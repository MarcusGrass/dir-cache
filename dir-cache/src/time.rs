use crate::error::{Error, Result};
use std::time::{Duration, SystemTime};

pub(crate) fn duration_from_nano_string(input: &str) -> Result<Duration> {
    let epoch_nanos: u128 = input
        .parse()
        .map_err(|_| Error::ParseMetadata(format!("Failed to parse timestamp from {input}")))?;
    duration_from_nanos(epoch_nanos)
}

pub(crate) fn duration_from_nanos(nanos: u128) -> Result<Duration> {
    let secs = nanos / 1_000_000_000u128;
    let nanos = nanos % 1_000_000_000u128;
    Ok(Duration::new(
        u64::try_from(secs).map_err(|_| Error::Arithmetic("Seconds too high to fit in a u64"))?,
        u32::try_from(nanos).map_err(|_| Error::Arithmetic("Nanos to high to fit in a u32"))?,
    ))
}

#[inline]
pub(crate) fn unix_time_now() -> Result<Duration> {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(Error::SystemTime)
}
