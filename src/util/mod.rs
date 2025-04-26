pub mod blueprint;
pub mod docker;
pub mod stdout_buffer;

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use strum::FromRepr;

// structs

/// Best and average fitness scores of a single generation,
/// evaluated in the broker and sent to the client after each generation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct PopEvaluation {
    pub best_fitness: f64,
    pub avg_fitness: f64,
}

/// Enum to represent the status of a squid node
#[derive(Clone, Copy, Debug, Serialize, Deserialize, FromRepr)]
#[repr(u8)]
pub enum NodeStatus {
    Idle,
    Active,
    Pulling,
    Crashed,
}

// util functions

pub fn env(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("${} not set", key))
}

pub fn de_usize(bytes: &[u8]) -> Result<usize> {
    Ok(de_u32(bytes)? as usize)
}

pub fn de_u8(bytes: &[u8]) -> Result<u8> {
    let len = bytes.len();
    let bytes: [u8; 1] = bytes
        .try_into()
        .map_err(|_| anyhow!("Invalid slice length for u8 conversion: {}", len))?;
    Ok(u8::from_be_bytes(bytes))
}

pub fn de_u32(bytes: &[u8]) -> Result<u32> {
    let len = bytes.len();
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| anyhow!("Invalid slice length for u32 conversion: {}", len))?;
    Ok(u32::from_be_bytes(bytes))
}

pub fn de_u64(bytes: &[u8]) -> Result<u64> {
    let len = bytes.len();
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow!("Invalid slice length for u64 conversion: {}", len))?;
    Ok(u64::from_be_bytes(bytes))
}

pub fn de_f64(bytes: &[u8]) -> Result<f64> {
    let len = bytes.len();
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow!("Invalid slice length for f64 conversion: {}", len))?;
    Ok(f64::from_be_bytes(bytes))
}

#[macro_export]
macro_rules! bail_assert {
    ($cond:expr) => {
        if !$cond {
            bail!("Assertion failed: {}", stringify!($cond));
        }
    };
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            bail!($($arg)+);
        }
    };
}
