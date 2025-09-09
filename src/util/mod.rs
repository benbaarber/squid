pub mod blueprint;
pub mod docker;
pub mod stdout_buffer;
pub mod zft;

use std::{collections::HashMap, path::PathBuf};

use anyhow::{Context, Result, anyhow};
use bincode::{
    config::{BigEndian, Configuration},
    error::{DecodeError, EncodeError},
};
use num_enum::{FromPrimitive, IntoPrimitive};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::blueprint::Blueprint;

// consts

pub const BINCODE_CONF: Configuration<BigEndian> = bincode::config::standard().with_big_endian();

// traits

/// Extension trait for `zmq::Socket` to send and receive single bytes more ergonomically
pub trait SocketByteExt {
    fn send_byte<T: Into<u8>>(&self, data: T, flags: i32) -> zmq::Result<()>;
    fn recv_byte(&self, flags: i32) -> zmq::Result<u8>;
}

impl SocketByteExt for zmq::Socket {
    #[inline]
    fn send_byte<T: Into<u8>>(&self, data: T, flags: i32) -> zmq::Result<()> {
        let b: u8 = data.into();
        self.send(&[b][..], flags)
    }

    #[inline]
    fn recv_byte(&self, flags: i32) -> zmq::Result<u8> {
        let mut b = [0u8; 1];
        self.recv_into(&mut b, flags)?;
        Ok(b[0])
    }
}

// structs

/// Experiment metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Experiment {
    pub id: u64,
    pub blueprint: Blueprint,
    pub url: String,
    pub out_dir: PathBuf,
    pub is_new: bool,
    pub is_local: bool,
    pub is_test: bool,
}

/// Best and average fitness scores of a single generation,
/// evaluated in the broker and sent to the client after each generation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct PopEvaluation {
    pub best_fitness: f64,
    pub avg_fitness: f64,
}

/// Enum to represent the status of a squid node
#[derive(Debug, FromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum NodeStatus {
    Idle,
    Active,
    Pulling,
    Crashed,
    #[default]
    Noop,
}

// util functions

pub fn env(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("${} not set", key))
}

pub fn bincode_serialize<T: Serialize>(val: T) -> std::result::Result<Vec<u8>, EncodeError> {
    bincode::serde::encode_to_vec(val, BINCODE_CONF)
}

pub fn bincode_deserialize<T: DeserializeOwned>(val: &[u8]) -> std::result::Result<T, DecodeError> {
    Ok(bincode::serde::decode_from_slice(&val, BINCODE_CONF)?.0)
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

pub fn broadcast_router<T>(
    map: &HashMap<u64, T>,
    router: &zmq::Socket,
    msgb: &[impl Into<zmq::Message> + Clone],
) -> Result<()> {
    for id in map.keys() {
        router.send(id.to_be_bytes().as_slice(), zmq::SNDMORE)?;
        router.send_multipart(msgb, 0)?;
    }

    Ok(())
}

#[macro_export]
macro_rules! bail_assert {
    ($cond:expr) => {
        if !$cond {
            anyhow::bail!("Assertion failed: {}", stringify!($cond));
        }
    };
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            anyhow::bail!($($arg)+);
        }
    };
}
