pub mod blueprint;
pub mod docker;
pub mod stdout_buffer;
pub mod zft;

use std::{
    any::type_name,
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result, anyhow};
use bincode::config::{BigEndian, Configuration};
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

/// Experiment history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpHistory {
    pub start: u64,
    pub best_fitness_vec: Vec<f64>,
    pub avg_fitness_vec: Vec<f64>,
}

#[derive(Debug)]
pub struct AtomicBoolRelaxed {
    pub inner: AtomicBool,
}

impl AtomicBoolRelaxed {
    pub fn new(val: bool) -> Self {
        Self {
            inner: AtomicBool::new(val),
        }
    }
    pub fn load(&self) -> bool {
        self.inner.load(Ordering::Relaxed)
    }

    pub fn store(&self, val: bool) {
        self.inner.store(val, Ordering::Relaxed)
    }
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

pub fn bincode_serialize<T: Serialize>(val: T) -> Result<Vec<u8>> {
    bincode::serde::encode_to_vec(val, BINCODE_CONF)
        .with_context(|| format!("Failed to serialize type {} to binary", type_name::<T>()))
}

pub fn bincode_deserialize<T: DeserializeOwned>(val: &[u8]) -> Result<T> {
    Ok(bincode::serde::decode_from_slice(val, BINCODE_CONF)
        .with_context(|| {
            format!(
                "Failed to deserialize type {} from binary",
                type_name::<T>()
            )
        })?
        .0)
}

pub fn create_exp_dir(path: &Path, id_x: &str, blueprint: &Blueprint) -> Result<()> {
    let mut out_dir = path.to_path_buf();

    // Create outdir

    if !out_dir.exists() {
        fs::create_dir_all(&out_dir)?;
    }
    // let timestamp = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();
    // out_dir.push(timestamp);
    out_dir.push(id_x);
    fs::create_dir(&out_dir)?;
    fs::write(out_dir.join("EXPERIMENT_ID"), id_x)?;
    fs::create_dir(out_dir.join("agents"))?;
    fs::write(
        out_dir.join("population_parameters.txt"),
        format!("{:#?}", &blueprint.ga),
    )?;

    let data_dir = out_dir.join("data");
    fs::create_dir(&data_dir)?;
    fs::write(data_dir.join("fitness.csv"), "avg,best\n")?;

    if let Some(csv_data) = &blueprint.csv_data {
        for (name, headers) in csv_data {
            fs::write(
                data_dir.join(name).with_extension("csv"),
                format!("gen,{}\n", headers.join(",")),
            )?;
        }
    }

    Ok(())
}

/// deserialize u32 to usize
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
