pub mod docker;
pub mod stdout_buffer;

use std::{collections::HashMap, path::PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use strum::FromRepr;
use toml::Value;

// structs

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExperimentConfig {
    pub task_image: String,
    pub genus: String,
    pub seed_dir: Option<PathBuf>,
    pub out_dir: PathBuf,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct GAConfig {
    pub population_size: usize,
    pub num_generations: usize,
    pub elitism_percent: f64,
    pub random_percent: f64,
    pub mutation_chance: f64,
    pub mutation_magnitude: f64,
    pub save_every: usize,
    pub save_percent: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Blueprint {
    pub experiment: ExperimentConfig,
    pub species: Value,
    pub ga: GAConfig,
    pub csv_data: Option<HashMap<String, Vec<String>>>,
}

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

const GENUSES: [&str; 1] = ["CTRNN"];

// TODO: pull this out into a shared library with broker instead of duplicating it
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct CTRNNSpecies {
    input_size: usize,
    hidden_size: usize,
    output_size: usize,
    step_size: f64,
    tau_bounds: (f64, f64),
    weight_bounds: (f64, f64),
    bias_bounds: (f64, f64),
    gain_bounds: (f64, f64),
}

impl Blueprint {
    pub fn validate(&self) -> Result<()> {
        // Species

        match self.experiment.genus.as_str() {
            "CTRNN" => {
                let species = CTRNNSpecies::deserialize(self.species.clone())
                    .context("Invalid CTRNN species")?;
                let total_size = species.input_size + species.hidden_size + species.output_size;
                bail_assert!(
                    total_size > 0,
                    "total size (species.input_size + species.hidden_size + species.output_size) must be at least 1"
                );
                bail_assert!(
                    species.step_size > 0.0,
                    "species.step_size must be greater than 0"
                );
                bail_assert!(
                    species.tau_bounds.0 <= species.tau_bounds.1,
                    "species.tau_bounds must be a valid range"
                );
                bail_assert!(
                    species.weight_bounds.0 <= species.weight_bounds.1,
                    "species.weight_bounds must be a valid range"
                );
                bail_assert!(
                    species.bias_bounds.0 <= species.bias_bounds.1,
                    "species.bias_bounds must be a valid range"
                );
                bail_assert!(
                    species.gain_bounds.0 <= species.gain_bounds.1,
                    "species.gain_bounds must be a valid range"
                );
            }
            _ => bail!("experiment.genus must be one of {:?}", GENUSES),
        }

        // GA

        bail_assert!(
            self.ga.population_size > 0,
            "ga.population_size must be at least 1"
        );
        bail_assert!(
            self.ga.num_generations > 0,
            "ga.num_generations must be at least 1"
        );
        bail_assert!(
            self.ga.elitism_percent >= 0.0 && self.ga.elitism_percent <= 1.0,
            "ga.elitism_percent must be between 0 and 1"
        );
        bail_assert!(
            self.ga.random_percent >= 0.0 && self.ga.random_percent <= 1.0,
            "ga.random_percent must be between 0 and 1"
        );
        bail_assert!(
            self.ga.mutation_chance >= 0.0 && self.ga.mutation_chance <= 1.0,
            "ga.mutation_chance must be between 0 and 1"
        );
        bail_assert!(
            self.ga.mutation_magnitude >= 0.0,
            "ga.mutation_magnitude must be non-negative"
        );
        bail_assert!(
            self.ga.save_percent >= 0.0 && self.ga.save_percent <= 1.0,
            "ga.save_percent must be between 0 and 1"
        );

        Ok(())
    }
}

impl GAConfig {
    pub fn display(&self) -> String {
        format!(
            "Population size: {}\nNum generations: {}\nElitism percent: {}\nRandom percent: {}\nMutation chance: {}\nMutation magnitude: {}",
            self.population_size,
            self.num_generations,
            self.elitism_percent,
            self.random_percent,
            self.mutation_chance,
            self.mutation_magnitude
        )
    }
}
