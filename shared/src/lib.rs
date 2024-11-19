use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// structs

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct GAConfig {
    pub population_size: usize,
    pub num_generations: usize,
    pub elitism_percent: f64,
    pub random_percent: f64,
    pub mutation_chance: f64,
    pub mutation_magnitude: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Blueprint {
    pub task_image: String,
    pub genus: String,
    pub species: Value,
    pub seeds: Option<Value>,
    pub ga_config: GAConfig,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Summary {
    pub score: f64,
}

// util functions

pub fn de_usize(frame: &[u8]) -> Result<usize> {
    let bytes: [u8; 4] = frame
        .try_into()
        .map_err(|_| anyhow!("Invalid message length for u32 conversion"))?;
    Ok(u32::from_le_bytes(bytes) as usize)
}
