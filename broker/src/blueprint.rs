use serde::{Deserialize, Serialize};

use crate::ga::PopulationConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blueprint {
    pub task_image: String,
    pub genus: String,
    pub species: String,
    pub seeds: Option<String>,
    pub ga_config: PopulationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerResult {
    pub score: f64,
}
