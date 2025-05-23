use std::{collections::HashMap, path::PathBuf};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::bail_assert;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExperimentConfig {
    pub image: String,
    pub out_dir: PathBuf,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum NN {
    CTRNN {
        input_size: usize,
        hidden_size: usize,
        output_size: usize,
        step_size: f64,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SelectionMethod {
    Softmax,
    Tournament { size: u32 },
    Roulette,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CrossoverMethod {
    Uniform,
    NPoint { n: u32 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum InitMethod {
    Flat,
    Random { seed: Option<u64> },
    Seeded { dir: PathBuf, mutate: bool },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GAConfig {
    pub population_size: usize,
    pub num_generations: usize,
    pub elitism_fraction: f64,
    pub random_fraction: f64,
    pub mutation_probability: f64,
    pub mutation_magnitude: f64,
    pub selection_fraction: f64,
    pub selection_method: SelectionMethod,
    pub crossover_probability: f64,
    pub crossover_method: Option<CrossoverMethod>,
    pub fitness_threshold: Option<f64>,
    pub init_method: InitMethod,
    pub save_every: usize,
    pub save_fraction: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Blueprint {
    pub experiment: ExperimentConfig,
    pub nn: NN,
    pub ga: GAConfig,
    pub csv_data: Option<HashMap<String, Vec<String>>>,
}

impl Blueprint {
    pub fn validate(&self) -> Result<()> {
        // NN

        // min param length to be upper bound for n-point crossover
        let min_param_len = match self.nn {
            NN::CTRNN {
                input_size,
                hidden_size,
                output_size,
                step_size,
            } => {
                let total_size = input_size + hidden_size + output_size;
                bail_assert!(
                    total_size > 0,
                    "total size (nn.input_size + nn.hidden_size + nn.output_size) must be at least 1"
                );
                bail_assert!(step_size > 0.0, "step_size must be greater than 0");
                total_size
            }
        };

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
            self.ga.elitism_fraction >= 0.0 && self.ga.elitism_fraction <= 1.0,
            "ga.elitism_fraction must be between 0 and 1"
        );
        bail_assert!(
            self.ga.random_fraction >= 0.0 && self.ga.random_fraction <= 1.0,
            "ga.random_fraction must be between 0 and 1"
        );
        bail_assert!(
            self.ga.elitism_fraction + self.ga.random_fraction <= 1.0,
            "sum of ga.elitism_fraction and ga.random_fraction must be <= 1.0"
        );
        bail_assert!(
            self.ga.mutation_probability >= 0.0 && self.ga.mutation_probability <= 1.0,
            "ga.mutation_probability must be between 0 and 1"
        );
        bail_assert!(
            self.ga.mutation_magnitude >= 0.0,
            "ga.mutation_magnitude must be non-negative"
        );
        bail_assert!(
            self.ga.selection_fraction >= 0.0 && self.ga.selection_fraction <= 1.0,
            "ga.selection_fraction must be between 0 and 1"
        );
        if let Some(CrossoverMethod::NPoint { n }) = self.ga.crossover_method {
            bail_assert!(
                n < min_param_len as u32,
                "ga.crossover_method NPoint.n must be less than the smallest parameter array ({})",
                min_param_len
            );
        }
        if let SelectionMethod::Tournament { size } = self.ga.selection_method {
            bail_assert!(
                size > 0,
                "ga.selection_method Tournament.size must be greater than 0"
            )
        }
        if self.ga.crossover_probability > 0.0 {
            bail_assert!(
                self.ga.crossover_method.is_some(),
                "ga.crossover_method must be defined if ga.crossover_probability is greater than 0"
            );
        }
        bail_assert!(
            self.ga.save_fraction >= 0.0 && self.ga.save_fraction <= 1.0,
            "ga.save_fraction must be between 0 and 1"
        );
        bail_assert!(
            self.ga.save_every <= self.ga.num_generations,
            "ga.save_every must be less than or equal to ga.num_generations"
        );

        Ok(())
    }
}
