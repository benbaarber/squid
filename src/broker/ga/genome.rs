use anyhow::bail;
use rand::{Rng, SeedableRng, rngs::StdRng};
use rand_distr::{Bernoulli, Distribution, StandardNormal, Uniform};
use serde::{Deserialize, Serialize};

use crate::util::blueprint::{CrossoverMethod, NN};

pub trait Species: Clone + Send + TryFrom<NN> + Serialize {
    type Genome: Genome<Species = Self>;
    fn random_genome(&self, seed: Option<u64>) -> Self::Genome;
    fn zeroed_genome(&self) -> Self::Genome;
}

pub trait Genome: Clone + Send + Serialize {
    type Species: Species<Genome = Self>;
    fn mutate(&mut self, probability: f64, magnitude: f64);
    fn crossover(&self, other: &Self, method: CrossoverMethod) -> Self;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CTRNNGenome {
    taus: Vec<f64>,
    weights: Vec<Vec<f64>>,
    biases: Vec<f64>,
    gains: Vec<f64>,
}

impl Genome for CTRNNGenome {
    type Species = CTRNNSpecies;

    fn mutate(&mut self, probability: f64, magnitude: f64) {
        let mut rng = rand::rng();
        let bern = Bernoulli::new(probability).unwrap();

        for tau in &mut self.taus {
            if bern.sample(&mut rng) {
                *tau += magnitude * rng.sample::<f64, _>(StandardNormal);
                *tau = tau.clamp(0.001, 16.0);
            }
        }

        for bias in &mut self.biases {
            if bern.sample(&mut rng) {
                *bias += magnitude * rng.sample::<f64, _>(StandardNormal);
                *bias = bias.clamp(-16.0, 16.0);
            }
        }

        for weight in self.weights.iter_mut().flatten() {
            if bern.sample(&mut rng) {
                *weight += magnitude * rng.sample::<f64, _>(StandardNormal);
                *weight = weight.clamp(-16.0, 16.0);
            }
        }
    }

    fn crossover(&self, other: &Self, method: CrossoverMethod) -> Self {
        let mut rng = rand::rng();
        match method {
            CrossoverMethod::Uniform => {
                let mut child = self.clone();
                let bern = Bernoulli::new(0.5).unwrap();

                for (i, tau) in child.taus.iter_mut().enumerate() {
                    if bern.sample(&mut rng) {
                        *tau = other.taus[i];
                    }
                }
                for (i, row) in child.weights.iter_mut().enumerate() {
                    for (j, weight) in row.iter_mut().enumerate() {
                        if bern.sample(&mut rng) {
                            *weight = other.weights[i][j];
                        }
                    }
                }
                for (i, bias) in child.biases.iter_mut().enumerate() {
                    if bern.sample(&mut rng) {
                        *bias = other.biases[i];
                    }
                }

                child
            }
            CrossoverMethod::NPoint { n } => {
                let mut child = self.clone();
                let mut tau_ixs = (0..n)
                    .map(|_| rng.random_range(0..self.taus.len()))
                    .collect::<Vec<_>>();
                tau_ixs.sort_unstable();
                let mut swap = false;
                let mut last = 0;
                for ix in tau_ixs {
                    if swap {
                        for i in last..ix {
                            child.taus[i] = other.taus[i];
                        }
                    }
                    swap = !swap;
                    last = ix;
                }

                let mut bias_ixs = (0..n)
                    .map(|_| rng.random_range(0..self.biases.len()))
                    .collect::<Vec<_>>();
                bias_ixs.sort_unstable();
                let mut swap = false;
                let mut last = 0;
                for ix in bias_ixs {
                    if swap {
                        for i in last..ix {
                            child.biases[i] = other.biases[i];
                        }
                    }
                    swap = !swap;
                    last = ix;
                }

                let mut weight_ixs = (0..n)
                    .map(|_| rng.random_range(0..self.weights.len()))
                    .collect::<Vec<_>>();
                weight_ixs.sort_unstable();
                let mut swap = false;
                let mut last = 0;
                for ix in weight_ixs {
                    if swap {
                        for i in last..ix {
                            child.weights[i].clone_from(&other.weights[i]);
                        }
                    }
                    swap = !swap;
                    last = ix;
                }

                child
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CTRNNSpecies {
    input_size: usize,
    hidden_size: usize,
    output_size: usize,
    step_size: f64,
}

impl TryFrom<NN> for CTRNNSpecies {
    type Error = anyhow::Error;

    #[allow(irrefutable_let_patterns)]
    fn try_from(value: NN) -> Result<Self, Self::Error> {
        if let NN::CTRNN {
            input_size,
            hidden_size,
            output_size,
            step_size,
        } = value
        {
            Ok(Self {
                input_size,
                hidden_size,
                output_size,
                step_size,
            })
        } else {
            bail!(
                "Failed to convert NN to CTRNNSpecies. Expected variant NN::CTRNN, got {:?}",
                value
            );
        }
    }
}

impl Species for CTRNNSpecies {
    type Genome = CTRNNGenome;

    fn random_genome(&self, seed: Option<u64>) -> Self::Genome {
        let mut rng = match seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_os_rng(),
        };
        let size = self.input_size + self.hidden_size + self.output_size;
        let tau_distr = Uniform::new(0.001, 16.0).unwrap();
        let weight_distr = Uniform::new(-16.0, 16.0).unwrap();

        CTRNNGenome {
            taus: (0..size)
                .map(|_| rng.sample::<f64, _>(&tau_distr))
                .collect(),
            weights: (0..size)
                .map(|_| {
                    (0..size)
                        .map(|_| rng.sample::<f64, _>(&weight_distr))
                        .collect()
                })
                .collect(),
            biases: (0..size)
                .map(|_| rng.sample::<f64, _>(&weight_distr))
                .collect(),
            gains: vec![1.0; size],
        }
    }

    fn zeroed_genome(&self) -> Self::Genome {
        let size = self.input_size + self.hidden_size + self.output_size;

        CTRNNGenome {
            taus: vec![0.0; size],
            weights: vec![vec![0.0; size]; size],
            biases: vec![0.0; size],
            gains: vec![1.0; size],
        }
    }
}
