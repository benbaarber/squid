use rand::Rng;
use rand_distr::{Bernoulli, Distribution, Normal, StandardNormal, Uniform};
use serde::{Deserialize, Serialize};

pub trait Species: Clone + Serialize {
    type Genome: Genome<Species = Self>;
    fn random_genome(&self) -> Self::Genome;
}

pub trait Genome: Clone + Serialize {
    type Species: Species<Genome = Self>;
    fn mutate(&mut self, species: &Self::Species, chance: f64, magnitude: f64);
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

    fn mutate(&mut self, species: &CTRNNSpecies, chance: f64, magnitude: f64) {
        let mut rng = rand::rng();
        let bern = Bernoulli::new(chance).unwrap();

        if bern.sample(&mut rng) {
            let (min_tau, max_tau) = species.tau_bounds;
            for tau in &mut self.taus {
                *tau += magnitude * rng.sample::<f64, _>(StandardNormal);
                *tau = tau.clamp(min_tau, max_tau);
            }
        }

        if bern.sample(&mut rng) {
            let (min_bias, max_bias) = species.bias_bounds;
            for bias in &mut self.biases {
                *bias += magnitude * rng.sample::<f64, _>(StandardNormal);
                *bias = bias.clamp(min_bias, max_bias);
            }
        }

        if bern.sample(&mut rng) {
            let (min_weight, max_weight) = species.weight_bounds;
            for weight in self.weights.iter_mut().flatten() {
                *weight += magnitude * rng.sample::<f64, _>(StandardNormal);
                *weight = weight.clamp(min_weight, max_weight);
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
    tau_bounds: (f64, f64),
    weight_bounds: (f64, f64),
    bias_bounds: (f64, f64),
    gain_bounds: (f64, f64),
}

impl Species for CTRNNSpecies {
    type Genome = CTRNNGenome;

    fn random_genome(&self) -> CTRNNGenome {
        let size = self.input_size + self.hidden_size + self.output_size;
        let mut rng = rand::rng();
        let normal = Normal::new(1.0, 0.333).unwrap();
        let tau_distr = Uniform::new(1e-5, 16.0).unwrap();
        let weight_distr = Uniform::new(-16.0, 16.0).unwrap();

        let (min_tau, max_tau) = self.tau_bounds;
        let (min_weight, max_weight) = self.weight_bounds;
        let (min_bias, max_bias) = self.bias_bounds;

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
}
