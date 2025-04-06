use anyhow::Result;
use rand::seq::IndexedRandom;
use shared::{GAConfig, PopEvaluation};

use super::{agent::Agent, genome::Species};

pub trait GenericPopulation {
    fn evaluate(&self) -> PopEvaluation;
    fn evolve(&mut self);
    fn pack_agent(&self, ix: usize) -> Result<Vec<u8>>;
    fn pack_save(&self) -> Result<Vec<u8>>;
    fn update_agent_fitness(&mut self, ix: usize, fitness: f64);
}

#[derive(Clone)]
pub struct Population<S: Species> {
    species: S,
    agents: Vec<Agent<S::Genome>>,
    config: GAConfig,
}

impl<S: Species> Population<S> {
    pub fn new(mut config: GAConfig, species: S, seeds: Option<Vec<Agent<S::Genome>>>) -> Self {
        let agents = match seeds {
            Some(mut agents) => {
                if agents.len() < config.population_size {
                    // Spawn remaining agents to reach intended population size
                    let num_seeds = agents.len();
                    let mut rng = rand::rng();
                    while agents.len() < config.population_size {
                        let parent = agents[..num_seeds]
                            .choose(&mut rng)
                            .expect("Agents vec is not empty");
                        let child = parent.clone_and_mutate(
                            &species,
                            config.mutation_chance,
                            config.mutation_magnitude,
                        );
                        agents.push(child);
                    }
                } else if agents.len() > config.population_size {
                    // log::warn!("Population was given {0} seed agents but population size was set to {1}. Increasing population size to {0}.", agents.len(), config.population_size);
                    config.population_size = agents.len();
                }

                agents
            }
            None => (0..config.population_size)
                .map(|_| Agent::new(species.random_genome()))
                .collect(),
        };

        Self {
            agents,
            species,
            config,
        }
    }

    /// Sort agents by fitness
    fn sort_agents(&mut self) {
        self.agents.sort_by(|a, b| {
            a.fitness
                .partial_cmp(&b.fitness)
                .expect("There are no NaN agent fitness values")
        });
    }
}

impl<S: Species> GenericPopulation for Population<S> {
    fn evaluate(&self) -> PopEvaluation {
        let best_fitness = self
            .agents
            .iter()
            .map(|x| x.fitness)
            .reduce(f64::max)
            .expect("Agents vec is not empty");

        let avg_fitness =
            self.agents.iter().map(|x| x.fitness).sum::<f64>() / self.agents.len() as f64;

        PopEvaluation {
            best_fitness,
            avg_fitness,
        }
    }

    fn evolve(&mut self) {
        self.sort_agents();

        let num_elites = (self.agents.len() as f64 * self.config.elitism_percent)
            .round()
            .max(1.0) as usize;
        let num_randoms = (self.agents.len() as f64 * self.config.random_percent).round() as usize;
        let random_start_ix = self.agents.len() - num_randoms;
        let mut rng = rand::rng();

        for i in num_elites..random_start_ix {
            let parent = self.agents[..num_elites]
                .choose(&mut rng)
                .expect("Agents vec is not empty");

            self.agents[i] = parent.clone_and_mutate(
                &self.species,
                self.config.mutation_chance,
                self.config.mutation_magnitude,
            );
        }

        for i in random_start_ix..self.agents.len() {
            self.agents[i] = Agent::new(self.species.random_genome());
        }
    }

    fn pack_agent(&self, ix: usize) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&self.agents[ix].genome)?)
    }

    fn pack_save(&self) -> Result<Vec<u8>> {
        let num_save = (self.agents.len() as f64 * self.config.save_percent)
            .round()
            .max(1.0) as usize;
        let packed_agents = self
            .agents
            .iter()
            .take(num_save)
            .map(|a| serde_json::to_string(&a.genome))
            .collect::<serde_json::Result<Vec<_>>>()?;
        let pack = serde_json::to_vec(&packed_agents)?;
        Ok(pack)
    }

    fn update_agent_fitness(&mut self, ix: usize, fitness: f64) {
        self.agents[ix].fitness = fitness;
    }
}
