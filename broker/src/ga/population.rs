use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use serde_json::Result;

use super::{genome::Species, Agent};

pub trait GenericPopulation {
    fn config(&self) -> PopulationConfig;
    fn pack_agent(&self, ix: usize) -> Result<Vec<u8>>;
    fn update_agent_score(&mut self, ix: usize, score: f64);
    fn evolve(&mut self);
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct PopulationConfig {
    pub population_size: usize,
    pub num_generations: usize,
    pub elitism_percent: f64,
    pub random_percent: f64,
    pub mutation_chance: f64,
    pub mutation_magnitude: f64,
}

impl Default for PopulationConfig {
    fn default() -> Self {
        Self {
            population_size: 100,
            num_generations: 25,
            elitism_percent: 0.1,
            random_percent: 0.0,
            mutation_chance: 0.2,
            mutation_magnitude: 0.8,
        }
    }
}

#[derive(Clone)]
pub struct Population<S: Species> {
    species: S,
    agents: Vec<Agent<S::Genome>>,
    config: PopulationConfig,
}

impl<S: Species> Population<S> {
    pub fn new(
        mut config: PopulationConfig,
        species: S,
        seeds: Option<Vec<Agent<S::Genome>>>,
    ) -> Self {
        let agents = match seeds {
            Some(mut agents) => {
                if agents.len() < config.population_size {
                    // Spawn remaining agents to reach intended population size
                    let num_seeds = agents.len();
                    let mut rng = rand::thread_rng();
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
        // let agents = vec![Agent::new(); config.population_size];

        Self {
            agents,
            species,
            config,
        }
    }

    /// Sort agents by fitness
    fn sort_agents(&mut self) {
        self.agents.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .expect("There are no NaN agent scores")
        });
    }
}

impl<S: Species> GenericPopulation for Population<S> {
    fn config(&self) -> PopulationConfig {
        self.config
    }

    fn pack_agent(&self, ix: usize) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.agents[ix].genome)
    }

    fn update_agent_score(&mut self, ix: usize, score: f64) {
        self.agents[ix].score = score;
    }

    fn evolve(&mut self) {
        self.sort_agents();

        let num_elites = (self.agents.len() as f64 * self.config.elitism_percent)
            .round()
            .max(1.0) as usize;
        let num_randoms = (self.agents.len() as f64 * self.config.random_percent).round() as usize;
        let random_start_ix = self.agents.len() - num_randoms;
        let mut rng = rand::thread_rng();

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

        for _ in random_start_ix..self.agents.len() {
            // self.agents[i] = Agent::new(self.model_config.build());
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     fn test_agent() -> Agent {
//         let model = CTRNN::new(1, 1, 1, 0.1);
//         Agent::new(model)
//     }

//     #[test]
//     fn sort_agents() {
//         let mut population = Population::new(
//             vec![test_agent(), test_agent()],
//             Fitness,
//             PopulationConfig::default(),
//         );
//         population.agents[0].score = 1.0;
//         population.agents[1].score = 0.0;
//         population.sort_agents();
//         assert_eq!(population.agents[0].score, 0.0);
//         assert_eq!(population.agents[1].score, 1.0);
//     }
// }
