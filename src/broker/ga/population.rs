use crate::util::{
    PopEvaluation,
    blueprint::{GAConfig, InitMethod},
};
use anyhow::{Result, bail};
use rand::seq::IndexedRandom;
use rand_distr::{Bernoulli, Distribution};

use super::{agent::Agent, genome::Species, selector::Selector};

pub trait GenericPopulation: Send {
    fn evaluate(&self) -> PopEvaluation;
    fn evolve(&mut self) -> Result<()>;
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
    pub fn new(config: GAConfig, species: S, seeds: Option<Vec<Agent<S::Genome>>>) -> Result<Self> {
        let agents = match config.init_method {
            InitMethod::Flat => (0..config.population_size)
                .map(|_| Agent::new(species.zeroed_genome()))
                .collect(),
            InitMethod::Random { seed } => (0..config.population_size)
                .map(|_| Agent::new(species.random_genome(seed)))
                .collect(),
            InitMethod::Seeded { mutate, .. } => {
                let Some(mut agents) = seeds else {
                    bail!("InitMethod was Seeded but `seeds` argument was None");
                };
                if agents.len() < config.population_size {
                    // Spawn remaining agents to reach intended population size
                    let num_seeds = agents.len();
                    let mut rng = rand::rng();
                    while agents.len() < config.population_size {
                        let parent = agents[..num_seeds]
                            .choose(&mut rng)
                            .expect("Agents vec is not empty");
                        let child = parent.clone_and_mutate(
                            config.mutation_probability,
                            config.mutation_magnitude,
                        );
                        agents.push(child);
                    }
                } else if agents.len() > config.population_size {
                    agents.truncate(config.population_size);
                }

                if mutate {
                    for agent in &mut agents {
                        agent.mutate(config.mutation_probability, config.mutation_magnitude);
                    }
                }

                agents
            }
        };

        Ok(Self {
            agents,
            species,
            config,
        })
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

    fn evolve(&mut self) -> Result<()> {
        self.sort_agents();

        let psize = self.agents.len() as f64;
        let elite_end_ix = (psize * self.config.elitism_fraction).floor() as usize;
        let random_start_ix = (psize - (psize * self.config.random_fraction).floor()) as usize;

        let selector = Selector::new(self.config.selection_method, &self.agents)?;
        let mut rng = rand::rng();

        let mut children = Vec::with_capacity(random_start_ix - elite_end_ix);
        if self.config.crossover_probability > 0.0 {
            let crossover_bern = Bernoulli::new(self.config.crossover_probability).unwrap();
            let crossover_method = self
                .config
                .crossover_method
                .expect("Crossover method was asserted to be Some in blueprint validation");
            for _ in elite_end_ix..random_start_ix {
                // TODO address case of same parent selected twice?
                if crossover_bern.sample(&mut rng) {
                    let p1 = selector.select(&mut rng);
                    let p2 = selector.select(&mut rng);
                    let mut child = Agent::crossover(&p1, &p2, crossover_method);
                    child.mutate(
                        self.config.mutation_probability,
                        self.config.mutation_magnitude,
                    );
                    children.push(child);
                } else {
                    let p = selector.select(&mut rng);
                    let child = p.clone_and_mutate(
                        self.config.mutation_probability,
                        self.config.mutation_magnitude,
                    );
                    children.push(child);
                }
            }
        } else {
            for _ in elite_end_ix..random_start_ix {
                let p = selector.select(&mut rng);
                let child = p.clone_and_mutate(
                    self.config.mutation_probability,
                    self.config.mutation_magnitude,
                );
                children.push(child);
            }
        }

        for (i, child) in children.into_iter().enumerate() {
            self.agents[elite_end_ix + i] = child;
        }

        for i in random_start_ix..self.agents.len() {
            self.agents[i] = Agent::new(self.species.random_genome(None));
        }

        Ok(())
    }

    fn pack_agent(&self, ix: usize) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&self.agents[ix].genome)?)
    }

    fn pack_save(&self) -> Result<Vec<u8>> {
        let num_save = (self.agents.len() as f64 * self.config.save_fraction)
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
