use anyhow::{Result, bail};
use rand::{Rng, seq::IndexedRandom};
use rand_distr::{Distribution, Uniform, weighted::WeightedIndex};

use crate::util::blueprint::SelectionMethod;

use super::agent::Agent;

/// Selects parent agents for repopulation based on the specified method
pub struct Selector<'a, G> {
    agents: &'a [Agent<G>],
    method: MethodState,
}

/// Stores method-specific data transforms used for selection
enum MethodState {
    Softmax { dist: WeightedIndex<f64> },
    Tournament { size: u32 },
    Roulette { dist: Option<Uniform<f64>> },
}

impl<'a, G> Selector<'a, G> {
    pub fn new(method: SelectionMethod, agents: &'a [Agent<G>]) -> Result<Self> {
        let method_state = match method {
            SelectionMethod::Softmax => {
                let max_fitness = agents
                    .iter()
                    .map(|a| a.fitness)
                    .reduce(f64::max)
                    .expect("Agents vec is not empty");
                let exps = agents
                    .iter()
                    .map(|a| (a.fitness - max_fitness).exp())
                    .collect::<Vec<_>>();
                let sum: f64 = exps.iter().sum();
                let softmax = exps.into_iter().map(|x| x / sum).collect::<Vec<_>>();
                let dist = WeightedIndex::new(softmax).expect("Softmax guarantees valid weights");
                MethodState::Softmax { dist }
            }
            SelectionMethod::Tournament { size } => MethodState::Tournament { size },
            SelectionMethod::Roulette => {
                let mut total_fitness = 0.0;
                for agent in agents {
                    if agent.fitness < 0.0 {
                        bail!(
                            "Roulette selection method requires all fitness values to be non-negative"
                        );
                    }
                    total_fitness += agent.fitness;
                }
                let dist = if total_fitness > 0.0 {
                    Some(Uniform::new(0.0, total_fitness).unwrap())
                } else {
                    None
                };
                MethodState::Roulette { dist }
            }
        };

        Ok(Self {
            agents,
            method: method_state,
        })
    }

    pub fn select<R: Rng>(&self, rng: &mut R) -> &'a Agent<G> {
        match self.method {
            MethodState::Softmax { ref dist } => {
                let ix = dist.sample(rng);
                &self.agents[ix]
            }
            MethodState::Tournament { size } => self
                .agents
                .choose_multiple(rng, size as usize)
                .max_by(|x, y| x.fitness.partial_cmp(&y.fitness).expect("No NaN fitness"))
                .expect("Agents vec is not empty"),
            MethodState::Roulette { dist } => match dist {
                Some(dist) => {
                    // TODO [optimization] binary search on cumulative distribution (time: O(n) -> O(log n))
                    let pick = dist.sample(rng);
                    let mut cur = 0.0;
                    for agent in self.agents {
                        cur += agent.fitness;
                        if cur >= pick {
                            return agent;
                        }
                    }
                    self.agents.last().expect("Agents vec is not empty")
                }
                None => self.agents.choose(rng).expect("Agents vec is not empty"),
            },
        }
    }
}
