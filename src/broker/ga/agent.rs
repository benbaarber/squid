use crate::util::blueprint::CrossoverMethod;

use super::genome::Genome;

#[derive(Clone)]
pub struct Agent<G> {
    pub genome: G,
    pub fitness: f64,
}

impl<G: Genome> Agent<G> {
    pub fn new(genome: G) -> Self {
        Self {
            genome,
            fitness: 0.0,
        }
    }

    pub fn mutate(&mut self, probability: f64, magnitude: f64) {
        self.genome.mutate(probability, magnitude);
    }

    pub fn crossover(&self, other: &Self, method: CrossoverMethod) -> Self {
        let child_genome = self.genome.crossover(&other.genome, method);
        Self {
            genome: child_genome,
            fitness: 0.0,
        }
    }

    pub fn clone_and_mutate(&self, probability: f64, magnitude: f64) -> Self {
        let mut child = self.clone();
        child.genome.mutate(probability, magnitude);
        child.fitness = 0.0;
        child
    }
}
