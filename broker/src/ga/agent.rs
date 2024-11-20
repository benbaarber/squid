use super::genome::Genome;

#[derive(Clone)]
pub struct Agent<G> {
    pub genome: G,
    pub fitness: f64,
}

impl<G: Genome> Agent<G> {
    pub fn new(genome: G) -> Self {
        Self { genome, fitness: 0.0 }
    }

    pub fn clone_and_mutate(&self, species: &G::Species, chance: f64, magnitude: f64) -> Self {
        let mut child = self.clone();
        child.genome.mutate(species, chance, magnitude);
        child.fitness = 0.0;
        child
    }
}
