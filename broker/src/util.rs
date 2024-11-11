use anyhow::{anyhow, Result};

/// Macro for deserializing a blueprint into a population with a given
/// species and genome type, and returning a boxed trait object
///
/// Only use this in the match statement in [crate::wake_population]
#[macro_export]
macro_rules! synthesize {
    ($species_type:ty, $genome_type:ty, $blueprint:expr) => {{
        let species: $species_type = serde_json::from_str(&$blueprint.species)?;
        let seeds = match &$blueprint.seeds {
            Some(seeds) => {
                let genomes: Vec<$genome_type> = serde_json::from_str(seeds)?;
                let agents = genomes.into_iter().map(Agent::new).collect::<Vec<_>>();
                Some(agents)
            }
            None => None,
        };

        let population = Population::new($blueprint.ga_config, species, seeds);
        Box::new(population)
    }};
}

pub fn de_usize(frame: &[u8]) -> Result<usize> {
    let bytes: [u8; 8] = frame
        .try_into()
        .map_err(|_| anyhow!("Invalid message length for u64 conversion"))?;
    Ok(u64::from_le_bytes(bytes) as usize)
}
