use anyhow::{anyhow, Result};

/// Macro for deserializing a blueprint into a population with a given
/// species and genome type, and returning a boxed trait object
///
/// Only use this in the match statement in [crate::wake_population]
#[macro_export]
macro_rules! synthesize {
    ($species_type:ty, $genome_type:ty, $blueprint:expr) => {{
        let species: $species_type = serde_json::from_value($blueprint.species.clone())?;
        let seeds = match &$blueprint.seeds {
            Some(seeds) => {
                let genomes: Vec<$genome_type> = serde_json::from_value(seeds.clone())?;
                let agents = genomes.into_iter().map(Agent::new).collect::<Vec<_>>();
                Some(agents)
            }
            None => None,
        };

        let population = Population::new($blueprint.ga_config, species, seeds);
        Box::new(population)
    }};
}

pub fn _de_router_id(frame: &[u8]) -> Result<u32> {
    let bytes: [u8; 4] = frame[1..]
        .try_into()
        .map_err(|_| anyhow!("Byte array should be 5 bytes long"))?;
    Ok(u32::from_le_bytes(bytes))
}
