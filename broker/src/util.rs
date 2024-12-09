use anyhow::{anyhow, Result};

/// Macro for deserializing a blueprint into a population with a given
/// species and genome type, and returning a boxed trait object
///
/// Only use this in the match statement in [crate::wake_population]
#[macro_export]
macro_rules! synthesize {
    ($species_type:ty, $genome_type:ty, $blueprint:expr, $seeds:expr) => {{
        let species = <$species_type>::deserialize($blueprint.species.clone())?;
        let agents = if $seeds.len() > 0 {
            let mut agents = Vec::with_capacity($seeds.len());
            for seed in $seeds {
                let genome: $genome_type =
                    serde_json::from_slice(&seed).context("Failed to deserialize seed json")?;
                let agent = Agent::new(genome);
                agents.push(agent);
            }
            Some(agents)
        } else {
            None
        };

        let population = Population::new($blueprint.ga, species, agents);
        Box::new(population)
    }};
}

pub fn _de_router_id(frame: &[u8]) -> Result<u32> {
    let bytes: [u8; 4] = frame[1..]
        .try_into()
        .map_err(|_| anyhow!("Byte array should be 5 bytes long"))?;
    Ok(u32::from_le_bytes(bytes))
}
