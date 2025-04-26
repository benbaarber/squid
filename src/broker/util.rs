/// Macro for deserializing a blueprint into a population with a given
/// species and genome type, and returning a boxed trait object
///
/// Only use this in the match statement in [crate::wake_population]
#[macro_export]
macro_rules! synthesize {
    ($species_type:ty, $genome_type:ty, $blueprint:expr, $seeds:expr) => {{
        let species = <$species_type>::try_from($blueprint.nn)?;
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

        let population = Population::new($blueprint.ga.clone(), species, agents)?;
        Box::new(population)
    }};
}
