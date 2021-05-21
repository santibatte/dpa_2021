## Final Task: BiasFairnessMetadata
#### Local Scheduler
luigi --module src.pipeline.luigi.u_bias_fairness_metadata BiasFairnessMetadata --ingest-type consecutive --bucket data-product-architecture-equipo-9 --local-scheduler
#### Remote Scheduler
#luigi --module src.pipeline.luigi.u_bias_fairness_metadata BiasFairnessMetadata --ingest-type consecutive --bucket data-product-architecture-equipo-9

##
