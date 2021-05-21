## SHELL SCRIPT TO EXECUTE LUIGI TASKS





## Final Task: BiasFairnessMetadata
#### Local Scheduler
#luigi --module src.pipeline.luigi.u_bias_fairness_metadata BiasFairnessMetadata --ingest-type consecutive --bucket data-product-architecture-equipo-9 --local-scheduler
#### Remote Scheduler
#luigi --module src.pipeline.luigi.u_bias_fairness_metadata BiasFairnessMetadata --ingest-type consecutive --bucket data-product-architecture-equipo-9



## Final Task: PredictMetadata
#### Local Scheduler
#luigi --module src.pipeline.luigi.x_predict_metadata PredictMetadata --ingest-type consecutive --bucket data-product-architecture-equipo-9 --local-scheduler
#### Remote Scheduler
#luigi --module src.pipeline.luigi.x_predict_metadata PredictMetadata --ingest-type consecutive --bucket data-product-architecture-equipo-9



## Final Task:
#### Local Scheduler
luigi --module src.pipeline.luigi.y_store_predictions_api StorePredictionsApi --ingest-type consecutive --bucket data-product-architecture-equipo-9 --local-scheduler
#### Remote Scheduler
#luigi --module src.pipeline.luigi.y_store_predictions_api StorePredictionsApi --ingest-type consecutive --bucket data-product-architecture-equipo-9