#sql create schema and tables 

DROP SCHEMA IF EXISTS dpa_metadata CASCADE;

CREATE SCHEMA dpa_metadata;

CREATE TABLE dpa_metadata.extract (
	extraction_time varchar DEFAULT NULL,
	raw_cols_deleted varchar DEFAULT NULL,
	raw_cols_left varchar DEFAULT NULL
);

CREATE TABLE dpa_metadata.saveS3 (
	save_time varchar DEFAULT NULL,
	s3_bucket_name varchar DEFAULT NULL,
	s3_key_name varchar DEFAULT NULL,
	df_shape varchar DEFAULT NULL
);

CREATE TABLE dpa_metadata.transformation (
	execution_date varchar DEFAULT NULL,
	number_of_transformations varchar DEFAULT NULL,
	new_columns varchar DEFAULT NULL
);

CREATE TABLE dpa_metadata.feature_engineering (
	execution_time varchar DEFAULT NULL,
	shape_prior_fe varchar DEFAULT NULL,
	num_features varchar DEFAULT NULL,
	name_features varchar DEFAULT NULL,
	num_cat_features varchar DEFAULT NULL,
	name_cat_features varchar DEFAULT NULL,
	num_num_features varchar DEFAULT NULL,
	name_num_features varchar DEFAULT NULL,
	shape_after_fe varchar DEFAULT NULL
);


 

 
