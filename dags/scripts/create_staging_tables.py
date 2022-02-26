create_observers_raw_table_sql = """
CREATE TABLE IF NOT EXISTS staging.observers_raw (
observer_id INTEGER, 
login VARCHAR(256), 
name VARCHAR(256), 
PRIMARY KEY(observer_id)
) DISTSTYLE AUTO;
"""

create_observations_raw_table_sql = """
CREATE TABLE IF NOT EXISTS staging.observations_raw (
observation_uuid VARCHAR(50), 
observer_id INTEGER, 
latitude DOUBLE PRECISION,
longitude DOUBLE PRECISION,
positional_accuracy INTEGER, 
taxon_id INTEGER, 
quality_grade VARCHAR(50), 
observed_on DATE, 
PRIMARY KEY(observation_uuid)
) DISTSTYLE AUTO;
"""

create_taxa_raw_table_sql = """
CREATE TABLE IF NOT EXISTS staging.taxa_raw (
taxon_id INTEGER, 
ancestry VARCHAR(256), 
rank_level DOUBLE PRECISION,
rank VARCHAR(20) SORTKEY,
"name" VARCHAR(256), 
active BOOLEAN, 
PRIMARY KEY(taxon_id)
) DISTSTYLE AUTO;
"""

create_geospatial_raw_table_sql = """
CREATE TABLE IF NOT EXISTS staging.species_geospatial_raw ( 
geometry GEOMETRY,
id_no INTEGER NOT NULL, 
binomial VARCHAR(MAX), 
presence INTEGER,	
origin INTEGER, 
seasonal INTEGER, 
compiler VARCHAR(256), 
yrcompiled INTEGER,
citation VARCHAR(MAX), 
subspecies VARCHAR(256), 
subpop VARCHAR(256), 
"source" VARCHAR(256), 
island VARCHAR(256), 
tax_comm VARCHAR(256), 
dist_comm VARCHAR(256), 
generalised BIGINT, 
legend VARCHAR(256), 
kingdom VARCHAR(256), 
phylum VARCHAR(256), 
class VARCHAR(256), 
order_ VARCHAR(256), 
family VARCHAR(256), 
genus  VARCHAR(256), 
category VARCHAR(256), 
marine BOOLEAN, 
terrestrial BOOLEAN, 
freshwater BOOLEAN, 
shape_leng DOUBLE PRECISION,
shape_area DOUBLE PRECISION, 
PRIMARY KEY(id_no)
) DISTSTYLE AUTO;
"""

