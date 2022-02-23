CREATE_AMPHIBIAN_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_amphibian_reference ( 
taxon_id INTEGER, 
scientific_name VARCHAR(256), 
genus VARCHAR(256),	
family VARCHAR(256), 
taxon_order VARCHAR(256), 
taxon_source VARCHAR(MAX), 
notes VARCHAR(256)) 
DISTSTYLE ALL;
"""

CREATE_OBSERVERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_observers (
observer_id INTEGER NOT NULL, 
login VARCHAR(256) NOT NULL, 
name VARCHAR(256),
PRIMARY KEY(observer_id))
DISTSTYLE ALL;
"""

CREATE_OBSERVATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_observations (
observation_uuid VARCHAR(50) NOT NULL, 
observer_id INTEGER NOT NULL, 
latitude DOUBLE PRECISION,
longitude DOUBLE PRECISION,
positional_accuracy INTEGER, 
taxon_id INTEGER, 
quality_grade VARCHAR(50), 
observed_on DATE, 
PRIMARY KEY(observation_uuid))
DISTSTYLE ALL;
"""

CREATE_TAXA_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_taxa (
taxon_id INTEGER NOT NULL, 
ancestry VARCHAR(256), 
rank_level DOUBLE PRECISION,
rank VARCHAR(20),
name VARCHAR(256), 
active BOOLEAN,  
PRIMARY KEY(taxon_id))
DISTSTYLE ALL;
"""

CREATE_PHOTOS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_photos (
photo_uuid VARCHAR(MAX) NOT NULL,
photo_id INTEGER, 
observation_uuid VARCHAR(MAX) NOT NULL, 
observer_id INTEGER , 
extension VARCHAR(MAX), 
license VARCHAR(MAX), 
width SMALLINT, 
height SMALLINT, 
"position" SMALLINT)
DISTSTYLE ALL;
"""

CREATE_SPECIES_DIST_SQL = """
CREATE TABLE IF NOT EXISTS staging_species_distribution ( 
geometry GEOMETRY,
id_no INTEGER, 
binomial VARCHAR(MAX), 
presence INTEGER,	
origin INTEGER, 
seasonal INTEGER, 
compiler VARCHAR(256), 
yrcompiler INTEGER,
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
marine VARCHAR(256), 
terrestial VARCHAR(256), 
freshwater VARCHAR(256), 
shape_leng DOUBLE PRECISION,
shape_area DOUBLE PRECISION
) 
DISTSTYLE ALL;
"""