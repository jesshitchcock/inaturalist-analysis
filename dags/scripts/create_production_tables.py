create_taxa_prod_table_sql = """
    CREATE TABLE IF NOT EXISTS production.taxa ( 
    taxon_id INTEGER PRIMARY KEY, 
    taxon_rank VARCHAR(100),
    taxon_rank_level INTEGER, 
    taxon_name VARCHAR(256), 
    taxon_active BOOLEAN, 
    taxon_ancestry VARCHAR(MAX), 
    ancestry_id INTEGER, 
    ancestry_position INTEGER, 
    ancestry_rank VARCHAR(100),
    ancestry_rank_level INTEGER, 
    ancestry_name VARCHAR(256), 
    ancestry_active BOOLEAN
    )
    DISTSTYLE ALL
    SORTKEY(taxon_rank, ancestry_id);
"""
create_geospatial_prod_table_sql = """
CREATE TABLE IF NOT EXISTS production.species_geospatial (
  iucn_id INTEGER PRIMARY KEY,
  taxon_id INTEGER NOT NULL,
  compiled_by VARCHAR(MAX),
  citation VARCHAR(MAX),
  year_compiled INTEGER,
  iucn_category_id VARCHAR(2) SORTKEY,
  iucn_category_name VARCHAR(50),
  marine BOOLEAN,
  terrestrial BOOLEAN,
  freshwater BOOLEAN,
  shape_length DOUBLE PRECISION, 
  shape_area DOUBLE PRECISION, 
  geometry GEOMETRY)
  DISTSTYLE ALL
  ;
"""
create_observers_prod_table_sql = """
CREATE TABLE IF NOT EXISTS production.observers (
  observer_id INTEGER PRIMARY KEY,
  login_name VARCHAR(MAX),
  observer_name VARCHAR(MAX),
  first_observation_date DATE SORTKEY
  ) 
  DISTSTYLE AUTO;
"""
create_observations_prod_table_sql = """
CREATE TABLE IF NOT EXISTS production.species_observations (
  observation_uuid VARCHAR(50) PRIMARY KEY,
  observer_id INTEGER, 
  latitude DOUBLE PRECISION, 
  longitude DOUBLE PRECISION, 
  positional_accuracy_in_metres DOUBLE PRECISION,
  taxon_id INTEGER SORTKEY,
  quality_grade VARCHAR(20),
  observation_date DATE DISTKEY,
  observation_geom_point GEOMETRY,
  observed_in_known_area BOOLEAN
); """