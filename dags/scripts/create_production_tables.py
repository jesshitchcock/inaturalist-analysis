create_taxa_prod_table_sql = """
CREATE TABLE IF NOT EXISTS production.taxa ( 
taxon_id INTEGER NOT NULL DISTKEY, 
taxon_rank VARCHAR(100),
taxon_rank_level INTEGER, 
taxon_name VARCHAR(256), 
taxon_active BOOLEAN, 
species_red_list_category VARCHAR(2), 
taxon_ancestry VARCHAR(MAX), 
ancestry_id INTEGER, 
ancestry_position INTEGER, 
ancestry_rank VARCHAR(100),
ancestry_rank_level INTEGER, 
ancestry_name VARCHAR(256), 
ancestry_active BOOLEAN, 
PRIMARY KEY(taxon_id))
SORTKEY (taxon_rank, ancestry_id) 
"""

