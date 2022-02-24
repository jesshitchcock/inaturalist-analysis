CREATE_TAXA_TABLE = """CREATE TABLE taxa AS 
-- Redshift does not support unnesting array data 
-- to get around this I have generated rows 1 - 30 to cross join each ancestry_id to its taxon_id record.
with rows AS (
              SELECT 1 AS n UNION ALL
              SELECT 2 UNION ALL
              SELECT 3 UNION ALL
              SELECT 4 UNION ALL
              SELECT 5 UNION ALL
              SELECT 6 UNION ALL
              SELECT 7 UNION ALL
              SELECT 8 UNION ALL
              SELECT 9 UNION ALL
              SELECT 10 UNION ALL
              SELECT 11 UNION ALL
              SELECT 12 UNION ALL
              SELECT 13 UNION ALL
              SELECT 14 UNION ALL
              SELECT 15 UNION ALL
              SELECT 16 UNION ALL
              SELECT 17 UNION ALL
              SELECT 18 UNION ALL
              SELECT 19 UNION ALL
              SELECT 20 UNION ALL 
              SELECT 21 UNION ALL
              SELECT 22 UNION ALL
              SELECT 23 UNION ALL
              SELECT 24 UNION ALL
              SELECT 26 UNION ALL
              SELECT 27 UNION ALL
              SELECT 28 UNION ALL
              SELECT 29 UNION ALL
              SELECT 30)
, ancestry_mapping AS (
                        SELECT
                            taxa.*, 
  							-- ancestry taxon_id position in ancestry string
  							rows.n as ancestry_position, 
                            TRIM(SPLIT_PART(
                              				-- if no ancestry data set ancestry field to NULL
                              				CASE 
                              					WHEN taxa.ancestry ='' 
                              					THEN NULL 
                              					ELSE ancestry 
                              				END , '/', rows.n)) AS ancestry_id
                        FROM 
                            rows
                        INNER JOIN
                              -- join the staging_taxa data to the rows table 
                              -- where the number of rows is equal to or less than the number of taxon_ids in the ancestry column
                              -- for example "48460/1/2/355675/20978/27880/27882/27907" has 8 taxon_ids listed in the ancestry column so we would join to rows 1 - 8
                                staging.taxa as taxa ON 
                                rows.n <= REGEXP_COUNT(taxa.ancestry, '/') + 1
                        WHERE 
                            -- filter for amphibia class (taxon_id =20978)
                            ancestry LIKE '48460/1/2/355675/20978%' OR
                            -- include taxon_ids listed prior to amphibia class
                            taxon_id IN (48460, 1, 2, 355675))
, redlist_status AS ( 
  -- Identify the IUCN red list category for each species
  -- grouping by binomial (species) and category to return one record.   
						SELECT 
  							binomial, 
  							category 
 						FROM 
  							staging.species_geospatial
  						GROUP BY 
  							binomial, 
  							category
)
SELECT 
     m.taxon_id, 
     m.rank as taxon_rank,
     m.rank_level as taxon_rank_level, 
     m.name as taxon_name, 
     m.active as taxon_active,   
     s.category as red_list_category, 
     m.ancestry as taxon_ancestry, 
     m.ancestry_id, 
     m.ancestry_position, 
     t.rank as ancestry_rank, 
     t.rank_level as ancestry_rank_level, 
     t.name as ancestry_name, 
     t.active as ancestry_active                                     
FROM 
     ancestry_mapping AS m 
LEFT JOIN staging.taxa t ON 
       -- join ancestry mapping to staging_taxa to get taxa info for each ancestry_id
       m.ancestry_id = t.taxon_id AND
       -- exclude taxon_ids that equal the ancestry_id
       m.ancestry_id <> m.taxon_id
LEFT JOIN redlist_status s ON 
       s.binomial = m.name AND 
       m.rank = 'species'
"""

CREATE_SPECIES_OBSERVATIONS_TABLE ="""
-- Are observations within the bounds of the known species distribution (plot observations co-ordinates with species distribution).
CREATE TABLE species_observations AS 
-- Are observations within the bounds of the known species distribution (plot observations co-ordinates with species distribution).
with selected_observations AS (
                              SELECT 
                                   o.*, t.taxon_name, ST_SetSRID(ST_Point(longitude, latitude), 4326) as geom_point
                              FROM
                                  staging.observations o
                                  INNER JOIN staging.taxa t ON 
                                      t.taxon_id = o.taxon_id AND 
                                      t.ancestry_name ='Amphibia' AND
                                      t.taxon_rank ='species' AND
                                      t.taxon_active IS True
)
geom_comparison AS ( 
                    SELECT 
                        ST_Within(o.geom_point, sd.geometry) as observed_in_known_area, 
                        o.*
                    FROM 
                        selected_observations o
                        INNER JOIN 	staging.species_geospatial sd  ON 
                            o.taxon_name = sd.binomial
                       )
   -- there are multiple geometry records for the same species. 
   -- take the max of the observed_in_known_area to see if the observation is in one of these areas 
SELECT
	observation_uuid, 
    observer_id, 
    latitude, 
    longitude, 
    positional_accuracy as positional_accuracy_in_metres, 
    taxon_id, 
    quality_grade, 
    observed_on, 
    -- recalculate the geom_point because you cannot group by geometry
  	ST_SetSRID(ST_Point(longitude, latitude), 4326) as geom_point, 
	CAST(MAX(CAST(observed_in_known_area AS INTEGER)) AS BOOLEAN) as observed_in_known_area
FROM 
	geom_comparison
GROUP BY
	observation_uuid, 
    observer_id, 
    latitude, 
    longitude, 
    positional_accuracy, 
    taxon_id, 
    quality_grade, 
    observed_on 
"""