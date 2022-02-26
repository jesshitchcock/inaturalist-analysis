taxa_transformation_sql = """
TRUNCATE TABLE production.taxa;
INSERT INTO production.taxa 
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
                              				END , '/', rows.n))::int AS ancestry_id
                        FROM 
                            rows
                        INNER JOIN
                              -- join the staging_taxa data to the rows table 
                              -- where the number of rows is equal to or less than the number of taxon_ids in the ancestry column
                              -- for example "48460/1/2/355675/20978/27880/27882/27907" has 8 taxon_ids listed in the ancestry column so we would join to rows 1 - 8
                                staging.taxa_raw as taxa ON 
                                rows.n <= REGEXP_COUNT(taxa.ancestry, '/') + 1
                        WHERE 
                            -- filter for amphibia class (taxon_id =20978)
                            ancestry LIKE '48460/1/2/355675/20978%' OR
                            -- include taxon_ids listed prior to amphibia class
                            taxon_id IN (48460, 1, 2, 355675))
SELECT 
     m.taxon_id, 
     m.rank as taxon_rank,
     CAST(m.rank_level AS INTEGER) as taxon_rank_level, 
     m.name as taxon_name, 
     m.active as taxon_active,   
     m.ancestry as taxon_ancestry, 
     CONVERT(INTEGER, m.ancestry_id) ancestry_id, 
     m.ancestry_position, 
     t.rank as ancestry_rank, 
     CAST(t.rank_level AS INTEGER) as ancestry_rank_level, 
     t.name as ancestry_name, 
     t.active as ancestry_active                                     
FROM 
     ancestry_mapping AS m 
LEFT JOIN staging.taxa_raw t ON 
       -- join ancestry mapping to staging_taxa to get taxa info for each ancestry_id
       CAST(m.ancestry_id AS INTEGER) = t.taxon_id AND
       -- exclude taxon_ids that equal the ancestry_id
       m.ancestry_id <> m.taxon_id
"""

geospatial_transformation_sql = """
TRUNCATE TABLE production.species_geospatial;
INSERT INTO production.species_geospatial
SELECT 
    sg.id_no AS iucn_id,
    t.taxon_id,  
    sg.compiler as compiled_by, 
    sg.citation as citation, 
    sg.yrcompiled as year_compiled, 
    sg.category as icun_category_id,
    CASE 
        WHEN sg.category = 'NE' THEN 'not evaluated'
        WHEN sg.category = 'DD' THEN 'data deficient'
        WHEN sg.category = 'LC' THEN 'least concern'
        WHEN sg.category = 'NT' THEN 'near threatened'
        WHEN sg.category = 'VU' THEN 'vulnerable'
        WHEN sg.category = 'EN' THEN 'endangered'
        WHEN sg.category = 'CR' THEN 'critically endangered'
        WHEN sg.category = 'EW' THEN 'extinct in the wild'
        WHEN sg.category = 'EX' THEN 'extinct'
    END as iucn_category_name, 
    sg.marine, 
    sg.terrestrial, 
    sg.freshwater, 
    sg.shape_leng as shape_length, 
    sg.shape_area as shape_area,
    geometry    
FROM 
    staging.species_geospatial_raw AS sg
    INNER JOIN production.taxa AS t ON
        sg.binomial = t.taxon_name AND 
        t.taxon_rank ='species' AND
        --ensure that there is only one record per species coming from the taxa table 
        -- do this by selecting the ancestry_id for Amphibia
        t.ancestry_id =20978
"""

observations_transformation_sql = """
TRUNCATE TABLE production.species_observations;
INSERT INTO production.species_observations
WITH selected_observations AS (
                              SELECT 
                                   o.*, t.taxon_name, ST_SetSRID(ST_Point(longitude, latitude), 4326) as geom_point
                              FROM
                                  staging.observations_raw o
                                  INNER JOIN production.taxa t ON 
                                      t.taxon_id = o.taxon_id AND 
                                      t.ancestry_id = 20978 AND
                                      t.taxon_rank = 'species'
)
, geom_comparison AS ( 
                    SELECT 
                        ST_Within(o.geom_point, sd.geometry) as observed_in_known_area, 
                        o.*
                    FROM 
                        selected_observations o
                        INNER JOIN 	staging.species_geospatial_raw sd  ON 
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
    observed_on as observation_date, 
    -- recalculate the geom_point because you cannot group by geometry
  	ST_SetSRID(ST_Point(longitude, latitude), 4326) as observation_geom_point, 
	CAST(MAX(CAST(observed_in_known_area AS INTEGER)) AS BOOLEAN) as observed_in_known_area
FROM 
	geom_comparison
WHERE observation_uuid IS NOT NULL
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

observers_transformation_sql = """
TRUNCATE TABLE production.observers;
INSERT INTO production.observers
SELECT 
    r.observer_id, 
    r.login, 
    r.name as observer_name, 
    MIN(observation_date) as first_observation_date 
FROM 
    staging.observers_raw r
    INNER JOIN production.species_observations o ON r.observer_id = o.observer_id
GROUP BY 
    r.observer_id, 
    r.login, 
    r.name
"""

