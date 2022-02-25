taxa_dist_sql = """with taxon_records AS ( 
  select 
  	count(*) as cnt, 
  	taxon_id 
 from 
  	production.taxa 
 group by 
  	taxon_id ) 
select 
	min(cnt) as min_cnt, 
    max(cnt) as max_cnt, 
    avg(cnt) as avg_cnt, 
    median(cnt) as median_cnt 
from 
	taxon_records"""

