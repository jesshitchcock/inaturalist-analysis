## Introduction  
This project uses Apache Airflow to automate a data pipeline in order to generate a data warehouse of iNaturalist Licenced 
Observation Open Image Data and International Union for Conservation of Nature (IUCN) Red List spatial data.

By combining citizen-science observations from iNaturalist with IUCN Red list spatial data, analysts will be able to 
explore the relationship between the known species distributions (spatial data) and what is being observed by citizen scientists. 

These insights could inform future research efforts. 

## Project Scope
I have limited the project scope to species in the class Amphibia. In 2022, 41% of all Amphibian species are threatened with extinction [red list]. 
While the project scope will be limited to Amphibians, the data model has been designed to be applicable to 
observation and spatial data for all living organisms. 


Some use cases for this data model could be: 
1. How many users have contributed to amphibian observations during a set period of time?
2. Are observations within the bounds of the known species distribution (plot observations co-ordinates with species distribution).
3. Should the species distribution be reassessed?
4. What is the observation cadence over the months of the year and has this changed year-on-year?  the year cou
5. Are users observing data deficient or endangered species?
6. Where are most of our observations coming from?
7. Are there iNaturalist users who are could be included in future research efforts?  

## Datasets 
### Introduction to the Data
#### iNaturalist Licensed Observation Images Open Dataset 
iNaturalist is a popular nature application where users can record and share plant and animal observation images. 
The app has a built-in peer review system that allows users to suggest or confirm identifications for their own or other users' observations, respectively. 
In addition to species identification, users can specify the location (latitude and longitude) of the observation.  
iNatualist collects millions of research-grade observations that can be used for research and conservation efforts. 

The iNaturalist Licensed Observation Images Open Dataset is one of the largest public datasets of photos of living organisms. [@1] 
This open dataset is housed and can be sourced from Amazon S3. 

This project uses the following observation metadata is available as compressed (gzip) comma-separated-values (csv) files: 
1. Taxa - a comprehensive list of taxon_ids for living organism observations
2. Observers - data about the user who made each observation
3. Observations - A record of individual observations of living organisms

Note for the purposes of this project we will not use the image data. 

#### International Union for Conservation of Nature (IUCN) Red List Spatial Data 
> The IUCN Red List of Threatened Species™ contains global assessments for more than 142,500 species. More than 80% of these (>115,000 species) have spatial data.

![IUCN Red List Categories](images/iucn_red_list_category.png)
These assessments classify species according to one of the following categories: 
- Not Evaluated
- Data Deficient
- Least Concern
- Near Threatened
- Vulnerable
- Endangered
- Extinct in the Wild
- Extinct

These classifications as well as spatial data are important for environmental impact assessments and the protection of wildlife. 
Spatial data in the Esri shapefile format can be downloaded from [iucnredlist.org](https://www.iucnredlist.org/resources/spatial-data-download).
For this project, I downloaded the Amphibian shapefile which includes polygon shapes for: 

- Tailless Amphibians (species from the order Anura)
- Tailed Amphibians (species from the order Caudata)
- Caecilian Amphibians (species from the order Gymnophiona)

### Data Exploration
The following jupyter notebooks explores the data from:
- iNaturalist Observations
- IUCN Amphibian Shapefile data 

## Data Model 
The data model was designed to facilitate data analytics based on the questions outlined in the Project Scope Section.  

1. Taxa 
<a href="#fn:bad" rel="footnote">2</a>



## References
[^1] iNaturalist Licensed Observation Images was first accessed on 2022-01-01 from https://registry.opendata.aws/inaturalist-open-data.
[^2] IUCN Spatial Data Download on 2022-01-01 from https://www.iucnredlist.org/resources/spatial-data-download