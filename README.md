# Weather Factors in Crime

The effects  of weather factors on the incidence of different types of crime have been studied for decades, and a substantial body of research suggests 
that such effects do exist.<sup><a href="#Rotton_Cohn_Textbook">1</a></sup> For example, it is recognized that, within a wide temperature range, increases in temperature correlate with higher violent crime rates.<sup><a href="#Ruderman_Cohn_2021">2</a>, <a href="#Mahendran_et_all_2021">3</a></sup> 

Numerous areas in this field are still being intensively researched. This applies to interactions between different factors (e.g.  temperature and humidity, or meteorological and geographical dimensions<sup><a href="#Brundson_et_all">4</a></sup>), as well as exploring relations between weather and crime patterns on a more detailed scale<sup><a href="#Rotton_Cohn_Textbook">1</a></sup> – to name a few. 

Data pipelines used to study this kind of problem are required to effectively process large raw datasets with high temporal and spatial granularity. The simple learning project discussed below is meant to be a first step in building such a data pipeline.

## Contents
[Technologies](#technologies) <br />
[Data](#data) <br />
  [Crime data](#crime_data) <br />
      [Incident locations](#incident_locations) <br />
      [Offense names](#combined_offense_names_problem) <br />
  [Weather data](#weather_data) <br />
[Data model](#data_model) <br />
  [dim_dates and dim_hours](#dim_date_and_time) <br />
  [dim_weather_stations and dim_agencies](#dim_ws_and_agencies) <br />
  [fct_crime_incidents: denormalized offense names](#denormalized_offense_names) <br />
  [fct_meteodata: reorganized and unified meteo datasets](#reorganized_and_unified_meteo_datasets) <br />
  [Distribution styles/keys and sort keys](#dist_and_sort_keys) <br />
[ETL](#etl) <br />
  [Extract](#extract) <br />
  [Transform and Load](#transform_and_load) <br />
      [Agencies and weather stations](#agencies_and_weather_stations) <br />
      [Meteorological parameters](#meteoparam_elt) <br />
      [Crime incidents](#crime_incidents_elt) <br />
      [Temporal dimension tables](#temporal_dimension_tables) <br />
      [Fact tables](#fact_tabless) <br />
[Creating and destroying the infrastructure via Terraform](#creating_infrastructure) </br>
[Further development](#further_development) </br>
[References](#references) <br />

<a name="technologies"></a><h2>Technologies</h2></a>
- AWS Redshift (version:  PostgreSQL 8.0.2 on i686-pc-linux-gnu)
- AWS S3
- Apache Airflow (version: 2.5.0)
- Docker (Docker Engine version: 20.10.12) and Docker Compose (version: 2.2.3)
- Terraform (version: 1.3.7 on windows_386)

<a name="data"></a><h2>Data</h2></a>

Currently, there are three data sources used in the project: 
1. <a name="crime_data_external"></a><a href="https://www.kaggle.com/datasets/louissebye/united-states-hate-crimes-19912017">United States Hate Crimes (1991-2018)</a>
2. <a name="ori_external"></a><a href="https://docs.google.com/spreadsheets/d/1UC-KdQqXKUD5xAQZdmQnBcJ_u02wCe21Cco7nOhfsqA/">U.S. law enforcement agencies and ORI (Originating Agency Identifier) numbers</a>
3. <a name="weather_data_external"></a><a href="https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data">Historical Hourly Weather Data 2012-2017</a>

(more to be added in further development).
  
<a name="crime_data"></a><h3>Crime data</h3>

The first source provides a 50.1 MB <a href="/src/airflow/data/crime_incidents.csv">CSV file</a> containing information regarding crime incidents (see the <a href="/docs/Columns%20of%20crime_incidents.csv.md">list of its columns</a>).
  
As for the information on incident locations and offenses, some extra work besides usual cleaning and formatting is needed to prepare the data for analysis.
  
<a name="incident_locations"></a><h4>Incident locations</h4>

The location of an incident is essential for determining the related weather conditions. Unfortunately, the best option to locate incidents via this dataset is to use the locations of the law enforcement agencies  the dataset associates with the incidents (<a href="/docs/Columns%20of%20crime_incidents.csv.md">item #3 of the list</a>). The agencies' latitudes and longitudes can be obtained from the <a href="/src/airflow/data/agencies.csv">`agencies.csv`</a> file acquired from <a href="#ori_external">the second data source above</a>.
  
<a name="combined_offense_names_problem"></a><h4>Offense names</h4>
  
Basically, the strings in the offense_name column (<a href="/docs/Columns%20of%20crime_incidents.csv.md">item #22</a>) utilize the set of standard offense names that <a href="https://ucr.fbi.gov/nibrs/2011/resources/nibrs-offense-codes">the National Incident-Based Reporting System</a> uses (they are listed in <a href="/docs/Offense%20names.md">Offense names.md</a>).
  
However, one string can include more than just one of these standard names: when several names apply to a single incident, they are combined by concatenation (e.g. `"Aggravated Assault;Destruction/Damage/Vandalism of Property;Intimidation;Robbery;Simple Assault"` that is five semicolon-separated standard names). It could impede data filtering and thus deteriorate the performance of analytics queries. To avoid this, we can separate such combined names and restructure the data.
  
<a name="weather_data"></a><h3>Weather data</h3>
  
The 71,2 MB dataset retrieved from <a href="#weather_data_external">the third source</a> is organized into seven CSV files: <a href="/src/airflow/data/meteodata/weather_stations.csv">one with weather station attributes</a>, including latitudes and longitudes, and <a href="https://github.com/AndreiMaikov/Weather_factors_in_crime--Terraform-Airflow-Redshift/tree/main/src/airflow/data/meteodata">six with meteoparameter values</a>. Each of the latter contains the values of one parameter (temperature, humidity, etc.) divided into station-specific columns, as well as a column providing the measurements' dates and hours. 
  
<a name="data_model"></a><h2>Data model</h2>

After a series of transformations (which are discussed <a href="#transform_and_load">below</a>), the data is finally loaded into the tables of a "frontend" schema called <a name="wxcr_def"></a>**`wxcr`**. The transformations are made in another, "backend" schema **`wxcr_be`**, which is also used for initial data staging and to keep data auditing and cleaning logs. 
  
The following diagram shows how the data in `wxcr` is organized into two fact tables and four dimension tables:

<a name="wxcr_erd"></a>
  
![ ](/img/wxcr_ERD.svg)
  
(due to the large number of columns `fct_crime_incidents` comprises, most of them are omitted from the diagram but listed in <a href="/docs/Columns%20of%20fct_crime_incidents.md"> Columns of fct_crime_incidents.md</a>). 
  
<a name="dim_date_and_time"></a>**dim_dates and dim_hours**
  
These temporal dimension tables are included in the schema to facilitate executing analytic queries. The table `dim_dates` assigns an id to each date within a range (not necessarily the minimal one) covering all the dates in both crime incidents and meteodata datasets. The table is generated in the data transformation process (see <a href="#temporal_dimension_tables">below</a>). Such dimension tables are widely used in Ralph Kimball's dimension modeling methodology.<sup><a href="#Kimball_Ross">5</a></sup> Likewise, `dim_hours` assigns an id to each hour from 00:00 to 23:00.
  
<a name="dim_ws_and_agencies"></a>**dim_weather_stations and dim_agencies**
  
The tables essentially replicate the structure of the data in <a href="/src/airflow/data/meteodata">`weather_stations.csv`</a> and <a href="/src/airflow/data">`agencies_csv`</a>.
   
<a name="denormalized_offense_names"></a>**fct_crime_incidents: denormalized offense names**
  
The most important difference between <a href="#crime_data">the original dataset</a> and `fct_crime_incidents` is that the latter includes additional columns each of which corresponds to one of the standard/uncombined offense names. The value of such an additional attribute is simply a boolean flag indicating whether this offense name is part of the original `offense_name` string in `crime_incidents.csv`. This resolves the query performance problem mentioned <a href="#combined_offense_names_problem">above</a>. 
  
<a name="reorganized_and_unified_meteo_datasets"></a>**fct_meteodata: reorganized and unified meteo datasets**
  
The values of <a href="#weather_data">the six meteorological parameters</a> are grouped together in `fct_meteodata` that has one column for each parameter. Each row of the table contains the data related to the same station (specified in the `stn` column) and the date and time the values were measured.

<a name="dist_and_sort_keys"></a>**Distribution styles/keys and sort keys**

The styles and keys shown on the diagram were chosen considering the tables' size, the most likely types of analytics queries, and what data is expected to be added in the future.

<a name="etl"></a><h2>ETL</h2>

In this project, a Redshift cluster and an AWS S3 bucket are used for ETL operations (<a href="#creating_infrastructure">the next section</a> discusses how the infrastructure is created using Terraform).

The ETL process, which finally loads the data into the <a href="#wxcr_def">`wxcr`</a> schema, is run by a local Docker-based instance of Apache Airflow. To set up the instance,  <a href="/src/airflow/docker-compose.yaml">a modified version</a> of <a href="https://airflow.apache.org/docs/apache-airflow/2.5.0/docker-compose.yaml">the "official" `docker-compose.yaml`</a> file and the shell script <a href="/src/scripts/airflow_up.sh>airflow_up.sh">`airflow_up.sh`</a> can be used.

The values of the environment variables required to execute `airflow_up.sh` are stored in two files: `.env` and `redshift_connection.env`. The first one is created manually by the user; the second one, automatically by the script `infrastructure_deploy.sh`. I didn’t include into this repository `.env` and `redshift-connection.env` with the actual variable values I used;  instead, see `.env.txt` and `redshift_connection.env.txt` for the variables’ names and other details. 
  
The entire ETL workflow is defined in <a href="/src/airflow/dags/weather_factors_in_crime.py">`weather_factors_in_crime.py`</a> and outlined in the following graph:

<a name="entire_workflow"></a>

![ ](/img/entire_graph.png)

The group **`create_sprocs`** in **`create_schemas_sprocs_upload_data`** consists of tasks that create <a href="/src/airflow/dags/sql/sprocs">the stored procedures</a> required downstream of `create_schemas_sprocs_upload_data`. The next graphs expand the rest of the DAG's task groups:

<a name="upload_data_to_s3__diagram"></a>

![ ](/img/upload_data_to_s3.png)

<a name="load_unpivot_meteoparam_tables"></a>

![ ](/img/load_unpivot_meteoparam_tables.png)

<a name="cdtt_and_pcit"></a>

![ ](/img/cdtt_and_pcit.png)

<a name="create_fct_tables"></a>

![ ](/img/create_fct_tables.png)

For detail, see <a href="/src/airflow/dags/weather_factors_in_crime.py">`weather_factors_in_crime.py`</a> and the subsections below.
  
<a name="extract"></a><h3>Extract</h3>

<a href="#data">The original data files</a> are uploaded from the <a href="/src/airflow/data">`data`</a> folder of the local filesystem to the project's S3 bucket. The upload tasks (the <a href="#upload_data_to_s3__diagram">upload_data_to_s3</a> task group) utilize the Airflow `LocalFilesystemToS3Operator` operator. They are created via <a href="https://airflow.apache.org/docs/apache-airflow/2.3.0/concepts/dynamic-task-mapping.html">dynamic task mapping</a> using the <a href="/src/airflow/data/auxfs/data_files.json">`data_files.json`</a> file that specifies the local and S3 paths to the data files.

<a name="transform_and_load"></a><h3>Transform and Load</h3>

<a name="agencies_and_weather_stations"></a><h4>Agencies and weather stations</h4>

The tasks `load_clean_agensies` and `load_clean_weather_station` copy agency and weather station attribute values from the corresponding files and format the copied data. 

Besides that, the tasks  involve handling original data errors. It is possible, for example, that an agency is presented in the agencies.csv file by more than one row, which are either identical or have the same `ori` value but differ in some other attribute's value. In the first case, only one of the identical rows is kept in the output of `load_clean_agensies`; in the second case, all such rows are discarded as giving unreliable information. Similar data cleansing is performed by `load_clean_weather_stations`.

The results are loaded into the target tables <a href="#wxcr_erd">`wxcr.dim_agencies`</a> and <a href="#wxcr_erd">`wxcr.dim_weather_stations`</a> and do not undergo any transformations after that. Logs documenting the cleansing done are added to the `wxcr_be.log_table`.

<a name="meteoparam_elt"></a><h4>Meteorological parameters</h4>

To create tables for staging the meteoparameter data from the S3 bucket to Redshift, two user-created CSV files are used: <a href="/src/airflow/data/auxfs/meteoparam_names_fmts.csv">`meteoparam_names_fmts.csv`</a> and <a href="/src/airflow/data/auxfs/station_names.csv">`station_names.csv`</a>. The first one specifies the format of each meteoparameter; the second one, the names of the weather stations (the user can define these names different from the column names in the original CSV files). 

The task <a href="#entire_workflow">`load_meteoparam_station_specs`</a> reads the information from `meteoparam_names_fmts.csv` and `station_names.csv`, and then <a href="#entire_workflow">`create_meteoparam_tbl_defs`</a>  generates strings required for table definitions (e.g. `datetime DATETIME, Philadelphia DECIMAL(5, 2), New_York DECIMAL(5, 2), ...` for the `wxcr_be.temperature` staging table). 

The task group <a href="#load_unpivot_meteoparam_tables">`load_unpivot_meteoparam_tables`</a>, which is <a href="#entire_workflow">downstream of `create_meteoparam_tbl_defs`</a>, comprises three types of tasks:

- `create_meteoparam_tbl` (creating staging tables for temperature, humidity, etc. based on the previously generated  table definition strings);
- `copy_meteoparam_tbl` (copying the data into these tables from the corresponding CSV files);
- `unpivot_meteoparam_tbl` (transforming a meteoparameter staging table's structure so that the data from all station-specific columns is placed into a single column and the station names corresponding to the data origin are indicated in another column).

A task of each type is created for each of the six meteorological parameters via <a href="https://airflow.apache.org/docs/apache-airflow/2.3.0/concepts/dynamic-task-mapping.html">dynamic task mapping</a>  &mdash; like in the case of the <a href="#extract">`upload_data_to_s3`</a> task group, but this time the information needed for the mapping is fetched from the table `wxcr_be.meteoparam_tbl_defs` created upstream by <a href="#entire_workflow">`create_meteoparam_tbl_defs`</a>.

When the six `unpivot_meteoparam_tbl` tasks have been completed, their results are joined by <a href="#entire_workflow">`join_meteoparam_unpv_tables`</a> on the `stn` (the station's name) and `datetime` (the date and time of the measurement) columns so that the temperature, humidity, etc. datasets are collected in one table. 

At this point, the only adjustment the meteo data still needs is <a href="#dim_date_and_time">assigning date and time ids to each record</a>. It will be done <a href="#fact_tables">in a later stage</a>.

<a name="crime_incidents_elt"></a><h4>Crime incidents</h4> 

The task <a href="#entire_workflow">`load_clean_crime_incidents`</a> <a name="load_clean_crime_incidents"></a> copies data from the project's S3 bucket and performs documented cleansing and formatting  &mdash; very similar to what `load_clean_agencies` and `load_clean_weather_station` do, but in contrast to these two tasks, the result of `load_clean_crime_incidents` is not ready for the "frontend" `wxcr` schema. The main reason is that the offense names have not yet been <a href="#combined_offense_names_problem">separated</a>. Besides, just like in the case of the meteoparameter data, <a href="#dim_date_and_time">assigning date and time ids</a> is required.

The task <a href="#entire_workflow">`split_ci_offenses`</a> separates the standard offense names and organizes them into individual rows of a new table `wxcr_be.ci_offense_names_normalized`.  Thus each incident in this new table is represented by as many rows as its original `offense_name` string has in `crime_incidents.csv` (for detail, see <a href="/src/airflow/dags/sql/sprocs/split_offenses_ci__sproc.sql">`split_offenses_ci__sproc.sql`</a>).

Using <a name="normalized_offense_names"></a>`wxcr_be.ci_offense_names_normalized`, the task group <a href="#cdtt_and_pcit">`pivot_ci_table`</a> 
- collects all the individual standard offense names mentioned in any of the `offense_name` strings of `crime_incidents.csv`; 
- creates a table `wxcr_be.ci_offense_names_denormalized` with
    - one column containing crime incident ids; 
    - each of the other columns corresponding to a "standard" offense name, and the column's values indicating if the offense name is relevant to the given incident.
  
<a name="temporal_dimension_tables"></a><h4>Temporal dimension tables</h4> 

The task <a href="#cdtt_and_pcit">`create_dim_dates`</a> generates a set of consecutive dates for the `dim_date` table. As mentioned <a href="#dim_date_and_time">above</a>, this set must cover at least all the dates included in any of the two cleaned datasets &ndash; crime and meteo. However, the user may want to have an even wider date range covered (for example, if some additional crime or meteo data is expected to come after the current data have been loaded into `wxcr`). In this case, the user specifies the date range that must be covered  (see comments in `weather_factors_in_crime.py`). If the user doesn't specify the range, the task automatically generates the minimal set of dates.
  
To generate the set, a recursive CTE (common table expression) is used (see <a href="/src/airflow/dags/sql/sprocs/create_date_range__sproc.sql">`create_date_range__sproc.sql`</a>). 
  
A similar algorithm is used to populate the table <a href="#dim_date_and_time">`dim_hours`</a>.

<a name="fact_tables"></a><h4>Fact tables</h4> 

The task group <a href="#create_fct_tables">`create_fct_tables`</a> completes the ETL process by joining upstream-created tables into <a href="#wxcr_erd">`fct_meteodata` and `fct_crime_incidents`</a>
(see <a href="/src/airflow/dags/sql/create_fct_crime_incidents.sql">`create_fct_crime_incidents.sql`</a> and 
<a href="/src/airflow/dags/sql/create_fct_meteodata.sql">`create_fct_meteodata.sql`</a>) and adding the required distribution and sort keys.

<a name="creating_infrastructure"></a><h2>Creating and destroying the infrastructure via Terraform</h2>

The infrastructure is defined in `the full.tf`and includes
- an AWS VPC;
- an Internet gateway;
- a subnet group including two subnets in the region chosen by the user;
- a route table and route table associations for the subnets;
- a security group;
- an IAM role and an associated role policy;
- a Redshift cluster;
- an S3 bucket;
- a local file `cluster-connection.env`.

The only prerequisite for creating it is that the user has an AWS EC2 instance deployed. 

**Step 1**

The user chooses 
- the AWS region that the cluster will  be deployed in;
- the names of the S3 bucket, the cluster and the database for wxcr and wxcr_be schemas;
- the login, the name of the IAM role, and the password. 

Then the user specifies them, along with the AWS account number, access key ID and secret access key, in the `.env` file. 

Besides that, some cluster-related details must be specified in the `def.tfvars` file.

**Step 2**

The script `infrastructure_deploy.sh` passes to Terraform the variable values from both `.env` and `dev.tfvars` and executes the commands that create the infrastructure.

The infrastructure can be terminated by running the `infrastructure_destroy.sh` script.

<a name="further_development"></a><h2>Further development</h2>
1. To provide for a meaningful analysis, more comprehensive datasets are required.
   - Adding crime incident data from sources different from <a href="#crime_data_external">the one used in this project</a> would facilitate a better understanding of crime patterns. It could be helpful to include data from non-US regions.
   - The weather data must match the crime data both geographically and temporally. Therefore, <a href="#weather_data_external">the current weather dataset</a> needs to be extended significantly.
2. It would be interesting to compare the performance of <a href="#data_model">the employed data model</a> with that of its alternatives (for example, one that uses <a href="#normalized_offense_names">normalized standard offense names</a> or a model without <a href="#dim_date_and_time">date and hour dimensions</a>). Another problem to look into is data model optimization (e.g. choosing the distribution styles/keys  and sort keys).
3. Some security improvements, including those related to handling AWS credentials and the use of IAM role policies, should be made.



<a name="references"></a><h2>References</h2>
1.	<a name="Rotton_Cohn_Textbook"></a>Rotton, J., Cohn, E. G. Climate, weather and crime. In: Bechtel, R. B., & Churchman, A. (Eds.). (2003). *Handbook of environmental psychology.* John Wiley & Sons. [<a href="https://scholar.google.com/scholar?cluster=15911032322184545526&hl=en&as_sdt=0,5">Google Scholar</a>]
2.	<a name="Ruderman_Cohn_2021"></a>Ruderman, D., & Cohn, E. G. (2021). Predictive extrinsic factors in multiple victim shootings. *The Journal of Primary Prevention, 42,* 59-75. [<a href="https://pubmed.ncbi.nlm.nih.gov/32671646/">PubMed</a>] [<a href="https://link.springer.com/article/10.1007/s10935-020-00602-3">Full Text</a>]
3.	<a name="Mahendran_et_all_2021"></a>Mahendran, R., Xu, R., Li, S., & Guo, Y. (2021). Interpersonal violence associated with hot weather. *The Lancet Planetary Health, 5*(9), e571-e572. [<a href="https://pubmed.ncbi.nlm.nih.gov/34508676/">PubMed</a>] [<a href="https://scholar.google.com/scholar?hl=en&as_sdt=0%2C5&q=%22Interpersonal+violence+associated+with+hot+weather%22&btnG=">Google Scholar</a>]
4.	<a name="Brundson_et_all"></a>Brunsdon, C., Corcoran, J., Higgs, G., & Ware, A. (2009). The influence of weather on local geographical patterns of police calls for service. *Environment and Planning B: Planning and Design, 36*(5), 906-926. [<a href="https://scholar.google.com/scholar?hl=en&as_sdt=0%2C5&q=%22The+influence+of+weather+on+local+geographical+patterns+of+police+calls+for+service%22">Google Scholar</a>]
5. <a name="Kimball_Ross"></a>Kimball, R., & Ross, M. (2011). *The data warehouse toolkit: the complete guide to dimensional modeling.* John Wiley & Sons. [<a href="https://scholar.google.com/scholar?hl=en&as_sdt=0%2C5&q=The+Data+Warehouse+Toolkit&btnG=">Google Scholar</a>]
