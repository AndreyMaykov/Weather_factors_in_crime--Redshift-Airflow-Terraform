  
  
**Кроме того, обсудить:**
1. Изъятие части колонок из crime_incidents.csv &mdash; обсуждать в Columns of fct_crime_incidents.md.
1. Добавление dim_date и dim_year
2. Restructuring/rearranging the weather data. 
Не объяснять здесь, почему именно такой дизайн выбран.
  
В качестве **Note** нужно бы добавить какие-то общие фразы о том, что в настоящее время подходы к построению модели данных для data warehouse обсуждаются очень широко (желательно бы дать ссылку/ссылки), и как видно из этих обсуждений, performance существенно зависит от платформы, на которой та или иная модель реализована, от характера аналитических запросов, от средств, которые используются для анализа данных (в частности, BI tools), а также и от того, насколько хорошо оптимизирована конкретная реализация того или иного подхода. [можно сначала об оптимизации, а потом о платформах и BI] При этом выявить наиболее эффективный подход из числа нескольких  часто выясняется только при тестировании их реализаций.
  
Поэтому использованная в настоящем проекте модель может иметь  более эффективные альтернативы. Например,  вместо денормализованных standard offense names для crime incident data можно попробовать использовать нормализованные  (т.е. таблицу ... из ... вместо ...), или иначе организовать weather data. Я надеюсь в дальнейшем провести сравнение различных подходов к моделированию данных.
  
# Key words to use
1. Data warehouse
2. Normalized/denormalized
3. Dimensional/star schema/model
4. ETL
5. ERD
  
|                                   |                              |                                |                                                           |
|-----------------------------------|------------------------------|--------------------------------|-----------------------------------------------------------|
|     incident_id                   |     state_name               |     juvenile_victim_count      |  $\textcolor{red}{\mathsf{offense\textunderscore{}name}}$ |
|     data_year                     |     division_name            |     total_offender_count       |     total_individual_victims                              |
| $\textcolor{red}{\mathsf{ori}}$   |     region_name              |     adult_offender_count       |     location_name                                         |
|     pub_agency_name               |     population_group_code    |     juvenile_offender_count    |     bias_desc                                             |
|     pub_agency_unit               |     population_group_desc    |     offender_race              |     victim_types                                          |
|     agency_type_name              |     incident_date            |     offender_ethnicity         |     multiple_offense                                      |
|     state_abbr                    |     adult_victim_count       |     victim_count               |     multiple_bias                                         |

|                                      |                                 |                                    |                                                               |
|--------------------------------------|---------------------------------|------------------------------------|---------------------------------------------------------------|
|     1. incident_id                   |     8. state_name               |     15. juvenile_victim_count      |  22. $\textcolor{red}{\mathsf{offense\textunderscore{}name}}$ |
|     2. data_year                     |     9. division_name            |     16. total_offender_count       |     23. total_individual_victims                              |
| 3. $\textcolor{red}{\mathsf{ori}}$   |    10. region_name              |     17. adult_offender_count       |     24. location_name                                         |
|     4. pub_agency_name               |    11. population_group_code    |     18. juvenile_offender_count    |     25. bias_desc                                             |
|     5. pub_agency_unit               |    12. population_group_desc    |     19. offender_race              |     26. victim_types                                          |
|     6. agency_type_name              |    13. incident_date            |     20. offender_ethnicity         |     27. multiple_offense                                      |
|     7. state_abbr                    |    14. adult_victim_count       |     21. victim_count               |     28. multiple_bias                                         |
  
|                                     |                                |                                  |                                                             |
|-------------------------------------|--------------------------------|----------------------------------|-------------------------------------------------------------|
|     `incident_id`                   |     `state_name`               |     `juvenile_victim_count`      |   $\textcolor{red}{\mathtt{offense\textunderscore{}name}}$  |
|     `data_year`                     |     `division_name`            |     `total_offender_count`       |     `total_individual_victims`                              |
|   $\textcolor{red}{\mathsf{ori}}$   |     `region_name`              |     `adult_offender_count`       |     `location_name`                                         |
|     `pub_agency_name`               |     `population_group_code`    |     `juvenile_offender_count`    |     `bias_desc`                                             |
|     `pub_agency_unit`               |     `population_group_desc`    |     `offender_race`              |     `victim_types`                                          |
|     `agency_type_name`              |     `incident_date`            |     `offender_ethnicity`         |     `multiple_offense`                                      |
|     `state_abbr`                    |     `adult_victim_count`       |     `victim_count`               |     `multiple_bias`                                         |
  
|                             |                                |                                 |                              |
|-----------------------------|--------------------------------|---------------------------------|------------------------------|
|     incident_id             |     data_year                  |     ori                         |     pub_agency_name          |
|     pub_agency_unit         |     agency_type_name           |     state_abbr                  |     state_name               |
|     division_name           |     region_name                |     population_group_code       |     population_group_desc    |
|     incident_date           |     adult_victim_count         |     juvenile_victim_count       |     total_offender_count     |
|     adult_offender_count    |     juvenile_offender_count    |     offender_race               |     offender_ethnicity       |
|     victim_count            |     offense_name               |     total_individual_victims    |     location_name            |
|     bias_desc               |     victim_types               |     multiple_offense            |     multiple_bias            |




<a href="https://docs.google.com/spreadsheets/d/1UC-KdQqXKUD5xAQZdmQnBcJ_u02wCe21Cco7nOhfsqA">sfd;kgjsdfhj</a> er[pojg;rtbs;m [perjpwh r totoihgrothoirthrop    ptrthortmprtm mmkdfssmlkmm] fklkfb reuwhgnlvw posrngpowrmhpond
<img align="right" src="https://github.com/AndreiMaikov/Weather_factors_in_crime--Terraform-Airflow-Redshift/blob/main/img/entire_875.png">
<br>
<br>
<img align="right" src="https://github.com/AndreiMaikov/Weather_factors_in_crime--Terraform-Airflow-Redshift/blob/main/img/upload_data_to_s3_x62.png">
<br />
<br /> 
<img align="right" src="https://github.com/AndreiMaikov/Weather_factors_in_crime--Terraform-Airflow-Redshift/blob/main/img/load_unpivot_meteoparam_tables_x62.png">
<br>
<br>
<img align="right" src="https://github.com/AndreiMaikov/Weather_factors_in_crime--Terraform-Airflow-Redshift/blob/main/img/cdtt_and_pcit_x62.png">
<br>
<br>
<p align="center">
  <img src="https://github.com/AndreiMaikov/Weather_factors_in_crime--Terraform-Airflow-Redshift/blob/main/img/create_fct_tables_x62.png">
</p>

************
![ ](https://github.com/AndreiMaikov/Weather_factors_in_crime--Terraform-Airflow-Redshift/blob/main/img/entire_785.png)

![ ](https://github.com/AndreiMaikov/Weather_factors_in_crime--Terraform-Airflow-Redshift/blob/main/img/create_fct_tables_x56.png)

