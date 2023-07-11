# Columns of the table wxcr.fct_crime_incidents

|            |                  |                  |
|------------|------------------|------------------|
| A. id<sup><a href="#primary_key">1</a></sup>     | C. hour_id          | E. incident_time |
| B. date_id                                       | D. incident_date    | F. name_cnt      |
| (3) ori                    | (17) adult_offender_count    | (23) total_individual_victims |
| (11) population_group_code | (18) juvenile_offender_count | (24) location_name            |
| (12) population_group_desc | (19) offender_race           | (25) bias_desc                |
| (14) adult_victim_count    | (20) offender_ethnicity      | (26) victim_types             |
| (15) juvenile_victim_count | (21) victim_count            | (27) multiple_offense         |
| (16) total_offender_count  | (22) offences<sup><a href="#offenses">2</a></sup>  | (28) multiple_bias   | 
| [1] simple assault                              | [17] statutory rape                             | [33] theft of motor vehicle parts or accessories |
| [2] murder and nonnegligent manslaughter        | [18] human trafficking, commercial sex acts     | [34] drug equipment violations                   |
| [3] burglary/breaking & entering                | [19] assisting or promoting prostitution        | [35] stolen property offenses                    |
| [4] rape                                        | [20] welfare fraud                              | [36] pornography/obscene material                |
| [5] drug/narcotic violations                    | [21] betting/wagering                           | [37] sodomy                                      |
| [6] shoplifting                                 | [22] aggravated assault                         | [38] pocket-picking                              |
| [7] fondling                                    | [23] intimidation                               | [39] purse-snatching                             |
| [8] theft from building                         | [24] destruction/damage/vandalism of property   | [40] sexual assault with an object               |
| [9] embezzlement                                | [25] robbery                                    | [41] prostitution                                |
| [10] kidnapping/abduction                       | [26] arson                                      | [42] hacking/computer invasion                   |
| [11] theft from coin-operated machine or device | [27] counterfeiting/forgery                     | [43] purchasing prostitution                     |
| [12] false pretenses/swindle/confidence game    | [28] all other larceny                          | [44] identity theft                              |
| [13] impersonation                              | [29] motor vehicle theft                        | [45] bribery                                     |
| [14] incest                                     | [30] weapon law violations                      | [46] negligent manslaughter                      |
| [15] extortion/blackmail                        | [31] theft from motor vehicle                   | [47] animal cruelty                              |
| [16] wire fraud                                 | [32] credit card/automated teller machine fraud | [48] not specified                               |

Column A assigns unique identifiers to crime incident records.

Columns B &ndash; E include the values of each incident's date and time, along with their ids that connect the table with `dim_dates` and `dim_hours` tables.

For each incident, column F gives the number of standard/uncombined offense names used in `crime_incidents.csv` (see <a href="/Readme.md">Readme.md</a> and <a href="/docs/Offense%20names.md">Offense names.md</a>).

Columns (3), (11), (12), (14) &ndash; (28) hold the data from <a href="/docs/Columns%20of%20crime_incidents.csv.md">the columns of crime_incidents.csv</a> with the same names (the rest of the file's columns are not used, as they contain the information provided in <a href="/src/airflow/data/agencies.csv">agencies.csv</a>).

The names of columns [1] &ndash; [48] are exactly the same as the standard/uncombined offense names; the values of such a column are flags indicating whether the name is relelvant to a given incident.


--------------------------------------------

Footnotes:

1. <a name="primary_key">Primary key</a> <br>
2. <a name="offenses">Renamed offense_name from crime_incidents.csv
  

  
