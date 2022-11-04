# Refer to Schedule_every_5_minutes.py for more details on how to schedule a job
# < sudo crontab -e >    to edit the cron job
# cro.log file to view the logs
#  Refer to https://www.notion.so/amiright/Postgres-in-WSL-7f9cb5767e5744489b77841cd248a60b for more details on how to setup postgres in WSL, saprk-submit, cronjobs etc
#


from All_details import *
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Crimes_in_Boston').getOrCreate()

# Loads csv to spark dataframe
crimes_df = spark.read.csv(
    project_dir+'DATA/crimes.csv', header=True, inferSchema=True)
offense_codes_df = spark.read.csv(
    project_dir+'DATA/offense_codes.csv', header=True, inferSchema=True)
police_district_codes_df = spark.read.csv(
    project_dir+'DATA/police_district_codes.csv', header=True, inferSchema=True)


# function to test if imported data is  not empty
def test_empty_df(df):
    if df.count() == 0:
        return True
    else:
        return False


# Function to test Schema and  rows count of data in databse
def test_database(df, table_name):
    if spark.read.jdbc(url=URL, table=table_name, properties=Properties).schema == df.schema and spark.read.jdbc(url=URL, table=table_name, properties=Properties).count() == df.count():
        return True
    else:
        return False


# test if dataframes are not empty
assert test_empty_df(crimes_df) == False, 'crimes_df is empty'
assert test_empty_df(offense_codes_df) == False, 'offense_codes_df is empty'
assert test_empty_df(
    police_district_codes_df) == False, 'police_district_codes_df is empty'

###  PREPROCESSING   PREPROCESSING   PREPROCESSING  PREPROCESSING PREPROCESSING   PREPROCESSING  PREPROCESSING PREPROCESSING ##

# for SHOOTING column replace null values with 'N'
crimes_df = crimes_df.withColumn('SHOOTING', F.when(F.col('SHOOTING').isNull(), 'N')
                                 .otherwise(F.col('SHOOTING')))

# remove OFFENSE_DESCRIPTION column
crimes_df = crimes_df.drop('OFFENSE_DESCRIPTION')
crimes_df.show(5)

# keep only first duplicate value in offence_code_df
offense_codes_df = offense_codes_df.dropDuplicates(['CODE'])
offense_codes_df.sort('CODE').show(5)


### QUESTIONS   QUESTIONS   QUESTIONS  QUESTIONS QUESTIONS     QUESTIONS   QUESTIONS  QUESTIONS QUESTIONS  ###


# 1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    #


# 2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    #


# 3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    #


# 4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    #


# 5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    #


# 6.    6.    6.    6.    6.    6.    6.    6.    6.    6.    6.    6.    6.    6.    6.    6.    6.    6.    #


# 6. (window function) Partition by district , order by  year and then rolling count the offenses

window = Window.partitionBy('DISTRICT').orderBy('YEAR')

window_df = crimes_df.filter(
    F.col('DISTRICT').isNotNull())  # remove null values

window_df = window_df.groupBy('DISTRICT', 'YEAR').agg(
    F.count('INCIDENT_NUMBER').alias('count_incidents'))

window_df = window_df.withColumn(
    'rolling_count_incidents', F.sum('count_incidents').over(window))

window_df.show()


############### SAVE to POSTGRES ###############
window_df.write.jdbc(url=URL, table='rolling_count',
                     mode='overwrite', properties=Properties)


############### TEST ###############
assert test_database(
    window_df, 'rolling_count') == False, 'rolling_count table is having different schema or count'


# 7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    #

# 7. Pivot incident years and count incident monthwise

pivot_df = crimes_df.groupBy('MONTH').pivot('YEAR').count().sort('MONTH')
pivot_df.show()

############################## SAVE to POSTGRES ###############
pivot_df.write.jdbc(url=URL, table='pivot_YEAR',
                    mode='overwrite', properties=Properties)

################# TEST ################
assert test_database(
    pivot_df, 'pivot_YEAR') == False, 'pivot_YEAR table is having different schema or count'


# 8.    8.    8.    8.    8.    8.    8.    8.    8.    8.    8.    8.    8.    8.    8.    8.    8.    8.    #


# 8. Count crimes involving any kind of "Robbery" in  each district name wise. district name is in police_district_codes_df

robbery_df = crimes_df.filter(F.col('OFFENSE_CODE_GROUP') == 'Robbery')

robbery_df = robbery_df.join(police_district_codes_df, robbery_df.DISTRICT ==
                             police_district_codes_df.District_Code, how='left')

robbery_df = robbery_df.filter(
    F.col('DISTRICT').isNotNull())  # remove null values

robbery_df = robbery_df.groupBy(
    'DISTRICT', 'District_Name').count().sort('DISTRICT')

robbery_df.show()

################## SAVE to POSTGRES ################
robbery_df.write.jdbc(url=URL, table='robbery_in_each_district',
                      mode='overwrite', properties=Properties)

################# TEST #################
assert test_database(
    robbery_df, 'robbery_in_each_district') == False, 'robbery_in_each_district table is having different schema or count'


# 9.    9.    9.    9.    9.    9.    9.    9.    9.    9.    9.    9.    9.    9.    9.    9.    9.    9.    #

# 9. For each day, list the hour when the incident number is highest alonmg with the count of incidents

incident_df = crimes_df.groupBy(
    'DAY_OF_WEEK', 'HOUR').count().sort('DAY_OF_WEEK', 'HOUR')

incident_df = incident_df.withColumn('max_incident', F.max(
    'count').over(Window.partitionBy('DAY_OF_WEEK')))

incident_df = incident_df.filter(F.col('count') == F.col('max_incident'))

incident_df = incident_df.drop('max_incident')

incident_df.show()

################ SAVE to POSTGRES #################
incident_df.write.jdbc(url=URL, table='Day_incident_hour',
                       mode='overwrite', properties=Properties)

################# TEST ##################
assert test_database(
    incident_df, 'Day_incident_hour') == False, 'Day_incident_hour table is having different schema or count'


# 10.    10.    10.    10.    10.    10.    10.    10.    10.    10.    10.    10.    10.    10.    10.    10.    10.    10.    #

# 10. List highest crime/offense group in each district (name) and the number of incidents

district_crime_count = crimes_df.join(
    police_district_codes_df, crimes_df.DISTRICT == police_district_codes_df.District_Code, how='left')

district_crime_count = district_crime_count.filter(
    F.col('DISTRICT').isNotNull())  # remove null values

district_crime_count = district_crime_count.groupBy(
    'DISTRICT', 'District_Name', 'OFFENSE_CODE_GROUP').count().sort('DISTRICT', 'count', ascending=False)

district_crime_count = district_crime_count.dropDuplicates(
    ['DISTRICT'])  # keep only first duplicate value
# as we sorted the dataframe in descending order of count column so first value will be highest

district_crime_count.show()

################### SAVE to POSTGRES #######################
district_crime_count.write.jdbc(
    url=URL, table='district_crime_count', mode='overwrite', properties=Properties)

#################### TEST ###############################
assert test_database(district_crime_count,
                     'district_crime_count') == False, 'district_crime_count table is having different schema or count'


# 11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    #
#


# 12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    #


# 13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    #


# 14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    #


# 15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    #


# 16.    16.    16.    16.    16.    16.    16.    16.    16.    16.    16.    16.    16.    16.    16.    16.    16.    16.    #

# 16. As each degreee of longitude is 111km apart,  list crimes with counts (yearwsie)  within a 100 km radius of BOSTON police headquater which is at 42.33397849555639, -71.09079628933894 (lat, long)

# udf to calculate distance between two
def degree_distance(lat1, long1, lat2, long2):
    # return distance in km
    return 111 * F.sqrt(F.pow(lat1 - lat2, 2) + F.pow(long1 - long2, 2))


F.udf(degree_distance, FloatType())

crimes_radius_111_df = crimes_df.withColumn('Distance_Apart', degree_distance(
    F.col('Lat'), F.col('Long'), 42.33397849555639, -71.09079628933894))

crimes_radius_111_df = crimes_radius_111_df.filter(
    F.col('Distance_Apart') <= 111)

crimes_radius_111_df = crimes_radius_111_df.groupBy(
    'OFFENSE_CODE_GROUP').pivot('YEAR').count().sort('OFFENSE_CODE_GROUP')

crimes_radius_111_df.show()


################### SAVE to POSTGRES #######################
crimes_radius_111_df.write.jdbc(
    url=URL, table='crimes_year_radius', mode='overwrite', properties=Properties)

#################### TEST ###############################
assert test_database(crimes_radius_111_df,
                     'crimes_year_radius') == False, 'crimes_year_radius table is having different schema or count'


# 17.    17.    17.    17.    17.    17.    17.    17.    17.    17.    17.    17.    17.    17.    17.    17.    17.    17.    #


# 17.  List all crimes that occurred in all district (namewsie) and in the  August 2016

crimes_august_2016_df = crimes_df.filter(
    F.col('YEAR') == 2016).filter(F.col('MONTH') == 8)

crimes_august_2016_df = crimes_august_2016_df.join(
    police_district_codes_df, crimes_august_2016_df.DISTRICT == police_district_codes_df.District_Code, how='left')

crimes_august_2016_df = crimes_august_2016_df.filter(
    F.col('DISTRICT').isNotNull())  # remove null values


crimes_august_2016_df = crimes_august_2016_df.groupBy('District_Name').agg(
    F.collect_set('OFFENSE_CODE_GROUP').alias('CRIME_GROUP')).sort('DISTRICT_NAME')

crimes_august_2016_df.show()

################### SAVE to POSTGRES #######################
crimes_august_2016_df.write.jdbc(
    url=URL, table='list_crimes_aug_2016', mode='overwrite', properties=Properties)


#################### TEST ###############################
assert test_database(crimes_august_2016_df,
                     'list_crimes_aug_2016') == False, 'list_crimes_aug_2016 table is having different schema or count'


# 18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    #


# 19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    #


# 20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    #


# 21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    #
