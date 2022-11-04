
# AMRIT

# spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar Spark_submit_Questions.py

# SAURAV
# spark-submit --driver-class-path /usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.5.0.jar Spark_submit_Questions.py


# SHIJAL
# spark-submit --driver-class-path /usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.5.0.jar Spark_submit_Questions.py


from All_details import *
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf, col, countDistinct, date_format, row_number
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Crimes_in_Boston')\
    .getOrCreate()


# Loads csv to spark dataframe
crimes_df = spark.read.csv('DATA/crimes.csv', header=True, inferSchema=True)
offense_codes_df = spark.read.csv(
    'DATA/offense_codes.csv', header=True, inferSchema=True)
police_district_codes_df = spark.read.csv(
    'DATA/police_district_codes.csv', header=True, inferSchema=True)


# function to test if imported data is  not empty

def test_empty_df(df):
    if df.count() == 0:
        return False
    else:
        return True


# Function to test Schema and  rows count of data in databse
def test_database(df, table_name):
    if (spark.read.jdbc(url=URL, table=table_name, properties=Properties).schema != df.schema) and (spark.read.jdbc(url=URL, table=table_name, properties=Properties).count() != df.count()):
        return False
    else:
        return True


# test if dataframes are not empty
assert test_empty_df(crimes_df), 'crimes_df is empty'
assert test_empty_df(offense_codes_df), 'offense_codes_df is empty'
assert test_empty_df(
    police_district_codes_df), 'police_district_codes_df is empty'

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

# fill empty string in REPORTING_AREA column with  null
crimes_df = crimes_df.withColumn('REPORTING_AREA', F.when(F.col('REPORTING_AREA') == ' ', None)
                                 .otherwise(F.col('REPORTING_AREA')))


### QUESTIONS   QUESTIONS   QUESTIONS  QUESTIONS QUESTIONS     QUESTIONS   QUESTIONS  QUESTIONS QUESTIONS  ###


# 1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    1.    #
# 1. Find all the list of dates in 2017 where ‘VANDALISM’ happened.

join_offense_code = crimes_df.join(
    offense_codes_df, crimes_df.OFFENSE_CODE == offense_codes_df.CODE, "inner")
vandalism_2017 = join_offense_code.filter((offense_codes_df['Name'] == 'VANDALISM') & (
    crimes_df['Year'] == 2017)).select(crimes_df['OCCURRED_ON_DATE'], offense_codes_df['Name'])
vandalism_2017.show()

############### SAVE to POSTGRES ###############
vandalism_2017.write.jdbc(url=URL, table='vandalism_2017',
                          mode='overwrite', properties=Properties)

##############  TEST  ##############
assert test_database(
    vandalism_2017, 'vandalism_2017'), 'vandalism_2017 is having different schema or count'

# 2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    2.    #
# 2.Show the data frame where the District is  null and then fill the null District with “District not Verified”. (udf)


def remove_na(replacenull):
    return "District Not Verifed"


udf_name = udf(remove_na)

null_district = crimes_df.filter(crimes_df['DISTRICT'].isNull())
# null_district.show()

fill_na = null_district.select(crimes_df['INCIDENT_NUMBER'], crimes_df['OFFENSE_CODE'], crimes_df['OFFENSE_CODE_GROUP'], udf_name(null_district['DISTRICT']))\
    .withColumnRenamed('remove_na(DISTRICT)', 'DISTRICT')

fill_na.show()

################### SAVE to POSTGRES ###################
fill_na.write.jdbc(url=URL, table='fill_na',
                   mode='overwrite', properties=Properties)

##################  TEST  ##################

assert test_database(
    fill_na, 'fill_na'), 'fill_na is having different schema or count'

# 3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    3.    #
# 3.Show the year and total number of Robbery happens in each year.
filtering_robbery = crimes_df.filter((crimes_df.OFFENSE_CODE_GROUP == "Robbery")).select(
    crimes_df.YEAR, crimes_df.OFFENSE_CODE_GROUP)
total_robbery = filtering_robbery.groupBy("YEAR").count().orderBy(
    "Year").withColumnRenamed("count", "Total Robbery in Year")
total_robbery.show()


################### SAVE to POSTGRES ###################
total_robbery.write.jdbc(url=URL, table='total_robbery',
                         mode='overwrite', properties=Properties)

##################  TEST  ##################

assert test_database(
    total_robbery, 'total_robbery'), 'total_robbery is having different schema or count'

# 4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    4.    #
# 4.Show all Offense_codes and names which are not listed in crime.csv but in offense_code.csv.

missing_offences = offense_codes_df.join(
    crimes_df, offense_codes_df.CODE == crimes_df.OFFENSE_CODE, "left_anti")

missing_offences.show()

################### SAVE to POSTGRES ###################
missing_offences.write.jdbc(
    url=URL, table='missing_offences', mode='overwrite', properties=Properties)

##################  TEST  ##################

assert test_database(
    missing_offences, 'missing_offences'), 'missing_offences is having different schema or count'


# 5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    5.    #

# 5.List offense_description which is occurred on Sunday around time ‘21:30:00’

merge_df = crimes_df.join(
    offense_codes_df, crimes_df.OFFENSE_CODE == offense_codes_df.CODE, "inner")

sunday_2130 = merge_df.filter((merge_df['DAY_OF_WEEK'] == 'Sunday') & (
    merge_df['OCCURRED_ON_DATE'].contains('21:30:00'))).select(merge_df['NAME'])

sunday_2130.show()


################### SAVE to POSTGRES ###################
sunday_2130.write.jdbc(url=URL, table='sunday_2130',
                       mode='overwrite', properties=Properties)

##################  TEST  ##################

assert test_database(
    sunday_2130, 'sunday_2130'), 'sunday_2130 is having different schema or count'
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
    window_df, 'rolling_count'), 'rolling_count table is having different schema or count'


# 7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    7.    #

# 7. Pivot incident years and count incident monthwise

pivot_df = crimes_df.groupBy('MONTH').pivot('YEAR').count().sort('MONTH')
pivot_df.show()

############################## SAVE to POSTGRES ###############
pivot_df.write.jdbc(url=URL, table='pivot_YEAR',
                    mode='overwrite', properties=Properties)

################# TEST ################
assert test_database(
    pivot_df, 'pivot_YEAR'), 'pivot_YEAR table is having different schema or count'


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
    robbery_df, 'robbery_in_each_district'), 'robbery_in_each_district table is having different schema or count'


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
    incident_df, 'Day_incident_hour'), 'Day_incident_hour table is having different schema or count'


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
                     'district_crime_count'), 'district_crime_count table is having different schema or count'


# 11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    11.    #
#
# 11. Find the  number of crime happened  for each year

year_crime_count = crimes_df.groupBy('YEAR').count().sort('YEAR')

year_crime_count.show()


################### SAVE to POSTGRES #######################
year_crime_count.write.jdbc(
    url=URL, table='year_crime_count', mode='overwrite', properties=Properties)

#################### TEST ###############################

assert test_database(
    year_crime_count, 'year_crime_count'), 'year_crime_count table is having different schema or count'


# 12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    12.    #


# 12. How many Verbal Disputes crimes were committed in 2018.

verbal_dispute = crimes_df.filter(
    F.col('OFFENSE_CODE_GROUP') == 'Verbal Disputes')

verbal_dispute = verbal_dispute.filter(F.col('YEAR') == 2018)

verbal_dispute = verbal_dispute.count()

verbal_dispute_df = spark.createDataFrame([(verbal_dispute,)], ['count'])

verbal_dispute_df.show()

################### SAVE to POSTGRES #######################

verbal_dispute_df.write.jdbc(
    url=URL, table='verbal_dispute', mode='overwrite', properties=Properties)

#################### TEST ###############################

assert test_database(
    verbal_dispute_df, 'verbal_dispute'), 'verbal_dispute table is having different schema or count'


# 13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    13.    #

# 13. Find how many times ‘Auto Theft’ happened in each year

auto_theft = crimes_df.filter(F.col('OFFENSE_CODE_GROUP') == 'Auto Theft')

auto_theft = auto_theft.groupBy('YEAR').count().sort('YEAR')

auto_theft.show()

################### SAVE to POSTGRES #######################
auto_theft.write.jdbc(url=URL, table='auto_theft',
                      mode='overwrite', properties=Properties)

#################### TEST ###############################

assert test_database(
    auto_theft, 'auto_theft'), 'auto_theft table is having different schema or count'


# 14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    14.    #


# 14.  Reporting Area having highest Shooting incident.

shooting_df = crimes_df.where(crimes_df.SHOOTING == 'Y')

shooting_df = shooting_df.groupBy(
    'REPORTING_AREA').count().sort('count', ascending=False)

shooting_df = shooting_df.filter(F.col('REPORTING_AREA').isNotNull())

shooting_df.show()

################### SAVE to POSTGRES #######################
shooting_df.write.jdbc(url=URL, table='shooting_df',
                       mode='overwrite', properties=Properties)

#################### TEST ###############################
assert test_database(
    shooting_df, 'shooting_df'), 'shooting_df table is having different schema or count'


# 15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    15.    #


# 15. Arrange street based on high “Homicide” incident

homicide_df = crimes_df.where(crimes_df.OFFENSE_CODE_GROUP == 'Homicide')

homicide_df = homicide_df.groupBy(
    'STREET').count().sort('count', ascending=False)

homicide_df = homicide_df.filter(F.col('STREET').isNotNull())

homicide_df.show()

################### SAVE to POSTGRES #######################
homicide_df.write.jdbc(url=URL, table='homicide_df',
                       mode='overwrite', properties=Properties)

#################### TEST ###############################
assert test_database(
    homicide_df, 'homicide_df'), 'homicide_df table is having different schema or count'


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
                     'crimes_year_radius'), 'crimes_year_radius table is having different schema or count'


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
                     'list_crimes_aug_2016'), 'list_crimes_aug_2016 table is having different schema or count'


# 18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    18.    #


# 19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    19.    #


# 20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    20.    #


# 21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    21.    #
