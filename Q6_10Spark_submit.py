# spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar  Q6_10Spark_submit.py

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Crimes_in_Boston')\
    .getOrCreate()

# edit your  DB info here
Properties = {'user': 'amrit', 'password': '1234'}
URL = 'jdbc:postgresql://localhost:5432/crimes_in_boston'

###############

# Loads csv to spark dataframe
crimes_df = spark.read.csv('DATA/crimes.csv', header=True, inferSchema=True)
offense_codes_df = spark.read.csv(
    'DATA/offense_codes.csv', header=True, inferSchema=True)
police_district_codes_df = spark.read.csv(
    'DATA/police_district_codes.csv', header=True, inferSchema=True)


#################### Preprocessing  ##############
# for SHOOTING column replace null values with 'N'
crimes_df = crimes_df.withColumn('SHOOTING', F.when(F.col('SHOOTING').isNull(), 'N')
                                 .otherwise(F.col('SHOOTING')))

# remove OFFENSE_DESCRIPTION column
crimes_df = crimes_df.drop('OFFENSE_DESCRIPTION')
crimes_df.show(5)

# keep only first duplicate value in offence_code_df
offense_codes_df = offense_codes_df.dropDuplicates(['CODE'])
offense_codes_df.sort('CODE').show(5)

#####################  Questions  ###################

# 6. (window function) Partition by district , order by  year and then rolling count the offenses

window = Window.partitionBy('DISTRICT').orderBy('YEAR')

window_df = crimes_df.filter(
    F.col('DISTRICT').isNotNull())  # remove null values

window_df = window_df.groupBy('DISTRICT', 'YEAR').agg(
    F.count('INCIDENT_NUMBER').alias('count_incidents'))

window_df = window_df.withColumn(
    'rolling_count_incidents', F.sum('count_incidents').over(window))

window_df.show()


################### SAVE to POSTGRES #######################
window_df.write.jdbc(url=URL, table='rolling_count',
                     mode='overwrite', properties=Properties)

# 7. Pivot incident years and count incident monthwise

pivot_df = crimes_df.groupBy('MONTH').pivot('YEAR').count().sort('MONTH')
pivot_df.show()

################### SAVE to POSTGRES #######################
pivot_df.write.jdbc(url=URL, table='pivot_YEAR',
                    mode='overwrite', properties=Properties)


# 8. Count crimes involving any kind of "Robbery" in  each district name wise. district name is in police_district_codes_df

robbery_df = crimes_df.filter(F.col('OFFENSE_CODE_GROUP') == 'Robbery')

robbery_df = robbery_df.join(police_district_codes_df, robbery_df.DISTRICT ==
                             police_district_codes_df.District_Code, how='left')

robbery_df = robbery_df.filter(
    F.col('DISTRICT').isNotNull())  # remove null values

robbery_df = robbery_df.groupBy(
    'DISTRICT', 'District_Name').count().sort('DISTRICT')

robbery_df.show()

################### SAVE to POSTGRES #######################
robbery_df.write.jdbc(url=URL, table='robbery_in_each_district',
                      mode='overwrite', properties=Properties)

# 9. For each day, list the hour when the incident number is highest alonmg with the count of incidents

incident_df = crimes_df.groupBy(
    'DAY_OF_WEEK', 'HOUR').count().sort('DAY_OF_WEEK', 'HOUR')

incident_df = incident_df.withColumn('max_incident', F.max(
    'count').over(Window.partitionBy('DAY_OF_WEEK')))

incident_df = incident_df.filter(F.col('count') == F.col('max_incident'))

incident_df = incident_df.drop('max_incident')

incident_df.show()

################### SAVE to POSTGRES #######################
incident_df.write.jdbc(url=URL, table='Day_incident_hour',
                       mode='overwrite', properties=Properties)


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
    url=URL, table='highest_crime_in_each_district_with_count', mode='overwrite', properties=Properties)
