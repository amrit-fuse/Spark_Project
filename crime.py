from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf,col,countDistinct,date_format,row_number
from pyspark.sql.window import Window

'''spark-submit --driver-class-path /usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.5.0.jar netflix.p
'''

spark = SparkSession.builder.appName("crime_boston").getOrCreate()

crime_df = spark.read.csv("/home/saurav/Downloads/crime.csv",header=True,inferSchema=True)

crime_df.show()


#1. Find all the list of dates in 2017 where ‘VANDALISM’ happened.
vandalism_2017 = crime_df.filter(crime_df['OFFENSE_DESCRIPTION']=='VANDALISM').select(crime_df['OCCURRED_ON_DATE'],crime_df['OFFENSE_DESCRIPTION'])
vandalism_2017.show()

vandalism_2017.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Q1', user='fusemachines', password='hello123').mode('overwrite').save()




#2.Show the data frame where the District is  null and then fill the null District with “District not Verified”. (udf) 
def remove_na(replacenull):
    return "District Not Verifed"
udf_name = udf(remove_na)

null_district = crime_df.filter(crime_df['DISTRICT'].isNull())
null_district.show()

fill_na = null_district.select(crime_df['INCIDENT_NUMBER'],crime_df['OFFENSE_CODE'],crime_df['OFFENSE_CODE_GROUP'],crime_df['OFFENSE_DESCRIPTION'],udf_name(null_district['DISTRICT']))\
.withColumnRenamed('remove_na(DISTRICT)','DISTRICT')

fill_na.show()

# *******ALTERNATIVE**********
# null_district.na.fill('District not verified',subset=['DISTRICT']).show()

fill_na.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Q2', user='fusemachines', password='hello123').mode('overwrite').save()



# 3.Show the year and total number of Robbery happens in each year.
filtering_robbery = crime_df.filter((crime_df.OFFENSE_CODE_GROUP=="Robbery")).select(crime_df.YEAR,crime_df.OFFENSE_CODE_GROUP)
total_robbery = filtering_robbery.groupBy("YEAR").count().orderBy("Year").withColumnRenamed("count","Total Robbery in Year")
total_robbery.show()

total_robbery.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Q3', user='fusemachines', password='hello123').mode('overwrite').save()



# 4.Show all Offense_codes and names which are not listed in crime.csv but in offense_code.csv.
dfj1 = spark.read.csv("/home/saurav/Downloads/crime.csv",header=True,inferSchema=True)
dfj2 = spark.read.csv("/home/saurav/Downloads/offense_codes.csv",header=True,inferSchema=True)

result = dfj2.join(dfj1,dfj1.OFFENSE_CODE==dfj2.CODE,"left_anti")
result.show()

result.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Q4', user='fusemachines', password='hello123').mode('overwrite').save()



# 5.List offense_description which is occurred on Sunday around time ‘21:30:00’
filter_day = crime_df.filter((crime_df.DAY_OF_WEEK == 'Sunday')).select(crime_df.OFFENSE_DESCRIPTION,crime_df.DAY_OF_WEEK,crime_df.OCCURRED_ON_DATE)
select_time = filter_day.select(crime_df.OFFENSE_DESCRIPTION,date_format('OCCURRED_ON_DATE','HH:mm:ss')).withColumnRenamed('date_format(OCCURRED_ON_DATE, HH:mm:ss)','Time')
final_result =select_time.filter(select_time.Time == '21:30:00')
final_result.show()

final_result.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Q5', user='fusemachines', password='hello123').mode('overwrite').save()