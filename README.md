# Apache Spark Final Project

This project include  topics from SQl, Pyspark, Pandas  and ORM

## Team members
1. [Amrit Prasad Phuyal](https://github.com/amrit-fuse)
2. [Saurav karki](https://github.com/saurav-fusemachines)
3. [Shijal Sharma Poudel](https://github.com/Shijal12)


+ Configure driver path

            spark = SparkSession.builder.appName('crime_boston')\
            .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar')\
            .getOrCreate()

+ Edit database connection string
 
            Dataframe_name.write.format('jdbc').options( url='jdbc:postgresql://127.0.0.1/Database_name', driver='org.postgresql.Driver', dbtable='Table_name', user='USER_NAME', password='****').mode('overwrite').save()

|**Question**|**PROCESS TO SOLVE**|
|---|---|
|1.Find all the list of dates in 2017 where ‘VANDALISM’ happened.| Filter the data where offense_description is Vandalism and then select the date and description from filtered data. |
|2.Show the data frame where the District is  null and then fill the null District with “District not Verified”. (udf)|First filter the dataframe using a filter function where district is null and then fill null values with “District not Verified “ by using na.fill( ) method.|
|3.Show the year and total number of Robbery happens in each year.|Filter the dataframe where the OFFENSE_CODE_GROUP = “Robbery” and then use groupBy(“YEAR”)and count( ) the groupby function.|
|4.Show all  Offense_codes and names which are not listed in crime.csv but in offense_code.csv.|Read the both dataframe and then use “left_anti_joins”
To exclude the matching conditions|
|5.List offense_description which is occurred on Sunday around time ‘21:30:00’|At first filter the data where DAY_OF_WEEK = ‘Sunday’ and then, from the filtered data format only timestamp from column ‘OCCURRED_ON_DATE’. Finally, filter the data where Time == ‘21:30:00’.|



