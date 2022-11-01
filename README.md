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
|Find all the list of dates in 2017 where ‘VANDALISM’ happened.| Filter the data where offense_description is Vandalism and then select the date and description from filtered data. |

