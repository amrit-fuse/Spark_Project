# Apache Spark Final Project

This project include  topics from SQl and  Pyspark and additional mentions like cron job and unit testing

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

+ Create a cron job using `sudo crontab -e` to run the script every minute


            # * * * * *  [path to script to execute] > [path to log file] 2>&1
            * * * * * /home/amrit/Spark_Project/Schedule_every_minute.sh >  /home/amrit/Spark_Project/corn.log 2>&1 


## Create and activate a virtual environment:

`>> python -m venv env_name`

`>> . env_name/bin/activate`

Use `pip install -r requirements.txt` to install the required packages.

##  Folders/Files and Description

|**Folders/Files**|**Description**|
|---|---|
|**Assests**|Contains the images used in the README.md file|
|**DATA**|Contains the colected data files in csv format|
|**.gitignore**|Contains the files and folders to be ignored by git|
|**All_Questions.ipynb**|Contains all the questions and answers in notebook format|
|**All_details.py**|Contains the user/passs, driver patha and postgresql jar file path for each member|
|**README.md**|Contains the project description|
|**Scheule_every_minute.sh**|Contains the cron job script that runs every minute ( AMRIT)|
|**Spark_submit_Questions.py**|Contains the spark_submit tailored for the project|
|**cron.log**|Contains the cron job log|
|**cron_job_Questions.py**|Containns the spark_submit tailored for cron job|
|**requirements.txt**|Contains the required packages to run the project|
|**schedule_job.sh**|Contains the cron job script that runs every  minute ( SAURAV)|
|**schedule_jobshijal.sh**|Contains the cron job script that runs every  minute ( SHIJAL)|\



|**Question**|**Snap from Database**|
|---|---|
|1. Find all the list of dates in 2017 where ‘VANDALISM’ happened.|![](/Asset/Q1.jpg) |
|2. Show the data frame where the District is  null and then fill the null District with “District not Verified”. (udf)| ![](/Asset/Q2.jpg) |
|3. Show the year and total number of Robbery happens in each year.| ![](/Asset/Q3.jpg) |
|4. Show all  Offense_codes and names which are not listed in crime.csv but in offense_code.csv.| ![](/Asset/Q4.jpg) |
|5. List offense_description which is occurred on Sunday around time ‘21:30:00’| ![](/Asset/Q5.jpg) |
|6. (window function) Partition by district , order by  year and then count the offenses.| ![](/Asset/Q6.jpg) |
|7. Pivot incident years and count incident monthwise| ![](/Asset/Q7.jpg) |
|8. Count crimes involving any kind of Robbery in   each district| ![](/Asset/Q8.jpg) |
|9. For each day, list the hour when the incident number is highest.| ![](/Asset/Q9.jpg) |
|10. List highest crime/offense group in each district (name) and the number of incident| ![](/Asset/Q10.jpg) |
|11. Find the  number of crime happened  for each year| ![](/Asset/Q11.jpg) |
|12. How many Verbal Disputes crimes were committed in 2018.| ![](/Asset/Q12.jpg) |
|13. Find how many times ‘Auto Theft’ happened in each year| ![](/Asset/Q13.jpg) |
|14. Reporting Area having highest Shooting incident. | ![](/Asset/Q14.jpg) |
|15. Arrange street based on high “Homicide” incident| ![](/Asset/Q15.jpg) |
|16. As each degree of longitude is 111 km apart,  list crimes with counts (yearwsie)  within a 100 km radius of BOSTON police headquarter which is at 42.33397849555639, -71.09079628933894 (lat, long) | ![](/Asset/Q16.jpg) |
|17. List all crimes that occurred in all district (namewsie) and in the  August 2016|![](/Asset/Q17.jpg) |





