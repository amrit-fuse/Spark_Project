## AMRIT

# modify the path to your script and driver location
echo "RUNNING  SPARK JOB "
/opt/spark/bin/spark-submit --driver-class-path /usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.5.0.jar   /home/amrit/Spark_Project/cron_job_Questions.py 
