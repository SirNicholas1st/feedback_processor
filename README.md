# feedback_processor
WIP This repository contains a simple Flask application that serves as a customer feedback form. The submitted data is sent to a Kafka topic. The Kafka consumer, implemented as a Spark application, processes the data and writes it to a Cassandra table for further analysis.


# Setup 

0. Note JDK is required for this project so it needs to be downloaded first.
1. Setup Kafka and create a topic for the project. Check this repositorys readme for info: https://github.com/SirNicholas1st/kafka_datastream
2. Setup Cassandra and create a keyspace for the project. The repository above also includes a how to do this.
3. Install Spark. Details below:

## Installing Apache Spark

1. Download Apache Spark from here https://spark.apache.org/downloads.html. Package type: *Pre-built for Apache Hadoop 2.7*
2. Create a new folder to for example to your C-drive and extract the file there.
3. Download winutils exe file from here: https://github.com/steveloughran/winutils. Select the correct hadoop version and open the bin directory and download *winutils.exe* file
4. Create a new folder called "hadoop" and a folder called "bin" under the created hadoop folder and paste the downloaded file to the bin directory.
5. Use windows search to find "Edit the system environment variables"
6. Select Environment variables

![EnvVars](Pics/environment_vars_1.png)

7. Create the following variables: JAVA_HOME path: jdk folder, HADOOP_HOME path: the hadoop folder you created, SPARK_HOME path: the extracted folder under the created spark folder, PYSPARK_HOME path: your python folder + python.exe for example D:\Python\python.exe.
8. Under system variables select "Path"
![path](Pics/path.png)
9. Click edit and add the following paths: %HADOOP_HOME%/bin, %SPARK_HOME%/bin
10. Verify the installation by opening a cmd and starting a spark shell with the command "spark-shell". The output should be the following:
![SparkShell](Pics/spark_shell.png)
NOTE: The start up might take a while and if the the command doesnt work, run the cmd as and admin.
