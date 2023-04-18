#CSV_TO_DELTA

Download pyspark 3.2.4 and delta-spark 1.2.1

After creating the python file
create a docker file 

build the docker file using 
"winpty docker build -t codetask ."
"winpty docker run -it codetask"
run the container


1. Design and code a Spark Job that ingests 1 or multiple CSV files into Deltalake

The function accepts csv files from the input folder and transforms the csv files using spark and delta, then writes the output into a table.

2. Produce a Docker Compose YAML file to run the job
In other to do this I created the docker file
based on the requirement, the development language is Python3, The pyspark version is 3.3.1 and deltalake is version 1.2.1
following the documentaion of delta-spark seen below
https://docs.delta.io/latest/releases.html
pyspark 3.3.1 is not compatible with deltalake 1.2.1 so i used pyspark 3.2.4
In my dockerfile, 
I used the OpenJDK 11 JRE (Slim version) as the base image to build my container and set up my enviroment to around python3, pyspark and java home
I also updated the packages and installed python3 with pyspark 3.2.4 and delta-spark 1.2.1
I also created a workdir called app in the container

I copied the python files, input and output folders into the /app
the entry point was spark-submit


I used "winpty docker build -t codetask ." to build the docker
I used "winpty docker run -it codetask" to run the docker file

I set up the docker-compose to use two services, the code-task image created and the spark-history-server image, the spark-history-server stores it's log in the logs folder.

I used docker-compose up to run the docker-compose file

3.

                             +-------------------+
                             |                   |
                             |  S3 Bucket        |
                             |                   |
                             +--------+----------+
                                      |
                                      |
                                      v
                             +--------+----------+
                             |                   |
                             |  Glue Catalog     |
                             |                   |
                             +--------+----------+
                                      |
                                      |
                                      v
        +----------------+      +---+---+         +---------------------+
        |                |      |       |         |                     |
        |    AWS Glue     +<-----+  EC2  +-------->+    DeltaLake table   |
        |                |      |       |         |                     |
        +--------+-------+      +---+---+         +---------------------+
                 |                   |
                 |                   |
                 v                   v
        +--------+------+   +--------+------+
        |               |   |               |
        |    S3 Bucket  |   | Spark Cluster |
        |               |   |               |
        +---------------+   +---------------+
This diagram shows how you can deploy the Spark job on AWS using S3 as the data source, AWS Glue as the metadata catalog, and DeltaLake









