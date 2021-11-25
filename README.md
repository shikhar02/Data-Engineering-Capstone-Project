Project Title- I94 Immigration & Global Temperature
========================================================

Project Summary
========================

In this project, I used Udacity provided projects which were i94 immigration data and GlobalLandTemperaturesByCity data. I 
created a data lake to store large data files which comes in various formats in a single bucket in S3. During the process,two  dimension and one fact tables were created by processing two datasets using an ETL pipeline. The main purpose was to process structured and semi-structured data which comes in various formats and make it avaliable at one place in a single file format for the future analysis using Redshift or other databases. I used Pyspark which is basically use to build spark applications in python as well as in SQL.  

With the final data model following questions can be answered:

- Which airline was taken most number of times for immigration?
- To which city people immigrated maximum number of time?
- What was the average change in temperature of the top five immigrated cities?
- What type of visa was issued maximum number of times and to which city?


__The project follows the following steps:__

* Step 1: Scoping the Project and Gathering Data \
* Step 2: Exploring and Assessing the Data \
* Step 3: Defining the Data Model \
* Step 4: Running ETL to Model the Data \
* Step 5: Completing the Project Write Up \

Step 1: Scoping the Project and Gathering Data
==========================================

- Scope

The i94 immigration data is obtained from International Trade Administration U.S. Department of Commerce. The I-94 provides a count of visitor arrivals to the United States (with stays of 1-night or more and visiting under certain visa types) to calculate U.S. travel and tourism volume exports. There are 3096313 records in the dataset with 28 features. While the other dataset, GlobalLandTemperaturesByCity, was collected from [KAGGLE](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data?select=GlobalLandTemperaturesByCity.csv). This dataset has 7 features and 8 million plus rows.

The end solution will be a data lake in S3 consisting of three tables: \

- Immigration Table \

- Temp Table \

- Facts Table \

Some features in i94 immigration were :
i94yr, i94mon, i94cit, i94res, i94port, arrdate, i94mode, i94addr, depdate.

Seven features in the global temperature data were:
 dt, AverageTemperature, AverageTemperatureUncertainty, City, Country, Latitude, Longitude.
 
Step 2: Exploring and Assessing the Data
=====================================         
                
- I94_immigration data contains 3 million+ rows whereas global temperature data contains 8 million+ rows.

- Both datasets will be filtered or joined on the basis of 'I94_SAS_Labels_Descriptions' text file. This file \
  basically contains i94 port codes for their city names and countries name in which cities are present.

Cleaning Steps
---------------
                
__Steps necessary to clean the data:__ \
                
- Removing duplicates. \
- Removing null values. \
- Changing datatypes. \
- Renaming Column names. \
- Changing column values from lower to upper. \
- Dropping not-so-important features. \
- Filtering data. \

Step 3: Define the Data Model
=======    
     
__3.1__ Conceptual Data Model
     
Mapping out the conceptual data model:
        
- The data model will comprise of one fact and 2 dimension tables.
- Some data quality issues will be resoved.
- Main purpose of this data model is to create a data lake in S3 by extracting and transforming data from different sources \
  avaliable in various file formats like sas, csv, txt. This would help analyst to query data to a single data source which \
  is S3 in my case and load data in redshift for further analysis in less time.


__3.2__ Mapping Out Data Pipelines
     
Listing the steps necessary to pipeline the data into the chosen data model \
        
- Load data from the source.
- Change datatypes of those features which are assigned incorrect datatypes.
- Remove duplicates, null values and filter data.
- Create immigration and temperature dimension tables by choosing relevant features.
- Create one facts table by joining two datasets on filtered i94destination city and i94port column"


Step 4: Run Pipelines to Model the Data
=======    
     
__4.1__ ___Create a data model____

Please, check ___etl.py___ python script.

__4.2__ ___Data Quality Checks___

- Data quality checks have been made to check the availability of records and duplicates in the datasets. \
- If number of rows in the dataset are greater than 1, then quality check is pass else failed. \
- If number of duplicate rows are greater then one then check failed else pass. \
- Also, this check will be done on the dimension and facts table by reading them from S3 bucket. Hence, by doing this we will \
  get the evidence that our data model is prepared according to our initial conceptual data model.

__4.3__ ___Data dictionary___ 
    
- i94 immigration data: Us National Tourism and Trade Office (SAS7BDAT format) \
        
  1- cicid = city id \
  2- i94yr = 4 digit year \
  3- i94mon = numeric month \
  4- i94port = 3 character code of destination USA city (This feature will be use to filter out data based on city codes in \
     i94 description file) \
  5- i94mode = 1 digit travel code (1=Air, 2=Sea, 3=Land, 9=Not Reported) \
  6- i94visa = reason for immigration (1=Business, 2=Pleasure, 3=Student) \
  7- Gender = Male Or Female \
  8- VisaType = Class of admission legally admitting the non-immigrant to temporarily stay in U.S. \
  9- airline = Airline used to arrive in U.S. \
  10-biryear = 4 digit year of birth \
  11-i94addr = Immigration State Code (This feature along with portstate feature from description text file will be use to \
               join immigration data with info dataframe) \
        
- Global Temperature data: Kaggle (csv format) \
     
  1- AverageTemperature = average temperature \
  2- dt = Year, Month and Day \
  3- AverageTemperatureUncertainy = average temperature uncertainty \
  4- Country = country name \
  5- City = city name (City feature will be use to filter data on cities list present in i94 description text file. \
  6- Latitude= latitude \
  7- Longitude = longitude
 
__Final Data Model__
 
- Immigration Table: \ 

  1- Cityid = Unique city id \
  2- i94year = 4 digit year \
  3- i94month = numeric month \
  4- i94mode = 1 digit travel code (1=Air, 2=Sea, 3=Land, 9=Not Reported) \
  5- Purpose_of_visit = i94visa; reason for immigration (1=Business, 2=Pleasure, 3=Student) \
  6- Visatype = Class of admission legally admitting the non-immigrant to temporarily stay in U.S. \
  7- Airline = Airline used to arrive in U.S. \
  8- i94destination_city = i94port; 3 character code of destination USA city.
  
- Temp Table: \

  1- Avg_Temperature = Mean value of average temperature over a period of time group by city and country. \
  2- i94port = 3 character code of destination USA city. \
  3- Country = country name. \
  4- City = City name. \
  5- portstate = Two character State Code.
  
- Facts Table: \

  1- immi.Cityid \
  2- immi.i94destination_city \
  3- immi.i94year \
  4- immi.i94address \
  5- immi.i94mode \
  6- immi.Visatype \
  7- immi.Purpose_of_visit \
  8- immi.Airline \
  9- tem.Avg_Temperature \
  10-tem.City
     
STEP-5
=======

__choice of tools and technologies used for this project.__
    
- This project uses PySpark which is mainly used for processing structured and semi-structured datasets. This makes it a \
  perfect use case for data aggregated from different formats. Also, this project uses S3 to store big data in 'parquet' \
  file format for easy and fast reads/writes for analysis purposes. Storing data in parquet file format is better than other \
  file formats because it takes less space due to the columnar storage and also reading/writing the files takes less time and \
  minimizes the latency.\
    
_Data should be updated every month in a given year because of the presence of month and year features in the datasets._
    
    
 * The data was increased by 100x.
 
     - If the data is increased by 100x's we need to reconsider the tooling. We can still use PySpark but we could not run it \
       locally because of the space and speed problems associated with processing large datasets on local machines. In this case,        runnig ETL pieplines and processing data on cluster would probably be a better choice.
       
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 
     - If the data populates a dashboard on a daily basis by 7am, we would need tooling that can handle time sensitive run \
       times and not manual processing. If data is stored in S3 buckets, it can be executed with a number of \
       tools such as Apache Airflow, SQS services, or Lambdas that can automate the ETL process.
       
 * The database needed to be accessed by 100+ people.
 
     - Amazon RDS Read Replicas can be use to create replicas of single database instance forread-heavy database worloads.\
       We can create one or more replicas of a given source DB Instance and serve high-volume application read traffic from \
       multiple copies of our data, thereby increasing the aggregate read throughput.
       Read replicas are available in Amazon RDS for MySQL, MariaDB, PostgreSQL, Oracle, and SQL Server as well as Amazon Aurora.


__Evidence that the ETL has processed the result in the final data model.__

- Analytical Question: Which visatype was issued maximum number of times to which city?

        Query:
        
        spark.sql('''
              SELECT tem.City,
              immi.Visatype,
              COUNT(immi.Visatype) AS Total_visa_issued
              FROM immi
              JOIN tem ON (immi.i94address==tem.portstate
              AND immi.i94destination_city==tem.i94port)
              GROUP BY tem.City, immi.Visatype
              ORDER BY Total_visa_issued DESC
              LIMIT 10
              ''')
              
              
        Result:
        
        
                +-----------+--------+-----------------+
                |       City|Visatype|Total_visa_issued|
                +-----------+--------+-----------------+
                |   NEW YORK|      WT|           171722|
                |LOS ANGELES|      WT|           161924|
                |      MIAMI|      B2|           152818|
                |LOS ANGELES|      B2|           134898|
                |   NEW YORK|      B2|            86132|
                +-----------+--------+-----------------+