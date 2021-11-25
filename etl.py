'''
Run this python scipt 'etl.py' by writing 'python etl.py' command in the terminal.
Enter AWS Access Key and Secret key in 'de.cfg' file.
'''


"""
   Discription: Importing important modules and functions.
   ------------
"""



import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession,functions as F
from pyspark.sql.functions import udf, col,upper
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp
from pyspark.sql.types import DateType

config = configparser.ConfigParser()
config.read('de.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

#AWS_ACCESS_KEY_ID=config['AWS']['AWS_ACCESS_KEY_ID']

#AWS_SECRET_ACCESS_KEY=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    

    """
            Discription: Function to start a spark session which will be use to process data and read or write data to S3.
    """
    
    spark = SparkSession.builder \
    .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
    .enableHiveSupport().getOrCreate()
    
    return spark


def process_data(spark, i94_input_data, output_data):
    
    

    """
            Discription: - This function will process i94 immigration data by reading files from the source and dividing data 
            into a dimension table by choosing important features: \
            - Immigration \
            - Also, this dimension table will be loaded in S3 and stored in parquet format under its respective directory name.
    """

    
    
    # read i94 immigration data file
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(i94_input_data)
    df_spark = df_spark.limit(100000)
    
    # Selecting desired columns for i94 immigration dataset.
    df =df_spark.select('cicid','gender','visatype','airline','biryear','i94yr','i94mon','i94addr','i94mode','i94visa','i94port')
    
    # Changing datatypes.
    df1 = df.withColumn('Cityid',col('cicid').cast('bigint')) \
        .withColumn('i94destination_city',col('i94port').cast('string')) \
        .withColumn('Gender',col('gender').cast('string')) \
        .withColumn('Visatype',col('visatype').cast('string')) \
        .withColumn('Airline',col('airline').cast('string')) \
        .withColumn('Birthyear',col('biryear').cast('int')) \
        .withColumn('i94year',col('i94yr').cast('bigint')) \
        .withColumn('i94month',col('i94mon').cast('int')) \
        .withColumn('i94address',col('i94addr').cast('string')) \
        .withColumn('i94mode',col('i94mode').cast('int')) \
        .withColumn('Purpose_of_visit',col('i94visa').cast('int'))
    
    #Creating a list of dictionary containing i94 port codes, cities and state code. This list will be use to join and filter \
    #other datasets.
    
    valid_port = {}
    list_of_dicts = []
    with open('I94_SAS_Labels_Descriptions.txt') as file:
        for line in file:
            line = line.strip()
            key, val = line.split('=')
            val_list= val.split(',')
            #print(val_list)
            valid_port[key] = val_list
            valid_port_dict = {}
            valid_port_dict['i94port'] = key
            valid_port_dict['portcity'] = val_list[0]
            valid_port_dict['portstate'] = val_list[1].strip()
            list_of_dicts.append(valid_port_dict)
            
    info = spark.createDataFrame(list_of_dicts)
    info.show(10)
    
    # Dropping non-relevant columns
    df1 = df1.drop('cicid','biryear','i94yr','i94mon','i94addr','i94port','i94visa')
    
    #Removing Null Values.
    df2 = df1.dropna(how='any')
    
    #Removing Duplicates
    df3 = df2.dropDuplicates(['Cityid'])
    
    # Filter data on cities
   # cond=[df3.i94destination_city==info.i94port,df3.i94address==info.portstate]
    final_immig = df3.filter(df3.i94destination_city.isin(list(valid_port.keys())))
    
    final_immig.show(10)
    
    # Creating 'immigration' table.
    immigration = final_immig.select('i94destination_city','i94year','i94address','Cityid',\
                                     'i94mode','Purpose_of_visit','Visatype','Airline','i94month')
    
    # Loading 'immigration' table to S3.
    immigration.write.parquet(output_data+'immigration/i94_immigration.parquet', \
                              partitionBy=['i94year','i94month'],mode='overwrite')

    
    
    
    '''
            Discription: - Now we will process "GlobalLandTemperatureByCity" dataset by reading files from the source and dividing 
            data into a dimension table: \
                
            - Population \
            - Also, this tables will be loaded in S3 and stored in parquet format under its respective directory name.
    '''
    
    
    
    #Reading data.
    temp_df = spark.read.csv('../../data2/GlobalLandTemperaturesByCity.csv',header=True)
    
    #Changing data types.
    temp_df = temp_df.limit(50000)
    temp_df2 = temp_df.withColumn('AverageTemperature',col('AverageTemperature').cast('float')) \
                      .withColumn('AverageTemperatureUncertainty',col('AverageTemperatureUncertainty').cast('float')) \
                      .withColumn('date',col('dt').cast(DateType())).drop(col('dt'))
    
    #Dropping duplicates
    temp_df2=temp_df2.dropDuplicates()
    
    # Dropping 'null' values containing rows.
    temp_df2 = temp_df2.dropna()
    
    # Calculating mean of average temperature over a period of time grouping by country and city.
    group_df = temp_df2.groupBy(['Country','City']).mean()
    
    #Renaming column name
    group_df2 = group_df.withColumnRenamed('avg(AverageTemperature)','Avg_Temperature')
    
    # Joining 'temp_df2' data with list_of_dict by creating a list_of_dict dataframe 
    temp_df4 = group_df2.select("*", upper(col('City')).alias('City1')).drop('City').withColumnRenamed('City1','City')
    
    #Joining temp_df4 data with df on feature 'portcity'
    final_temp = temp_df4.join(info, temp_df4.City==info.portcity)
    
    # Creating 'temp' dimension table.
    temp = final_temp.select('Avg_Temperature','i94port','Country','City','portstate')
    
    # Loading 'temp' table to S3.
    temp.write.parquet(output_data+'temp/global_temp.parquet',mode='overwrite')
    
    #Creating facts table
  #  cond4=[immigration.i94address==temp.portstate]
    
   # final_facts = immigration.join(temp,cond4)
    immigration.createOrReplaceTempView("immi")
    temp.createOrReplaceTempView("tem")
    final_immig.createOrReplaceTempView('fi')
    
    final_facts = spark.sql('''
                SELECT 
                immi.Cityid,
                immi.i94destination_city,
                immi.i94year,
                immi.i94address,
                immi.i94mode,
                immi.Visatype,
                immi.Purpose_of_visit,
                immi.Airline,
                tem.Avg_Temperature,
                tem.City
                FROM immi
                JOIN tem ON (immi.i94address==tem.portstate
                AND immi.i94destination_city==tem.i94port)
                ''')
    
    final_facts.show(10)
    
    # Load facts table to S3.
    final_facts.write.parquet(output_data+'facts/i94_temp.parquet',mode='overwrite')
    
    
# Reading immigration table from S3.   
def read_immigration_dim(spark):
    
    df_loc = 's3a://batman1/immigration/i94_immigration.parquet'
    
    read_immigration = spark.read.format('parquet').load(df_loc)
    
    return read_immigration


# Reading temp table from S3.   
def read_temperature_dim(spark):
    
    df_loc = 's3a://batman1/temp/global_temp.parquet'
    
    read_temp = spark.read.format('parquet').load(df_loc)
        
    return read_temp


# Testing final data model by running a query.    
def analyzing_result(spark):
    
    immigration = read_immigration_dim(spark)
    temp = read_temperature_dim(spark)
    
    immigration.createOrReplaceTempView("immi")
    temp.createOrReplaceTempView("tem")
    
    A1 = spark.sql('''
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
    
    A1.show()
    

# Running two quality checks.
def quality_check(spark,output_data,obj):
    
    df = output_data+ obj

    read_df = spark.read.format('parquet').load(df)

    # Checking for records.
    num = read_df.count()

    if num > 0:
        print(f"Quality check PASSED with record count:{num}")
    else:
        print(f"Quality check FAILED with record count:{num}")
        
    # Checking for duplicate rows.
    dup = read_df.groupBy(read_df.columns)\
    .count()\
    .where(F.col('count') > 1)\
    .select(F.sum('count').alias('Count'))
    
    dup.show()
    count = dup.count()
    
    if count > 1:
        print('Duplicate rows are present in the data')
    else:
        print('No duplicate rows are present in the data')
    

def main():
    spark = create_spark_session()
    i94_inputs = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    output_data = 's3a://batman1/'
    process_data(spark,i94_inputs,output_data)
    quality_check(spark,output_data,'immigration/i94_immigration.parquet/*/*/*.parquet')
   # quality_check(spark,output_data,'alien/aliens.parquet/*/*.parquet')
    quality_check(spark,output_data,'temp/global_temp.parquet/*.parquet')
   # quality_check(spark,output_data,'location/loc.parquet/*/*/*.parquet')
    quality_check(spark,output_data,'facts/i94_temp.parquet/*.parquet')
    read_immigration_dim(spark)
    read_temperature_dim(spark)
    analyzing_result(spark)
    
    
    
if __name__ == "__main__":
    main()
    

