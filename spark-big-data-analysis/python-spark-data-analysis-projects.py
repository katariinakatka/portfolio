# Databricks notebook source
# MAGIC %md
# MAGIC Copyright 2025 Tampere University<br>
# MAGIC This notebook and software was developed for a Tampere University course COMP.CS.320.<br>
# MAGIC This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.<br>
# MAGIC Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, year, when, round, countDistinct, min, udf, lit, avg
from pyspark.sql.types import FloatType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project 1: Video Game Sales Analysis (CSV Data)

# COMMAND ----------


# Load video game sales data from CSV into a DataFrame
videogamesalesDF= spark.read \
    .option("header", "true") \
    .option("delimiter", "|") \
    .option("inferSchema", "true") \
    .csv("abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/sales/video_game_sales_2024.csv") \
    .fillna(0)

# Create a new column for total global sales by summing regional sales
videogamesalesDF=videogamesalesDF.withColumn("global_sales", col("na_sales") + col("jp_sales") + col("pal_sales") + col("other_sales"))

# Filter games released between 2001–2010 and aggregate sales
# Grouping is done by publisher, release year, and console
modifiedDF= videogamesalesDF \
    .filter(col("release_date").between("2001-01-01", "2010-12-31")) \
    .withColumn("release_year", year(col("release_date"))) \
    .withColumn("console", col("console")) \
    .groupBy("publisher", "release_year", "console") \
    .agg(
        sum("jp_sales").alias("total_sales_Japan"),
        sum("global_sales").alias("total_sales_global")
    )
    

# Calculate total Japan sales per publisher and sort descending
salesbypublisherDF=modifiedDF \
    .groupBy("publisher") \
    .agg(sum("total_sales_Japan").alias("total_sales_Japan")) \
    .sort(col("total_sales_Japan").desc()) 


# Identify the publisher with the highest total sales in Japan
bestJapanPublisher = salesbypublisherDF.first()["publisher"]

# For the top publisher, calculate yearly sales: total Japan sales, total global sales, global sales specifically for PS2 games
bestJapanPublisherSales = modifiedDF \
    .filter(modifiedDF.publisher == bestJapanPublisher) \
    .groupBy("release_year") \
    .agg(
        sum(when(col("console") == "PS2", col("total_sales_global")).otherwise(0)).alias("sales_ps2"),
        sum("total_sales_Japan").alias("total_sales_Japan"),
        sum("total_sales_global").alias("total_sales_global")) \
    .sort(col("release_year").asc())


# Rename columns and round numeric values
bestJapanPublisherSales= bestJapanPublisherSales \
    .withColumnRenamed("release_year", "year") \
    .withColumn("total_sales_Japan", round(col("total_sales_Japan"), 2)) \
    .withColumn("total_sales_global", round(col("total_sales_global"), 2)) \
    .withColumn("sales_ps2", round(col("sales_ps2"), 2)) 


# COMMAND ----------

print(f"The publisher with the highest total video game sales in Japan is: '{bestJapanPublisher}'")
print("Sales data for this publisher:")
bestJapanPublisherSales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project 2: Building Density Analysis in Finnish Municipalities (Parquet Data)

# COMMAND ----------

import math

kampusareenaBuildingId: str = "101060573F"

# returns the distance between points (lat1, lon1) and (lat2, lon2) in kilometers
# based on https://community.esri.com/t5/coordinate-reference-systems-blog/distance-on-a-sphere-the-haversine-formula/ba-p/902128
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R: float = 6378.1  # radius of Earth in kilometers
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    deltaPhi = math.radians(lat2 - lat1)
    deltaLambda = math.radians(lon2 - lon1)

    a = (
        math.sin(deltaPhi * deltaPhi / 4.0) +
        math.cos(phi1) * math.cos(phi2) * math.sin(deltaLambda * deltaLambda / 4.0)
    )

    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# COMMAND ----------

haversineUDF=udf(haversine, FloatType())

task2DF=spark.read.parquet("abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/buildings")

kampusareenaDF=task2DF.filter(task2DF.building_id == kampusareenaBuildingId)
lat=kampusareenaDF.select("latitude_wgs84").collect()[0][0]
long=kampusareenaDF.select("longitude_wgs84").collect()[0][0]

# Count the number of different postal codes per municipality
areasDF= task2DF \
    .groupBy("municipality") \
    .agg(countDistinct("postal_code").alias("areas"))

# Count the number of different streets per municipality
streetsDF= task2DF \
    .groupBy("municipality") \
    .agg(countDistinct("street").alias("streets"))

# Count the number of different buildings per municipality
buildingsDF= task2DF \
    .groupBy("municipality") \
    .agg(countDistinct("building_id").alias("buildings"))

# Calculate the minimum distance between the Kampusareena building and all other buildings
# Selectthe minimum distance per municipality
mindistanceDF= task2DF \
    .withColumn("distance", round(haversineUDF(lit(lat), lit(long), col("latitude_wgs84"), col("longitude_wgs84")), 1)) \
    .groupBy("municipality") \
    .agg(min("distance").alias("mindistance"))

# Calculate the buldings per area ratio, combining the results in one DataFrame and sorting them 
municipalityDF = areasDF \
    .join(streetsDF, "municipality") \
    .join(buildingsDF, "municipality") \
    .withColumn("buildings_per_area", round(col("buildings") / col("areas"), 1)) \
    .join(mindistanceDF, "municipality") \
    .sort(col("buildings_per_area").desc())
  

# COMMAND ----------

print("The 10 municipalities with the highest buildings per area (postal code) ratio:")
municipalityDF.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project 3: Closest Building to Average Location (Parquet data from project 2)

# COMMAND ----------

# Select the buildings in Tampere
tampere_buildingsDF= task2DF.filter(task2DF.municipality == "Tampere").dropDuplicates(["building_id"]) 

# Calculate the average latitude and longitude for the buildings in Tampere
tampere_avg_lat=tampere_buildingsDF.select(avg("latitude_wgs84")).first()[0]
tampere_avg_long=tampere_buildingsDF.select(avg("longitude_wgs84")).first()[0]

# Select the buildings in Hervanta
hervanta_buildingsDF= tampere_buildingsDF.filter(task2DF.postal_code == "33720")

# Calculate the average latitude and longitude for the buildings in Hervanta
hervanta_avg_lat=hervanta_buildingsDF.select(avg("latitude_wgs84")).first()[0]
hervanta_avg_long=hervanta_buildingsDF.select(avg("longitude_wgs84")).first()[0]

# Calculate the distances between the buildings and the average location
tampere_buildingsDF = tampere_buildingsDF \
    .withColumn("distance", haversineUDF(lit(tampere_avg_lat), lit(tampere_avg_long), col("latitude_wgs84"), col("longitude_wgs84"))) \
    .sort(col("distance").asc())

hervanta_buildingsDF = hervanta_buildingsDF \
    .withColumn("distance", haversineUDF(lit(hervanta_avg_lat), lit(hervanta_avg_long), col("latitude_wgs84"), col("longitude_wgs84"))) \
    .sort(col("distance").asc())
     
# Select the first building and the first distance in the sorted list and formulate the result
TampereList = tampere_buildingsDF.select("street", "house_number").first()[0:]
tampereAddress = f"{TampereList[0]} {TampereList[1]}"

tampereDistance = tampere_buildingsDF.select(col("distance").alias("distance")).first()[0]
tampereDistance = f"{tampereDistance:.3f}"

HervantaList= hervanta_buildingsDF.select("street", "house_number").first()[0:]
hervantaAddress= f"{HervantaList[0]} {HervantaList[1]}"

hervantaDistance=hervanta_buildingsDF.select(round("distance", 3)).first()[0]
hervantaDistance = f"{hervantaDistance:.3f}" 


# COMMAND ----------

print(f"The address closest to the average location in Tampere: '{tampereAddress}' at ({tampereDistance} km)")
print(f"The address closest to the average location in Hervanta: '{hervantaAddress}' at ({hervantaDistance} km)")