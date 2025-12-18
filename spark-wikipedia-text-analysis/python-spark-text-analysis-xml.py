# Databricks notebook source
# MAGIC %md
# MAGIC Copyright 2025 Tampere University<br>
# MAGIC This notebook and software was developed for a Tampere University course COMP.CS.320.<br>
# MAGIC This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.<br>
# MAGIC Author(s): Ville Heikkilä \([ville.heikkila@tuni.fi](mailto:ville.heikkila@tuni.fi))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Text Analysis with Wikipedia XML Data

# COMMAND ----------

# imports

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, split, regexp_replace,lower, length, udf, length, count, countDistinct, row_number, sum, first, to_date
from pyspark.sql.window import Window

# COMMAND ----------

# reading the xml file into a DataFrame and selecting the necessary columns

xmlFile = "abfss://shared@tunics320f2025gen2.dfs.core.windows.net/assignment/wikipedia/*.xml"

df = spark.read.option("rowTag", "page").format("xml").load(xmlFile)

df = df.select("title", "revision.timestamp", "revision.text._VALUE")

# splitting the text column into lines, removing punctuation and converting to lowercase

articlesdf = df.withColumn("lines", explode(split(col("_VALUE"), "\n"))).drop("_VALUE")

articlesdf = articlesdf.withColumn("text_no_punctuation", regexp_replace(col("lines"), r"[.,;:!?()\[\]{}]", "")).drop("lines")

articlesdf = articlesdf.withColumn("text_no_punctuation_lower", lower(col("text_no_punctuation"))).drop("text_no_punctuation")

# splitting the text column into words, removing words that are not letters, removing words that are in the exclude_words list

wordsdf = articlesdf.withColumn("words", explode(split(col("text_no_punctuation_lower"), " "))).drop("text_no_punctuation_lower")

exclude_words=["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december", "summer", "spring", "winter", "autumn"]

filteredwordsdf = wordsdf.filter(
    (col("words").rlike(r"^[a-z]+$")) &
    (~col("words").isin(exclude_words)) &
    (length(col("words")) >= 5)
)

# selecting the top 10 most frequent words

tenMostFrequentWordsDF=filteredwordsdf.groupBy("words").count().orderBy(col("count").desc()).limit(10)


# COMMAND ----------

print("Top 10 most frequent words across all articles:")
tenMostFrequentWordsDF.show(truncate=False)


# COMMAND ----------

softwaredf=filteredwordsdf.filter(col("words") == "software").groupBy("title").count().filter(col("count") > 5).orderBy(col("title"))

softwareArticles = [title for title, count in softwaredf.collect()]

# COMMAND ----------

print("Articles in alphabetical order where the word 'software' appears more than 5 times:")
for title in softwareArticles:
    print(f" - {title}")

# COMMAND ----------

# the total number of articles
total_article=articlesdf.select("title").distinct().count()
print(total_article)

# calculating the length of words and counting how many times each word appears in the articles
wordlengthsDF = filteredwordsdf \
    .withColumn("title", col("title")) \
    .groupBy("words") \
    .agg(
        length(col("words")).alias("length"),
        countDistinct(col("title")).alias("count")) 
    
# calculating the percentage of articles each word appears in
wordlengthsDF=wordlengthsDF.withColumn("percent", (col("count") / total_article * 100)).drop("count")

# selecting the top10 longest words that appear in at least 10% of the articles, adding ranking and dropping unnecessary columns
longestWords10DF = wordlengthsDF \
    .filter(col("percent") >= 10) \
    .orderBy(col("length").desc(), col("words")) \
    .withColumnRenamed("words", "word_10") \
    .limit(10) \
    .withColumn("rank", row_number().over(Window.orderBy(col("length").desc()))) \
    .drop("count") \
    .drop("length") \
    .drop("percent")

# below, repeating the process for the other percentages
longestWords25DF = wordlengthsDF \
    .filter(col("percent") >= 25) \
    .orderBy(col("length").desc(), col("words")) \
    .withColumnRenamed("words", "word_25") \
    .limit(10) \
    .withColumn("rank", row_number().over(Window.orderBy(col("length").desc()))) \
    .drop("count") \
    .drop("length") \
    .drop("percent")

longestWords50DF = wordlengthsDF \
    .filter(col("percent") >= 50) \
    .orderBy(col("length").desc(), col("words")) \
    .withColumnRenamed("words", "word_50") \
    .limit(10) \
    .withColumn("rank", row_number().over(Window.orderBy(col("length").desc()))) \
    .drop("count") \
    .drop("length") \
    .drop("percent")


longestWords75DF = wordlengthsDF \
    .filter(col("percent") >= 75) \
    .orderBy(col("length").desc(), col("words")) \
    .withColumnRenamed("words", "word_75") \
    .limit(10)  \
    .withColumn("rank", row_number().over(Window.orderBy(col("length").desc()))) \
    .drop("count") \
    .drop("length") \
    .drop("percent") 


longestWords90DF = wordlengthsDF \
    .filter(col("percent") >= 90) \
    .orderBy(col("length").desc(), col("words")) \
    .withColumnRenamed("words", "word_90") \
    .limit(10)  \
    .withColumn("rank", row_number().over(Window.orderBy(col("length").desc()))) \
    .drop("count") \
    .drop("length") \
    .drop("percent")

# joining the dataframes
longestWordsDF = longestWords10DF \
    .join(longestWords25DF, "rank", "outer") \
    .join(longestWords50DF, "rank", "outer") \
    .join(longestWords75DF, "rank", "outer") \
    .join(longestWords90DF, "rank", "outer") 


# COMMAND ----------

print("The longest words appearing in at least 10%, 25%, 50%, 75, and 90% of the articles:")
longestWordsDF.show(truncate=False)

# COMMAND ----------

# counting characters per article
charactersDF = df.withColumn("timestamp", col("timestamp")).filter(col("timestamp") < "2025-10-01").groupBy("title").agg(sum(length(col("_VALUE"))).alias("characters"))

# filtering articles/words by date, counting how many times the words appear the articles
WordsByArticleDF = filteredwordsdf.filter(col("timestamp") < "2025-10-01").withColumn("date", col("timestamp")).groupBy("title", "words", "timestamp").agg(count("words").alias("count"))

# splitting the rows into partitions based on titles
w = Window.partitionBy("title").orderBy(col("count").desc())

# adding the row numbers 1-5
WordsByArticleDF = WordsByArticleDF.withColumn("rank", row_number().over(w)).filter(col("rank") <= 5)

# forming the result by converting "rank" -rows into columns and joining data frames
frequentWordsDF = WordsByArticleDF.withColumn("date", to_date(col("timestamp"))).drop("timestamp").groupBy("title", "date").pivot("rank").agg(first("words")).join(charactersDF, "title", "inner").orderBy("date")

frequentWordsDF=frequentWordsDF.withColumn("title", col("title")).withColumn("date", col("date")).withColumn("characters", col("characters")).withColumn("word_1", col("1")).withColumn("word_2", col("2")).withColumn("word_3", col("3")).withColumn("word_4", col("4")).withColumn("word_5", col("5")).drop("1").drop("2").drop("3").drop("4").drop("5")

# COMMAND ----------

print("Top 5 most frequent words per article (excluding forbidden words) in articles last updated before October 2025:")
frequentWordsDF.show(truncate=False)