# Text Analysis with Apache Spark (Wikipedia XML Data)

This project performs large-scale text analysis on a collection of long
Wikipedia articles using Apache Spark. The articles were extracted in XML
format using Wikipedia’s official export tools and processed using Spark’s
XML data source.


## Dataset

- Source: Wikipedia (Special:LongPages)
- Format: XML
- Content: Full text of long Wikipedia articles

Each article consists of multiple lines separated by newline characters,
with words separated by whitespace.


## Word Definition and Filtering Rules

A word is included in the analysis if:
- Punctuation characters are removed: `. , ; : ! ? ( ) [ ] { }`
- All letters are converted to lowercase
- The word contains only English alphabet characters (`a–z`)
- The word is at least 5 characters long
- The word is not a month name (e.g. january, february, …)
- The word is not a season name (summer, autumn, winter, spring)


## Analysis Tasks

### 1. Most Frequent Words Across All Articles
- Identified the 10 most frequent words across the entire dataset
- Output produced as a DataFrame with:
  - `word`
  - `total_count`


### 2. Articles with Frequent Occurrence of a Specific Word
- Identified all article titles where the word **"software"**
  appears more than 5 times
- Output produced as a list of article titles


### 3. Longest Common Words Across Articles
- Identified the 10 longest words that appear in:
  - at least 10%
  - at least 25%
  - at least 50%
  - at least 75%
  - at least 90% of the articles
- Words with equal length are ranked alphabetically
- Output produced as a DataFrame with columns:
  - `rank`
  - `word_10`
  - `word_25`
  - `word_50`
  - `word_75`
  - `word_90`


### 4. Per-Article Word Frequency Analysis
- Analyzed articles last updated before October 2025
- For each article:
  - extracted title and last update date
  - calculated total character count
  - identified the 5 most frequent words
- Output produced as a DataFrame with columns:
  - `title`
  - `date`
  - `characters`
  - `word_1`, `word_2`, `word_3`, `word_4`, `word_5`


## Technologies Used
- Apache Spark
- Spark SQL and DataFrames
- Python
- XML data sources
- Databricks


## Key Learnings
- Processing semi-structured XML data 
- Designing robust text-cleaning and filtering pipelines
- Combining DataFrame-based analytics with distributed processing concepts
- Translating complex analytical requirements into reproducible Spark workflows
