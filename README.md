# Big Data Assessed Exercise: Financial Asset Recommender with Apache Spark
> COMPSCI5088 Big Data: Systems, Programming and Management M - 2024-25

This project is a batch-processing Spark application that ranks financial assets based on past performance using large-scale historical price and metadata. It was developed as part of the Big Data coursework and received **24/25 marks**, demonstrating both correctness and scalability.

Task Sheet: [Assessed-Exercise.pdf](/Assessed-Exercise.pdf)

## Project Summary

The goal of this Spark-based Java application is to recommend the **top 5 financial assets** for investment by computing **technical indicators** such as returns and volatility from historical stock data. The data is processed using a pipeline of distributed transformations and actions, optimized for performance and minimal shuffling.

## Technologies & Concepts

* Apache Spark (4.0.0-preview2)
* Java 21
* RDD and Dataset APIs
* Broadcast Variables
* Distributed Filtering, Mapping, Grouping, and Sorting
* Functional decomposition using custom transformation classes.

## Pipeline

Driver Code: [AssessedExercise.java](/src/bigdata/app/AssessedExercise.java#L135)

1. Loading large-scale CSV and JSON datasets using Spark's SQL API.
2. Filtering asset metadata based on P/E ratio and data completeness.
3. Time-range filtering of stock price data (past 1 year from a reference date).
4. Computing returns and volatility per asset using a 5-day and 251-day window, respectively.
5. Filtering high-volatility stocks and broadcasting necessary metadata.
6. Ranking assets by returns and returning the top 5.

## Performance Highlights

Feedback: [Assessed-Exercise-Feedback.pdf](/Assessed-Exercise-Feedback.pdf)

* Correct use of solution components like broadcast variables and distributed sorting (10/10)
* Full marks on code correctness and output (5/5)
* Efficient use of Spark (4/5 for performance; 33s vs 30s benchmark for Task Time)
* Clean, well-documented, and scalable code (5/5)
