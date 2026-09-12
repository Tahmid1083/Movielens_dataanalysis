# MovieLens Data Analysis

A Hadoop-based data analysis project using the **MovieLens 1M dataset**.

The project uses **Java MapReduce** and **Apache Pig** to analyze movie ratings, genres, users, and rating trends.

## Dataset

| File          |   Records | Description                  |
| ------------- | --------: | ---------------------------- |
| `movies.csv`  |     3,883 | Movie information and genres |
| `ratings.csv` | 1,000,209 | User movie ratings           |
| `users.csv`   |     6,040 | User demographic information |

## MapReduce Programs

* **AverageRating.java** — Calculates average movie ratings.
* **GenreCounter.java** — Counts movie genres.
* **HighRatingFilter.java** — Filters ratings of 4.0 or higher.
* **UserRatingJoin.java** — Combines ratings with user information.

## Apache Pig

* **top_movies.pig** — Finds top-rated movies.
* **genre_breakdown.pig** — Analyzes movie genres.
* **demographics_analysis.pig** — Analyzes ratings by gender and age.
* **rating_trends.pig** — Analyzes rating trends over the years.
* **user_behavior.pig** — Analyzes user rating behavior.

## Technologies

* Java
* Apache Hadoop
* MapReduce
* Apache Pig
* HDFS
* MovieLens 1M Dataset
Datasetlink:https://grouplens.org/datasets/movielens/1m/
