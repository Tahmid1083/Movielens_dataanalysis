-- Disable MapReduce JobHistory server checks
SET pig.job.submitted.timestamp 'true';
SET mapreduce.job.counters.limit 120;

-- Load raw data
ratings_raw = LOAD '/movielens/input/ratings.csv' USING PigStorage(',') 
    AS (userId:chararray, movieId:chararray, rating:chararray, timestamp:chararray);

-- Step 1: Filter header & null values
ratings_filtered = FILTER ratings_raw BY userId != 'userId' AND userId IS NOT NULL;

-- Step 2: Convert Unix timestamp to year string and cast ratings
ratings_timed = FOREACH ratings_filtered GENERATE 
    (float)rating AS rating, 
    ToString(ToDate((long)timestamp * 1000L), 'yyyy') AS review_year;

-- Step 3: Group ratings by year
grouped_years = GROUP ratings_timed BY review_year;

-- Step 4: Calculate annual volume and average rating
yearly_stats = FOREACH grouped_years GENERATE 
    group AS review_year, 
    COUNT(ratings_timed) AS total_reviews, 
    ROUND_TO(AVG(ratings_timed.rating), 2) AS avg_yearly_rating;

-- Step 5: Sort chronologically
sorted_years = ORDER yearly_stats BY review_year ASC;

-- Output Results
DUMP sorted_years;
STORE sorted_years INTO '/movielens/pig_out_trends' USING PigStorage(',');