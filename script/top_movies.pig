-- Run in MapReduce mode so STORE writes to HDFS (not local disk)
SET fs.defaultFS 'hdfs://localhost:9000';
SET pig.job.submitted.timestamp 'true';
SET mapreduce.job.counters.limit 120;
SET mapreduce.jobhistory.address '0.0.0.0:1';
SET ipc.client.connect.max.retries '0';

-- Load data as raw chararray to safely filter headers
movies_raw = LOAD 'hdfs://localhost:9000/movielens/input/movies.csv' USING PigStorage(',')
    AS (movieId:chararray, title:chararray, genres:chararray);

ratings_raw = LOAD 'hdfs://localhost:9000/movielens/input/ratings.csv' USING PigStorage(',')
    AS (userId:chararray, movieId:chararray, rating:chararray, timestamp:chararray);

-- Step 1: Filter out the CSV headers
movies_filtered = FILTER movies_raw BY movieId != 'movieId' AND movieId IS NOT NULL;
ratings_filtered = FILTER ratings_raw BY userId != 'userId' AND userId IS NOT NULL;

-- Step 2: Safely cast columns to numeric types
movies = FOREACH movies_filtered GENERATE
    (int)movieId AS movieId,
    title AS title,
    genres AS genres;

ratings = FOREACH ratings_filtered GENERATE
    (int)userId AS userId,
    (int)movieId AS movieId,
    (float)rating AS rating;

-- Step 3: Group & Calculate Averages
grp_ratings = GROUP ratings BY movieId;
avg_ratings = FOREACH grp_ratings GENERATE
    group AS movieId,
    AVG(ratings.rating) AS avg_rating,
    COUNT(ratings) AS num_ratings;

-- Filter for popular movies (e.g., > 5 reviews)
popular_movies = FILTER avg_ratings BY num_ratings > 5;

-- Step 4: Join with Movies
joined_data = JOIN popular_movies BY movieId, movies BY movieId;

projected_data = FOREACH joined_data GENERATE
    movies::title AS title,
    popular_movies::avg_rating AS avg_rating,
    popular_movies::num_ratings AS total_votes;

-- Step 5: Top 10 via nested ORDER (avoids TotalOrderPartitioner sampler, which fails on this cluster)
all_movies = GROUP projected_data ALL;
top10 = FOREACH all_movies {
    sorted_movies = ORDER projected_data BY avg_rating DESC;
    limited = LIMIT sorted_movies 10;
    GENERATE FLATTEN(limited);
};

-- Persist to HDFS (DUMP only prints to stdout and does not write HDFS)
STORE top10 INTO 'hdfs://localhost:9000/movielens/pig_out1' USING PigStorage(',');
