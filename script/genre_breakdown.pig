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

movies_filtered = FILTER movies_raw BY movieId != 'movieId' AND movieId IS NOT NULL;
ratings_filtered = FILTER ratings_raw BY userId != 'userId' AND userId IS NOT NULL;

movies = FOREACH movies_filtered GENERATE
    (int)movieId AS movieId,
    title AS title,
    genres AS genres;

ratings = FOREACH ratings_filtered GENERATE
    (int)userId AS userId,
    (int)movieId AS movieId,
    (float)rating AS rating;

-- Join movies and ratings
movie_ratings = JOIN ratings BY movieId, movies BY movieId;

-- Flatten genres (split by '|')
genre_rows = FOREACH movie_ratings GENERATE
    ratings::rating AS rating,
    FLATTEN(TOKENIZE(movies::genres, '|')) AS genre;

-- Group by individual Genre
grp_genre = GROUP genre_rows BY genre;
genre_summary = FOREACH grp_genre GENERATE
    group AS genre,
    COUNT(genre_rows) AS rating_count,
    AVG(genre_rows.rating) AS avg_rating;

-- Order by overall rating volume (nested ORDER avoids TotalOrderPartitioner)
all_genres = GROUP genre_summary ALL;
sorted_genres = FOREACH all_genres {
    ordered = ORDER genre_summary BY rating_count DESC;
    GENERATE FLATTEN(ordered);
};

-- Persist to HDFS (DUMP only prints to stdout and does not write HDFS)
STORE sorted_genres INTO 'hdfs://localhost:9000/movielens/pig_out3' USING PigStorage(',');
