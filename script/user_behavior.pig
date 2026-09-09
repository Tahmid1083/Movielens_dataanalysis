-- Run in MapReduce mode so STORE writes to HDFS (not local disk)
SET fs.defaultFS 'hdfs://localhost:9000';
SET pig.job.submitted.timestamp 'true';
SET mapreduce.job.counters.limit 120;
SET mapreduce.jobhistory.address '0.0.0.0:1';
SET ipc.client.connect.max.retries '0';

-- Load raw data directly from HDFS
ratings_raw = LOAD 'hdfs://localhost:9000/movielens/input/ratings.csv' USING PigStorage(',') 
    AS (userId:chararray, movieId:chararray, rating:chararray, timestamp:chararray);

-- Step 1: Filter header & nulls
ratings_filtered = FILTER ratings_raw BY userId != 'userId' AND userId IS NOT NULL;

-- Step 2: Cast user IDs and ratings
ratings = FOREACH ratings_filtered GENERATE 
    (int)userId AS userId, 
    (float)rating AS rating;

-- Step 3: Group by individual user
grouped_users = GROUP ratings BY userId;

-- Step 4: Calculate total reviews and average rating given per user
user_metrics = FOREACH grouped_users GENERATE 
    group AS userId, 
    COUNT(ratings) AS review_count, 
    ROUND_TO(AVG(ratings.rating), 2) AS user_avg_rating;

-- Step 5: Segment users based on review activity count
user_segments = FOREACH user_metrics GENERATE 
    userId, 
    review_count, 
    user_avg_rating, 
    (
        review_count >= 100 ? 'Heavy Reviewer' : 
        (review_count >= 30 ? 'Moderate Reviewer' : 'Casual Reviewer')
    ) AS engagement_tier;

-- Step 6: Group by segment tier to get overall summary stats
grouped_segments = GROUP user_segments BY engagement_tier;

segment_summary = FOREACH grouped_segments GENERATE 
    group AS tier, 
    COUNT(user_segments) AS total_users, 
    ROUND_TO(AVG(user_segments.user_avg_rating), 2) AS tier_avg_rating;

-- STORE directly to HDFS path
STORE segment_summary INTO 'hdfs://localhost:9000/movielens/pig_out_user_segments' USING PigStorage(',');