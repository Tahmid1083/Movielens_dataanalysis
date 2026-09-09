-- Run in MapReduce mode so STORE writes to HDFS (not local disk)
SET fs.defaultFS 'hdfs://localhost:9000';
SET pig.job.submitted.timestamp 'true';
SET mapreduce.job.counters.limit 120;
SET mapreduce.jobhistory.address '0.0.0.0:1';
SET ipc.client.connect.max.retries '0';

-- Load data as raw chararray to safely filter headers
users_raw = LOAD 'hdfs://localhost:9000/movielens/input/users.csv' USING PigStorage(',')
    AS (userId:chararray, gender:chararray, age:chararray, occupation:chararray, zip:chararray);
ratings_raw = LOAD 'hdfs://localhost:9000/movielens/input/ratings.csv' USING PigStorage(',')
    AS (userId:chararray, movieId:chararray, rating:chararray, timestamp:chararray);

users_filtered = FILTER users_raw BY userId != 'userId' AND userId IS NOT NULL;
ratings_filtered = FILTER ratings_raw BY userId != 'userId' AND userId IS NOT NULL;

users = FOREACH users_filtered GENERATE
    (int)userId AS userId,
    gender AS gender,
    (int)age AS age,
    (int)occupation AS occupation,
    zip AS zip;

ratings = FOREACH ratings_filtered GENERATE
    (int)userId AS userId,
    (int)movieId AS movieId,
    (float)rating AS rating;

-- Join ratings with user demographic info
user_ratings = JOIN ratings BY userId, users BY userId;

-- Group by Gender and calculate average rating given
grp_gender = GROUP user_ratings BY users::gender;
gender_stats = FOREACH grp_gender GENERATE
    group AS gender,
    AVG(user_ratings.ratings::rating) AS avg_given_rating,
    COUNT(user_ratings) AS total_ratings;

-- Group by Age group and calculate stats
grp_age = GROUP user_ratings BY users::age;
age_stats = FOREACH grp_age GENERATE
    group AS age_group,
    AVG(user_ratings.ratings::rating) AS avg_given_rating,
    COUNT(user_ratings) AS total_ratings;

STORE gender_stats INTO 'hdfs://localhost:9000/movielens/pig_out2_gender' USING PigStorage(',');
STORE age_stats INTO 'hdfs://localhost:9000/movielens/pig_out2_age' USING PigStorage(',');
