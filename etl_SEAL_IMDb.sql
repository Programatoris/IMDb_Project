CREATE DATABASE SEAL_IMDb;
--USE DATABASE SEAL_IMDB;

CREATE SCHEMA SEAL_IMDb.staging;

USE SCHEMA SEAL_IMDb.staging;



CREATE OR REPLACE TABLE movie_staging (
    movie_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(256) NOT NULL,
    yearr INT NOT NULL,
    date_published DATE NOT NULL,
    duration INT NOT NULL,
    country VARCHAR(256),
    worldwide_gross_income INT,
    languages VARCHAR(256),
    production_company VARCHAR(256)
);
CREATE OR REPLACE TABLE names_staging (
    name_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(48) NOT NULL,
    height INT,
    date_of_birth DATE,
    known_for_movies STRING
);
CREATE OR REPLACE TABLE ratings_staging (
    movie_id VARCHAR(20) PRIMARY KEY,
    avg_rating DECIMAL(3, 1) NOT NULL,
    total_votes INT NOT NULL,
    median_rating DECIMAL(3, 1) NOT NULL
);
CREATE OR REPLACE TABLE director_mapping_staging (
    movie_id VARCHAR(20) NOT NULL,
    name_id VARCHAR(20) NOT NULL,
    FOREIGN KEY (movie_id) REFERENCES movie_staging(movie_id),
    FOREIGN KEY (name_id) REFERENCES names_staging(name_id),
    PRIMARY KEY (movie_id, name_id)
);
CREATE OR REPLACE TABLE genre_staging (
    genre VARCHAR(50) PRIMARY KEY,
    movie_id VARCHAR(20) NOT NULL,
    FOREIGN KEY (movie_id) REFERENCES movie_staging(movie_id)
);
CREATE OR REPLACE TABLE role_mapping_staging (
    movie_id VARCHAR(20) NOT NULL,
    name_id VARCHAR(20) NOT NULL,
    category VARCHAR(20) NOT NULL,
    FOREIGN KEY (movie_id) REFERENCES movie_staging(movie_id),
    FOREIGN KEY (name_id) REFERENCES names_staging(name_id),
    PRIMARY KEY (movie_id, name_id)
);



CREATE OR REPLACE STAGE SEAL_IMDb_STAGE;
--DROP STAGE SEAL_IMDb_STAGE;

CREATE OR REPLACE FILE FORMAT my_csv_format 
    TYPE = 'CSV' 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FIELD_DELIMITER = ';'
    SKIP_HEADER = 0;

CREATE OR REPLACE FILE FORMAT my_csv_format2 
    TYPE = 'CSV' 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FIELD_DELIMITER = ';'
    SKIP_HEADER = 0
    NULL_IF = ('NULL', 'null');

    
    
COPY INTO movie_staging (movie_id, title, yearr, date_published, duration, country, worldwide_gross_income, languages, production_company)
FROM @SEAL_IMDB_STAGE/moviee.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format2, NULL_IF = ('\\N'));

COPY INTO names_staging (name_id, name, height, date_of_birth, known_for_movies)
FROM @SEAL_IMDB_STAGE/namess.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

COPY INTO ratings_staging (movie_id, avg_rating, total_votes, median_rating)
FROM @SEAL_IMDB_STAGE/ratingss.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

COPY INTO director_mapping_staging (movie_id, name_id)
FROM @SEAL_IMDB_STAGE/director_mappingg.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

COPY INTO genre_staging (movie_id, genre)
FROM @SEAL_IMDB_STAGE/genree.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

COPY INTO role_mapping_staging (movie_id, name_id, category)
FROM @SEAL_IMDB_STAGE/role_mappingg.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format);



CREATE OR REPLACE TABLE DIM_MOVIE AS
SELECT DISTINCT
    m.movie_id AS dim_movie_id,
    m.title,
    m.yearr,
    m.duration,
    m.country,
    m.worldwide_gross_income,
    m.languages,
    m.production_company,
    m.date_published,
    g.genre,
    CASE
        WHEN m.duration BETWEEN 0 AND 30 THEN 'short'
        WHEN m.duration BETWEEN 31 AND 90 THEN 'medium'
        WHEN m.duration > 91 THEN 'long'
        ELSE 'unknown'
    END AS duration_score,
    CASE
        WHEN m.worldwide_gross_income BETWEEN 0 AND 500000 THEN 'poor'
        WHEN m.worldwide_gross_income BETWEEN 500001 AND 5000000 THEN 'good'
        WHEN m.worldwide_gross_income > 5000000 THEN 'great'
        ELSE 'unknown'
    END AS income_score
FROM movie_staging m
JOIN genre_staging g ON m.movie_id = g.movie_id;

CREATE OR REPLACE TABLE DIM_PERSON AS
SELECT DISTINCT
    n.name_id AS dim_person_id,
    n.name,
    n.height,
    n.date_of_birth,
    n.known_for_movies,
    r.category,
    dm.movie_id AS starred_movie_id,
    CASE
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM n.date_of_birth) <= 30 THEN 'young'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM n.date_of_birth) BETWEEN 31 AND 65 THEN 'middle-aged'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM n.date_of_birth) > 65 THEN 'senior'
        ELSE 'unknown'
    END AS age_category
FROM names_staging n
JOIN role_mapping_staging r ON r.name_id = n.name_id
LEFT JOIN director_mapping_staging dm ON dm.name_id = n.name_id;

CREATE OR REPLACE TABLE DIM_DATE AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(m.date_published AS DATE)) AS dim_date_id,
    CAST(m.date_published AS DATE) AS date,
    DATE_PART(day, m.date_published) AS day,
    DATE_PART(dow, m.date_published) + 1 AS day_of_week,
    DATE_PART(month, m.date_published) AS month,
    DATE_PART(year, m.date_published) AS year,
    DATE_PART(week, m.date_published) AS week,
    DATE_PART(quarter, m.date_published) AS quarter,
    CASE DATE_PART(dow, m.date_published) + 1
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_of_week_name,
    CASE DATE_PART(month, m.date_published)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_name
FROM movie_staging m
GROUP BY CAST(m.date_published AS DATE), 
         DATE_PART(day, m.date_published), 
         DATE_PART(dow, m.date_published),
         DATE_PART(month, m.date_published),
         DATE_PART(year, m.date_published),
         DATE_PART(week, m.date_published),
         DATE_PART(quarter, m.date_published);

CREATE OR REPLACE TABLE DIM_TIME AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('HOUR', m.date_published)) AS dim_time_id,
    m.date_published AS timestamp,
    CASE
        WHEN TO_NUMBER(TO_CHAR(m.date_published, 'HH24')) = 0 THEN 12
        WHEN TO_NUMBER(TO_CHAR(m.date_published, 'HH24')) <= 12 THEN TO_NUMBER(TO_CHAR(m.date_published, 'HH24'))
        ELSE TO_NUMBER(TO_CHAR(m.date_published, 'HH24')) - 12
    END AS hour,
    CASE
        WHEN TO_NUMBER(TO_CHAR(m.date_published, 'HH24')) < 12 THEN 'AM'
        ELSE 'PM'
    END AS ampm
FROM movie_staging m;

CREATE OR REPLACE TABLE FACT_MOVIE AS
SELECT 
    r.movie_id AS fact_movie_id,
    r.avg_rating,
    r.total_votes,
    r.median_rating,
    d.dim_person_id AS person_id,
    date_dim.dim_date_id AS dim_dateId,
    time_dim.dim_time_id AS dim_timeId,
    dim_movie.dim_movie_id AS dim_movie_id,
    CASE
        WHEN r.avg_rating BETWEEN 0 AND 2.5 THEN 'bad'
        WHEN r.avg_rating BETWEEN 2.6 AND 5.0 THEN 'average'
        WHEN r.avg_rating BETWEEN 5.1 AND 7.5 THEN 'good'
        WHEN r.avg_rating BETWEEN 7.6 AND 10 THEN 'great'
        ELSE 'unknown'
    END AS rating_score
FROM ratings_staging r
JOIN DIM_MOVIE dim_movie ON r.movie_id = dim_movie.dim_movie_id
JOIN DIM_PERSON d ON d.starred_movie_id = r.movie_id
JOIN DIM_DATE date_dim ON dim_movie.date_published = date_dim.date
JOIN DIM_TIME time_dim ON time_dim.timestamp = dim_movie.date_published;



DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS names_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS role_mapping_staging;
