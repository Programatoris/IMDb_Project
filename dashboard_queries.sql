--1. MOST MOVIES PRODUCED
SELECT production_company, COUNT(*) AS movie_count
FROM DIM_MOVIE
GROUP BY production_company
ORDER BY movie_count DESC
LIMIT 10;


--2. DISTRIBUTION OF PEOPLE IN MOVIE INDUSTRY BY AGE
SELECT age_category, COUNT(*) AS count
FROM DIM_PERSON
GROUP BY age_category
ORDER BY count DESC;


--3. MOVIE COUNT OVER THE YEARS
SELECT year, COUNT(*) AS movie_count
FROM DIM_DATE
JOIN DIM_MOVIE ON DIM_DATE.date = DIM_MOVIE.date_published
GROUP BY year
ORDER BY year;


--4. Average Gross Income vs. Average Rating by Genre
SELECT 
    m.genre, 
    ROUND(AVG(m.worldwide_gross_income), 2) AS avg_income, 
    ROUND(AVG(f.avg_rating), 2) AS avg_rating
FROM FACT_MOVIE f
JOIN DIM_MOVIE m ON f.fact_movie_id = m.dim_movie_id
GROUP BY m.genre
ORDER BY avg_income DESC;


--5. Top 5 Directors by Average Rating
SELECT 
    p.name AS director_name, 
    ROUND(AVG(f.avg_rating), 2) AS avg_rating, 
    COUNT(*) AS movie_count
FROM FACT_MOVIE f
JOIN DIM_PERSON p ON f.person_id = p.dim_person_id
GROUP BY p.name
ORDER BY avg_rating DESC
LIMIT 5;


--6. Duration vs. Average Rating of Movies
SELECT 
    m.title, 
    m.duration, 
    f.avg_rating
FROM DIM_MOVIE m
JOIN FACT_MOVIE f ON m.dim_movie_id = f.fact_movie_id
WHERE m.duration IS NOT NULL 
AND f.avg_rating IS NOT NULL
ORDER BY m.duration DESC
LIMIT 10;
