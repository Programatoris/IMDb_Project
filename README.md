# IMDb_Project ETL Proces
Tento repozitár obsahuje postup ETL procesu na tému IMDb v prostredí snowflake. Výsledok tohto procesu nám umožňuje lahšie analyzovať dáta napr. pomocou vizualizácií.
___
## 1. Úvod
Projekt je zameraný na preskúmanie tém ako sú napríklad sledovanosť filmov podľa rôznych vekových skupín či, ktoré sú najúspešnejšie filmové vydavateľstvá a za, ktoré filmy vďačia svojmu úspechu.
### 1.1 Popis zdrojových dát
Dáta sú čerpané z raw, netransformovanej databázy. Táto databáza obsahuje 6 základných tabuliek:
  - `movie` (obsahuje stĺpce ako názov, rok vydania, dĺžku a podobne)
  - `ratings` (obsahuje stĺpce ako priemerné hodnotenie, celkový počet hlasov a stredné hodnotenie)
  - `genre` (obsahuje názov žánru)
  - `names` (obsahuje stĺpce ako meno, výšku, dátum narodenia a známe filmy)
  - `director_mapping` (spojovacia tabuľka medzi movie a names pre režisérov)
  - `role_mapping` (spojovacia tabuľka medzi movie a names pre hercov)
### 1.2 ERD Diagram
![IMDB_ERD](https://github.com/user-attachments/assets/95c76368-3935-40db-bcdd-177d37e7a91f)

Dáta z entito-relačného-diagramu (ERD) budú následne transformované pomocou ETL procesu.
___
## 2. Návrh dimenzionálneho modelu
### 2.1 Popis hlavných metrík a kľúčov vo faktovej tabuľke
Faktová tabuľka obsahuje hlavné metriky na analýzu filmov, vrátane ich hodnotení, počtu hlasov a súvisiacich informácií o dátume a čase publikácie, ako aj údajov o osobnostiach, ktoré sa podieľali na tvorbe filmu.

**Metriky:**
  - `avg_rating` - priemerné hodnotenie filmu.
  - `total_votes` - celkový počet hlasov.
  - `median_rating` - medián hodnotení.
  - `rating_score` - kategória hodnotenia (napr. "bad", "good").

**Kľúče:**
  - `fact_movie_id` - primárny kľúč faktovej tabuľky.
  - `dim_person_id` - odkaz na dimenziu DIM_PERSON.
  - `dim_dateId` - odkaz na dimenziu DIM_DATE.
  - `dim_timeId` - odkaz na dimenziu DIM_TIME.
  - `dim_movie_id` - odkaz na dimenziu DIM_MOVIE.
___
### 2.2 Stručný popis dimenzií
**1. DIM_MOVIE**
  - `Údaje:` Informácie o filmoch (názov, rok, trvanie, krajina, príjmy, žáner, kategórie ako duration_score a income_score).
  - `Vzťah:` Prepája sa cez dim_movie_id k filmovým hodnoteniam v FACT_MOVIE.
  - `Typ dimenzie:` SCD typu 2 (zmeny v trvaní, príjmoch atď.).

**2. DIM_PERSON**
  - `Údaje:` Informácie o osobnostiach (meno, dátum narodenia, veková kategória, výška, filmy, na ktorých pracovali).
  - `Vzťah:` Prepája sa cez dim_person_id k filmom v FACT_MOVIE.
  - `Typ dimenzie:` SCD typu 2 (zmeny vo filmoch, ktoré osobnosť ovplyvnila).

**3. DIM_DATE**
  - `Údaje:` Informácie o dátumoch (deň, mesiac, kvartál, názvy dní a mesiacov).
  - `Vzťah:` Prepája sa cez dim_date_id na dátum publikácie v FACT_MOVIE.
  - `Typ dimenzie:` Stabilná dimenzia.

**4. DIM_TIME**
  - `Údaje:` Detaily o čase (hodiny, AM/PM).
  - `Vzťah:` Prepája sa cez dim_time_id na čas publikácie v FACT_MOVIE.
  - `Typ dimenzie:` Stabilná dimenzia.

![star_schema2](https://github.com/user-attachments/assets/ec0054f6-ce7a-4528-bbc5-ab443626469a)

Dáta sú usporiadané do faktovej a dimenzionálnych tabuliek v hviezdicovej schéme (Star Schema).
___
## 3. ETL proces v nástroji snowflake
### 3.1 Krok 1: Extrakcia (E)
V tomto kroku sa získavajú údaje zo vstupných tabuliek (staging tabuliek). Tieto údaje môžu byť ďalej transformované alebo čistené predtým, než sa načítajú do cieľových tabuliek.
```sql
CREATE OR REPLACE STAGE SEAL_IMDb_STAGE;
```
Vytvorenie STAGE na uschovanie csv dát.

```sql
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
```
Vytvorenie custom formátu pre čítanie súborov csv, ktoré sú uložené v STAGE.
    
```sql
COPY INTO movie_staging (movie_id, title, yearr, date_published, duration, country, worldwide_gross_income, languages, production_company)
FROM @SEAL_IMDB_STAGE/moviee.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format2, NULL_IF = ('\\N'));

COPY INTO names_staging (name_id, name, height, date_of_birth, known_for_movies)
FROM @SEAL_IMDB_STAGE/namess.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format);
```
Následné kopírovanie dát zo STAGE do staging tabuliek podľa custom formátu.
___

### 3.2 Transformácia (T)
V tomto kroku sa údaje transformujú, čo zahŕňa výpočty nových atribútov, agregáciu údajov a prípravu na načítanie do cieľových tabuliek (dimenzionálnych alebo faktových tabuliek).

**SQL príkazy:**
  - `JOIN:` Používa sa na kombinovanie údajov zo súvisiacich tabuliek na základe spoločných polí, ako je movie_id alebo name_id.
  - `CASE WHEN:` Tento príkaz sa využíva na kategorizáciu číselných alebo dátových polí na preddefinované skupiny (napr. kategorizácia dĺžky filmu alebo hrubého príjmu do kategórií ako 'krátky', 'stredný', 'dlhý' alebo 'chudobný', 'dobrý', 'skvelý').
  - `EXTRACT a DATE_PART:` Používajú sa na manipuláciu s dátumami, napríklad na získanie roka, mesiaca a dňa z časovej pečiatky.


**Transformácia údajov o filmoch (DIM_MOVIE)**
```sql
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
        WHEN m.duration > 90 THEN 'long'
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
```
**Účel:**
  - Vytvoriť tabuľku DIM_MOVIE, ktorá obsahuje unikátne záznamy o filmoch.
  - Použiť príkaz CASE WHEN na kategorizáciu dĺžky filmu a hrubého príjmu do rôznych hodnôt, ako sú 'krátky', 'stredný', 'dlhý', 'zlý', 'dobrý' a 'skvelý'.
  - Použiť JOIN na spájanie údajov o filmoch a žánroch.

**Transformácia údajov o osobnostiach (DIM_PERSON)**
```sql
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
```
**Účel:**
  - Vytvoriť tabuľku DIM_PERSON, ktorá obsahuje podrobnejšie údaje o osobnostiach.
  - Použiť príkaz CASE WHEN na kategorizáciu vekovej skupiny osôb, napr. 'mladý', 'stredného veku' a 'senior'.
  - Použiť JOIN na spájanie údajov o rolách a menách.

**Transformácia údajov v tabuľke (FACT_MOVIE)**
```sql
CREATE OR REPLACE TABLE FACT_MOVIE AS
SELECT 
    r.movie_id AS fact_movie_id,
    r.avg_rating,
    r.total_votes,
    r.median_rating,
    d.dim_person_id AS person_id,
    date_dim.dim_date_id AS dim_dateId,
    time_dim.dim_time_id AS dim_timeId,
    dim_movie.dim_movie_id AS movie_id,
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
```
**Účel:**
  - Vytvoriť faktovú tabuľku FACT_MOVIE, ktorá spája hodnotenia, filmy, osoby, dátumy a časové údaje.
  - Použiť JOIN na spojenie rôznych dimenzionálnych tabuliek (DIM_MOVIE, DIM_PERSON, DIM_DATE, DIM_TIME).
  - Použiť príkaz CASE WHEN na kategorizáciu hodnotenia do kategórií ako 'zlý', 'priemerný', 'dobrý', 'skvelý'.
___

### 3.3 Načítanie (L)
V poslednom kroku sa transformované údaje načítajú do faktových a dimenzionálnych tabuliek, kde budú pripravené na analýzu. Následne po úspešnom načítaní transformovaných dát môžeme pôvodné tabuľky (staging) s raw dátami vymazať.
```sql
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS names_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS role_mapping_staging;
```
Príkaz DROP vymaže konkrétnu tabuľku aj s dátami, ktoré obsahuje. Tento príkaz je doplnení o podmienku IF EXISTS, ktorá predchádza prípadným chybám.

