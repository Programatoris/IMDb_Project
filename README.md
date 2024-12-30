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

### 3.2 Krok 2: Transformácia (T)
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

### 3.3 Krok 3: Načítanie (L)
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
___

## 4 Vizualizácia dát
### 4.1 Snowflake dashboard
![dashboard_visualizations](https://github.com/user-attachments/assets/143bcc29-8181-4f10-b009-515d6b6e5bfa)

Dashboard obsahuje **6 grafov**, ktoré obsahujú odpovede na otázky ako najviac vydaných filmov, počet ľudí vo filmovom priemysle podľa veku, porovnanie zárobku a hodnotenia filmov podľa žánru a podobne.
___

### 4.2 Popis a vysvetlenie jednotlivých grafov
**1.) MOST MOVIES PRODUCED**
```sql
SELECT production_company, COUNT(*) AS movie_count
FROM DIM_MOVIE
GROUP BY production_company
ORDER BY movie_count DESC
LIMIT 10;
```
Tento graf zobrazuje počet filmov, ktoré každá produkčná spoločnosť vytvorila. Graf pomáha odpovedať na otázku, ktorá produkčná spoločnosť je najaktívnejšia v oblasti výroby filmov.

**2.) DISTRIBUTION OF PEOPLE IN MOVIE INDUSTRY BY AGE**
```sql
SELECT age_category, COUNT(*) AS count
FROM DIM_PERSON
GROUP BY age_category
ORDER BY count DESC;
```
Graf zobrazuje rozdelenie osôb pracujúcich v filmovom priemysle podľa vekových kategórií. Pomáha odpovedať na otázku, ako sú v priemysle zastúpené rôzne vekové skupiny. Ukazuje, ktoré vekové kategórie dominujú v rôznych oblastiach filmovej produkcie.

**3.) MOVIE COUNT OVER THE YEARS**
```sql
SELECT year, COUNT(*) AS movie_count
FROM DIM_DATE
JOIN DIM_MOVIE ON DIM_DATE.date = DIM_MOVIE.date_published
GROUP BY year
ORDER BY year;
```
Tento graf zobrazuje počet filmov podľa rokov, v ktorých boli vydané. Pomáha zodpovedať otázku, ako sa počet vydaných filmov menil v priebehu rokov. Môže ukázať trend v rastúcom alebo klesajúcom počte produkcií počas času.

**4.) Average Gross Income vs. Average Rating by Genre**
```sql
SELECT 
    m.genre, 
    ROUND(AVG(m.worldwide_gross_income), 2) AS avg_income, 
    ROUND(AVG(f.avg_rating), 2) AS avg_rating
FROM FACT_MOVIE f
JOIN DIM_MOVIE m ON f.fact_movie_id = m.dim_movie_id
GROUP BY m.genre
ORDER BY avg_income DESC;
```
Tento graf porovnáva priemerný celosvetový zisk a priemerné hodnotenie podľa žánrov filmov. Pomáha zodpovedať otázku, ktoré žánre filmov majú najvyšší priemerný zisk a najvyššie hodnotenie. Ukazuje vzťah medzi finančným úspechom a kvalitou filmov v rôznych žánroch.

**5.) Top 5 Directors by Average Rating**
```sql
SELECT 
    p.name AS director_name, 
    ROUND(AVG(f.avg_rating), 2) AS avg_rating, 
    COUNT(*) AS movie_count
FROM FACT_MOVIE f
JOIN DIM_PERSON p ON f.person_id = p.dim_person_id
GROUP BY p.name
ORDER BY avg_rating DESC
LIMIT 5;
```
Tento graf zobrazuje päť režisérov s najvyšším priemerným hodnotením filmov. Pomáha zodpovedať otázku, ktorí režiséri dosiahli najlepšie hodnotenia od divákov. Zobrazuje tiež počet filmov, ktoré daní režiséri režírovali, čo môže naznačovať ich konzistenciu v tvorbe kvalitných filmov.

**6.) Duration vs. Average Rating of Movies**
```sql
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
```
Tento graf zobrazuje vzťah medzi dĺžkou filmu a jeho priemerným hodnotením. Pomáha zodpovedať otázku, či existuje vzťah medzi trvaním filmu a jeho hodnotením. Môže ukázať, či dlhšie alebo kratšie filmy majú tendenciu získavať lepšie hodnotenia.
___

  - **Vypracoval:** Erik Martiš
