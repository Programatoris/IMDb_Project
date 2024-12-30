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
