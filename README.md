# IMDb_Project ETL Proces
Tento repozitár obsahuje postup ETL procesu na tému IMDb v prostredí snowflake. Výsledok tohto procesu nám umožňuje lahšie analyzovať dáta napr. pomocou vizualizácií. 
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
