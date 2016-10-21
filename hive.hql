create database ybelkhayat location '/user/ybelkhayatzougari/'; -- On cree la base de donnees dans le dossier personnel dans users/

use ybelkhayatzougari; -- Puis on utilise cette base de donnees

CREATE EXTERNAL TABLE IF NOT EXISTS stats(prenom STRING, genre ARRAY<STRING>, origine ARRAY<STRING>)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;' COLLECTION ITEMS TERMINATED BY ',' location '/user/ybelkhayatzougari/tables'; -- On cree la table qui va representer notre csv

load data inpath '/user/ybelkhayatzougari/prenoms.csv' into table stats_prenoms; -- On charge les donnees du csv dans notre table

create table orc_stats stored as ORC as select * from stats_prenoms; -- On l'enregistre au format orc pour optimiser

/* Les 3 MapReduce faits auparavant avec Java */

/*
	Compter les nombre de prénoms par origine.
	On utilise ltrim pour gérer les strings mal formés avec des espaces dans les bords.
*/
select ltrim(origines),count(1) from orc_stats lateral view explode(origine) temp as origines group by ltrim(origines);

/*
	Compter le nombre de prénoms qui ont n origines
*/
select size(origine),count(1) from orc_stats group by size(origine);

/*
	Compter le ratio de prénoms males sur le nombre total de prénoms
*/
select count(case when array_contains(genre,"m") then 1 else null end) / count(*) from orc_stats;