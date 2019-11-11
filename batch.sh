#! /bin/bash

# import et préparation des données
bash prepare.sh

# calcul des itinéraires les plus courts
time for D in $(seq -w 01 95)
do
  echo $D
done | parallel python calcule.py {}

# table inverse POI > population
psql -c "
DROP TABLE IF EXISTS decheteries_2017_pop;
CREATE TABLE decheteries_2017_pop AS
SELECT
    d.code_sinoe,
    d.nom,
    d.geom,
    sum(ind) as population 
FROM
    decheteries_2017 d
JOIN
    proximite_decheteries_2017 p ON (p.code_sinoe=d.code_sinoe)
JOIN
    insee_carroyage_2015 c ON (c.idinspire=p.idinspire)
GROUP BY 1,2,3;
"

# import liste des circonscriptions / communes
psql -c "create table deputes_circo (CODE_DPT text,NOM_DPT text,CODE_COMMUNE text,NOM_COMMUNE text,CODE_CIRC_LEGISLATIVE text,CODE_CANTON text,NOM_CANTON text);"
psql -c "\copy deputes_circo from Table_de_correspondance_circo_legislatives2017-1.csv with (format csv, header true)"
psql -c "
update deputes_circo set code_dpt = right('00'||code_dpt,2);
update deputes_circo set code_commune = code_dpt|| right('000'||code_commune,3);
update deputes_circo set code_circ_legislative = right('00'||code_circ_legislative,2);
"

# liste des députes
csvcut 8-rne-deputes.txt -t -e iso8859-1 -c 1,3,5,6,7 -K 1 > deputes.csv
psql -c "create table deputes (dep text, circo text, nom text, prenom text, sexe text)"
psql -c "\copy deputes from deputes.csv with (format csv, header true)"

# vue élu + email / circo / commune / cp
psql -c "
create or replace view deputes_communes as
select
    dep,
    circo,
    nom,
    prenom,
    sexe,
    code_commune as depcom,
    lower(format('%s.%s@assemblee-nationale.fr',replace(unaccent(prenom),' ',''),replace(unaccent(nom),' ',''))) as email
from
    deputes d
join
    deputes_circo c on (code_dpt=dep and code_circ_legislative=circo)
group by 1,2,3,4,5,6,7;
"

# export CSV stats nationales
psql -c "COPY (

SELECT
    'France' as Dep,
    '' as Circo,
    'Total' as Nom,
    '' as Prenom,
    '' as Genre,
    '' as email,
    count(distinct(p.code_sinoe)) as nb,
    sum(distance*ind/1000)/sum(ind) as km,
    sum(temps/60*ind)/sum(ind) as mn,
    max(distance/1000) max_km,
    max(temps/60) as max_mn,
    sum(case when temps> 600 then ind else 0 end)::int as pop_10mn,
    sum(case when temps>1200 then ind else 0 end)::int as pop_20mn,
    sum(case when temps>1800 then ind else 0 end)::int as pop_30mn,
    sum(case when temps> 600 then ind else 0 end)/sum(ind)*100 as pct_10mn,
    sum(case when temps>1200 then ind else 0 end)/sum(ind)*100 as pct_20mn,
    sum(case when temps>1800 then ind else 0 end)/sum(ind)*100 as pct_30mn
FROM
    insee_carroyage_2015 c
JOIN
    proximite_decheteries_2017_clean p ON (p.idinspire=c.idinspire)
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2

) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)" > proximite_decheteries_deputes.csv

# export CSV stats puis par député(e)
psql -c "COPY (

SELECT
    d.dep,
    d.circo,
    d.nom,
    d.prenom,
    case when d.sexe='M' then 'Monsieur' else 'Madame' end,
    d.email,
    count(distinct(p.code_sinoe)) as nb,
    round((sum(distance*ind/1000)/sum(ind))::numeric,1) as km,
    (sum(temps/60*ind)/sum(ind))::int as mn,
    max(distance/1000)::int max_km,
    max(temps/60)::int as max_mn,
    sum(case when temps> 600 then ind else 0 end)::int as pop_10mn,
    sum(case when temps>1200 then ind else 0 end)::int as pop_20mn,
    sum(case when temps>1800 then ind else 0 end)::int as pop_30mn,
    sum(case when temps> 600 then ind else 0 end)/sum(ind)*100 as pct_10mn,
    sum(case when temps>1200 then ind else 0 end)/sum(ind)*100 as pct_20mn,
    sum(case when temps>1800 then ind else 0 end)/sum(ind)*100 as pct_30mn
FROM
    insee_carroyage_2015 c
JOIN
    proximite_decheteries_2017_clean p ON (p.idinspire=c.idinspire)
JOIN
    deputes_communes d ON (d.depcom=c.depcom)
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2

) TO STDOUT WITH (FORMAT CSV, HEADER FALSE)" >> proximite_decheteries_deputes.csv
