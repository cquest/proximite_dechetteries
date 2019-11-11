#! /bin/bash

curl -s http://files.opendatarchives.fr/data.ademe.fr/sinoe-annuaire-decheteries-dma-2017%20SINOE%20-%20Annuaire%20d%C3%A9cheteries%20DMA%202017.csv.gz | gunzip | in2csv -f xls > decheteries_2017_sinoe.csv

psql -c "CREATE TABLE decheteries_2017_sinoe (code_sinoe text, nom text, depcom text, lon numeric, lat numeric,qualite float)"
csvcut -c 2,3,11,16,17,18 decheteries_2017_sinoe.csv  | psql -c '\copy decheteries_2017_sinoe from STDIN with (format csv, header true)'

psql -c 'CREATE TABLE decheteries_2017_geocodees (code_sinoe text, nom text, depcom text, lon numeric, lat numeric)'
csvcut decheteries_2017.geocoded.csv -c 3,4,12,1,2 | psql -c '\copy decheteries_2017_geocodees from STDIN with (format csv, header true)'

psql -c "
DROP TABLE IF EXISTS decheteries_2017;
CREATE TABLE decheteries_2017 AS
SELECT
  s.code_sinoe,
  s.nom,
  right('0'||s.depcom) as depcom,
  s.qualite,
  case when qualite<1 and geom is not null then st_setsrid(st_makepoint(s.lon, s.lat),4326) else st_transform(st_setsrid(st_makepoint(s.lat, s.lon),3857),4326) end as geom
FROM
  decheteries_2017 d
JOIN decheteries_2017_sinoe s ON d.code_sinoe=s.code_sinoe;
CREATE INDEX ON decheteries_2017 using gist (geom);
"



wget http://data.cquest.org/insee_carroyage/Filosofi2015_carreaux_200m_shp.zip
unzip Filosofi2015_carreaux_200m_shp.zip
for Z in *.7z; do 7z x $Z; done

for SHP in *.shp
do
  ogr2ogr -f pgdump /vsistdout/ -nln insee_carroyage_2015 -t_srs EPSG:4326 Filosofi2015_carreaux_200m_metropole.shp -append | psql
done


psql -c "CREATE INDEX ON insee_carroyage_2015 (depcom)"
