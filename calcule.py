#! /usr/bin/python3

import sys
import csv
import json
import time

import psycopg2
import requests

db = psycopg2.connect(dbname="cquest")
db.set_session(autocommit=True)

c = db.cursor()
request = requests.Session()

c.execute("CREATE TABLE IF NOT EXISTS proximite_decheteries_2017_clean (idinspire text, code_sinoe text, temps float, distance float)")
c.execute("CREATE INDEX IF NOT EXISTS proximite_decheteries_2017_clean_id on proximite_decheteries_2017_clean (idinspire)")

sql = """
SELECT
    c.idinspire,
    st_x(st_centroid(c.wkb_geometry)) as lon,
    st_y(st_centroid(c.wkb_geometry)) as lat,
    array_agg(st_length(ST_ShortestLine(geom, c.wkb_geometry)::geography) order by st_length(ST_ShortestLine(geom, c.wkb_geometry)::geography)) as distance,
    string_agg(format('%s,%s', st_x(d.geom), st_y(d.geom)), ';' order by st_length(ST_ShortestLine(geom, c.wkb_geometry)::geography)) as coord,
    string_agg(d.code_sinoe, ';' order by st_length(ST_ShortestLine(geom, c.wkb_geometry)::geography)) as sinoe
FROM
    insee_carroyage_2015 c
JOIN
    decheteries_2017_clean d ON (ST_Intersects(st_buffer(c.wkb_geometry::geography, 15000)::geometry, geom))
LEFT JOIN
    proximite_decheteries_2017_clean p ON (p.idinspire=c.idinspire)
WHERE
    c.depcom like '"""+sys.argv[1]+"""%'
    AND p.code_sinoe IS NULL
GROUP BY 1, 2, 3
"""


c.execute(sql)
carreaux = c.fetchall()
values = ""
print('Département:', sys.argv[1], ', Carreaux:',len(carreaux))
for r in carreaux:
    origin = '%.6f,%.6f' % (r[1], r[2])
    destinations = r[4]
    query = 'http://192.168.0.79:5000/table/v1/car/' + \
        origin+';'+destinations+'?sources=0&annotations=duration,distance'

    try:
        osrm = request.get(query)
    except:
        time.sleep(1)
        request = requests.Session()
        osrm = request.get(query)

    # search for shortest duration
    routes = json.loads(osrm.text)

    best_route = None
    if 'durations' in routes:
        for index in range(0,len(routes['durations'][0])):
            try:
                if routes['durations'][0][index] and routes['durations'][0][index] > 0:
                    if best_route is None:
                        best_route = index
                    elif routes['durations'][0][index] < routes['durations'][0][best_route]:
                        best_route=index
            except:
                print(r, routes['durations'][0])
    
        
        best_time = routes['durations'][0][best_route]
        best_destination = r[4].split(';')
        best_destination = best_destination[best_route-1]
        best_sinoe = r[5].split(';')
        best_sinoe = best_sinoe[best_route-1]
        # distance itinéraire + départ > iti + iti > arrivée
        distance = routes['distances'][0][best_route]
        distance = distance + routes['sources'][0]['distance']
        distance = distance + routes['destinations'][best_route]['distance']

        print(r[0], best_sinoe, best_time, distance)
        values = values + "('%s', '%s', %f, %f)," % (
            r[0], best_sinoe, best_time, distance)
        if len(values) > 100000:
            values = values[:-1]
            c.execute(
                "INSERT INTO proximite_decheteries_2017_clean VALUES " + values)
            c.execute('COMMIT')
            values = ""
    else:
        print(r[0],"no duration")

if values != "":
    values = values[:-1]
    sql = "INSERT INTO proximite_decheteries_2017_clean VALUES " + values
    c.execute(sql)
c.execute('COMMIT')
