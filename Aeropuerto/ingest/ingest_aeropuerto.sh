#!/bin/sh

rm -f /home/hadoop/scripts/examen_final/ejercicio_1/*.csv 

wget -P /home/hadoop/scripts/examen_final/ejercicio_1 https://data-engineer-edvai-public.s3.amazonaws.com/2021-informe-ministerio.csv
wget -P /home/hadoop/scripts/examen_final/ejercicio_1 https://data-engineer-edvai-public.s3.amazonaws.com/202206-informe-ministerio.csv
wget -P /home/hadoop/scripts/examen_final/ejercicio_1 https://data-engineer-edvai-public.s3.amazonaws.com/aeropuertos_detalle.csv

/home/hadoop/hadoop/bin/hdfs dfs -rm /buckets/aeropuerto/*.csv 

/home/hadoop/hadoop/bin/hdfs dfs -put  /home/hadoop/scripts/examen_final/ejercicio_1/*.csv /buckets/aeropuerto
