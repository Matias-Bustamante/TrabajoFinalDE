#!/bin/sh 

rm -f /home/hadoop/scripts/examen_final/ejercicio_2/*.csv 

wget -P /home/hadoop/scripts/examen_final/ejercicio_2 -O /home/hadoop/scripts/examen_final/ejercicio_2/car_rental_data.csv https://data-engineer-edvai-public.s3.amazonaws.com/CarRentalData.csv
wget -P /home/hadoop/scripts/examen_final/ejercicio_2 -O /home/hadoop/scripts/examen_final/ejercicio_2/georef_usa.csv https://data-engineer-edvai-public.s3.amazonaws.com/georef-united-states-of-america-state.csv

/home/hadoop/hadoop/bin/hdfs dfs -rm /buckets/alquiler_automovil/*.csv 

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/scripts/examen_final/ejercicio_2/*.csv /buckets/alquiler_automovil
