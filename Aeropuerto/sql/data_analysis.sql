
--Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022

Select COUNT(*) as CantidadDeVuelos from aeropuerto.vuelos v  
where fecha BETWEEN  '2021-12-01' AND '2022-01-31'


--Cantidad de pasajeros que viajaron en aerolineas argentinas entre el 01/01/2021 30/06/2022

Select aerolinea_nombre , SUM(pasajeros) as TotalPasajeros from aeropuerto.vuelos 
where aerolinea_nombre like '%AEROLINEAS ARGENTINAS SA%' and 
fecha between '2021-01-01' and '2022-06-30'
group by aerolinea_nombre 

--Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto
--de arribo, ciudad de arribo, y cantidad de pasajeros de cada vuelo, entre el 01/01/2022
--y el 30/06/2022 ordenados por fecha de manera descendiente

Select v.fecha, 
v.horautc ,
v.aeropuerto as codigo_salida, 
a1.denominacion as ciudad_salida, 
v.origen_destino as codigo_arribo, 
a2.denominacion as ciudad_arribo, 
v.pasajeros  as cantidadPasajero 
from aeropuerto.vuelos as v
join aeropuerto.detalle_vuelo a1 
on v.aeropuerto =a1.iata 
join aeropuerto.detalle_vuelo a2 
on v.origen_destino =a2.iata
where v.fecha between '2022-01-01' and '2022-06-30'
order by v.fecha desc 


--Cuales son las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el
--30/06/2022 exceptuando aquellas aerolíneas que no tengan nombre.

Select 
t.aerolinea, 
t.cantidadPasajero, 
t.ranking
FROM (
Select 
RANK() over(order by SUM(v.pasajeros) desc) as ranking,
v.aerolinea_nombre as aerolinea, 
SUM(v.pasajeros) as cantidadPasajero
FROM aeropuerto.vuelos as v 
where v.aerolinea_nombre <>"0" and v.fecha between '2021-01-01' and '2022-06-30'
group by v.aerolinea_nombre   ) as t
where t.ranking<=10


--Cuales son las 10 aeronaves más utilizadas entre el 01/01/2021 y el 30/06/22 que
--despegaron desde la Ciudad autónoma de Buenos Aires o de Buenos Aires,
--exceptuando aquellas aeronaves que no cuentan con nombre. 
Select 
t.aeronave , 
t.Utilizacion, 
t.ranking
FROM (
Select 
RANK() OVER(order by COUNT(*) desc) as ranking,  
v.aeronave as aeronave, 
count(*) as Utilizacion 
from aeropuerto.vuelos v 
join aeropuerto.detalle_vuelo as dv 
on v.aeropuerto=dv.iata and dv.provincia  like '%BUENOS AIRES%' 
WHERE v.aeronave <>"0" and v.fecha between '2021-01-01' and '2022-06-30' 
group by v.aeronave 
order by Utilizacion desc ) as t 
where t.ranking <=10
