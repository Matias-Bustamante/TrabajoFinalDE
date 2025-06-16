
--Consulta SQL 

--Cantidad de alquileres de autos, teniendo en cuenta sólo los vehículos
--ecológicos (fuelType hibrido o eléctrico) y con un rating de al menos 4

Select COUNT(*) as total_alquiler from car_rental.car_rental_analytics cra 
where fueltype IN ('hybrid', 'electric') and rating>=4

--los 5 estados con menor cantidad de alquileres

Select 
t.ranking, 
t.state_name, 
t.total_alquiler 
FROM (
Select 
ROW_NUMBER() OVER(order by COUNT(*) asc) as ranking, 
state_name as state_name, 
Count(*) as total_alquiler
from car_rental.car_rental_analytics cra 
group by state_name) as t 
where t.ranking <=5

--los 10 modelos (junto con su marca) de autos más rentados
Select 
t.ranking, 
t.marca, 
t.modelo, 
t.precio
FROM
(
Select 
ROW_NUMBER() OVER(order by SUM(rate_daily) DESC ) as ranking, 
make as marca, 
model as modelo, 
SUM(rate_daily) as precio
from car_rental.car_rental_analytics cra 
group by make, model ) as t  
where t.ranking <=10

--Mostrar por año, cuántos alquileres se hicieron, teniendo en cuenta automóviles
--fabricados desde 2010 a 2015

Select year, Count(*) as total_alquiler FROM car_rental.car_rental_analytics cra 
where year between 2010 and 2015 
group by year 

--las 5 ciudades con más alquileres de vehículos ecológicos (fuelType hibrido o electrico)
Select 
t.ranking , 
t.city, 
t.total_alquiler
FROM (
Select 
row_number() OVER(order by COUNT(*) desc) as ranking, 
city, 
Count(*) as total_alquiler
from car_rental.car_rental_analytics cra 
where fuelType in ('hybrid', 'electric')
group by city ) as t 
where t.ranking <=5


--el promedio de reviews, segmentando por tipo de combustible

Select fuelType as TipoCombustible, AVG(reviewcount ) as revisiones from car_rental.car_rental_analytics cra 
group by fueltype 


