
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col,lower 
from pyspark.sql.types import IntegerType
spark=SparkSession.builder.master("spark://172.17.0.2:7077").enableHiveSupport().getOrCreate() 

from pyspark.sql import SparkSession 
from pyspark.sql.functions import col,lower 
from pyspark.sql.types import IntegerType
spark=SparkSession.builder.master("spark://172.17.0.2:7077").enableHiveSupport().getOrCreate() 

##Leer datos en formato csv 
df_car=spark.read.option("Header", True).csv("hdfs://172.17.0.2:9000/buckets/alquiler_automovil/car_rental_data.csv") 
df_geo=spark.read.option("Header", True).option("delimiter", ";").csv("hdfs://172.17.0.2:9000/buckets/alquiler_automovil/georef_usa.csv")

##Se verifica tipo de datos de las columnas
df_car.printSchema()
df_geo.printSchema()

##Se modifica los nombres de las columnas 

df_car=df_car.select(lower(col("fuelType")).alias("fuelType"), col("rating").cast(IntegerType()), col("renterTripsTaken").cast(IntegerType()), 
                     col("reviewCount").cast(IntegerType()), col("`location.city`").alias("city"), 
                     col("`location.country`").alias("country"), col("`location.latitude`").alias("latitude"), 
                     col("`location.longitude`").alias("longitude"),
                     col("`location.state`").alias("state_name"),
                     col("`owner.id`").cast(IntegerType()).alias("owner_id"), 
                     col("`rate.daily`").cast(IntegerType()).alias("rate_daily"), 
                     col("`vehicle.make`").alias("make"), 
                     col("`vehicle.model`").alias("model"), 
                     col("`vehicle.type`").alias("type"), 
                     col("`vehicle.year`").cast(IntegerType()).alias("year"))

df_geo=df_geo.select(
                    col("`Geo Point`").alias("geo_point"), 
                    col("`Geo Shape`").alias("geo_shape"), 
                    col("Year").cast(IntegerType()).alias("year"), 
                    col("`Official Code State`").alias("official_code_state"), 
                    col("`Official Name State`").alias("official_name_state"), 
                    col("`Iso 3166-3 Area Code`").alias("iso_3166_3_area_code"), 
                    col("Type").alias("type"), 
                    col("`United States Postal Service state abbreviation`").alias("united_state_postal"), 
                    col("`State FIPS Code`").alias("fpis_code"), 
                    col("`State GNIS Code`").alias("gnis_code")
)

df_car=df_car.filter(col("rating").isNotNull())

df_car=df_car.filter(col("state_name")!="TX") 

df_geo=df_geo.filter(col("official_name_state")!="Texas")


df_results=df_car.join(df_geo, df_car["state_name"]==df_geo["united_state_postal"], "inner").select("fuelType", "rating", "renterTripsTaken", "reviewCount", "city", 
                                                                                        "state_name", "owner_id", "rate_daily", "make", "model", df_car["year"])


df_results.write.insertInto("car_rental.car_rental_analytics")

