
from pyspark.sql import SparkSession
from pyspark.sql.functions import col 
from pyspark.sql.functions import to_date

spark=SparkSession.builder.master("spark://172.17.0.2:7077").enableHiveSupport().getOrCreate() 

vuelos_2021=spark.read.option('header', True).option('delimiter',';').csv("hdfs://172.17.0.2:9000/buckets/aeropuerto/2021-informe-ministerio.csv")
vuelos_2022=spark.read.option('header', True).option('delimiter',';').csv("hdfs://172.17.0.2:9000/buckets/aeropuerto/202206-informe-ministerio.csv")
detalles=spark.read.option('header', True).option('delimiter',';').csv("hdfs://172.17.0.2:9000/buckets/aeropuerto/aeropuertos_detalle.csv")
vuelos_2021.columns ##visualización de las columnas vuelos 2021 

vuelos_2021.printSchema()  ##Tipos de datos vuelos 2021

vuelos_2022.columns ## visualiza columna vuelos 2022

vuelos_2022.printSchema() ##Visualiza tipo de datos vuelos 2022

detalles.columns ## Visualiza columna dataframe detalle

detalles.printSchema() ##Visualiza tipo de datos dataframe detalles

vuelos=vuelos_2021.union(vuelos_2022) 
vuelos.filter(vuelos["Fecha"].contains("2022")).show(10, False)

df_vuelos=vuelos.select("Fecha", col("Hora UTC").alias("hora_utc"), col("Clase de Vuelo (todos los vuelos)").alias("clase_de_vuelo"), 
                        col("Clasificación Vuelo").alias("clasificacion_de_vuelo"), col("Tipo de Movimiento").alias("tipo_de_movimiento"), 
                        col("Aeropuerto").alias("aeropuerto"), col("Origen / Destino").alias("origen_destino"), 
              col("Aerolinea Nombre").alias("aerolinea_nombre"), col("Aeronave").alias("aeronave"), col("Pasajeros").alias("pasajeros"))

df_vuelo_domestico=df_vuelos.filter(df_vuelos.clasificacion_de_vuelo!="Internacional")

df_vuelo_domestico=df_vuelo_domestico.na.fill(value=0, subset=["pasajeros"])

data_vuelo=df_vuelo_domestico.select(to_date("Fecha", "dd/MM/yyyy").alias("Fecha"), df_vuelo_domestico["hora_utc"], df_vuelo_domestico["clase_de_vuelo"], 
                                    df_vuelo_domestico["clasificacion_de_vuelo"], df_vuelo_domestico["tipo_de_movimiento"], 
                                    df_vuelo_domestico["aeropuerto"], df_vuelo_domestico["origen_destino"], df_vuelo_domestico["aerolinea_nombre"], 
                                    df_vuelo_domestico["aeronave"], df_vuelo_domestico["pasajeros"].cast("int"))

data_vuelo.printSchema()

df_detalles_vuelo=detalles.select(detalles["local"], detalles["oaci"], detalles["iata"], 
                                 detalles["tipo"], detalles["denominacion"], detalles["coordenadas"], 
                                 detalles["latitud"], detalles["longitud"], 
                                 detalles["elev"], 
                                 detalles["uom_elev"], detalles["ref"], detalles["distancia_ref"], 
                                 detalles["direccion_ref"], detalles["condicion"], 
                                 detalles["control"], detalles["region"], detalles["uso"], 
                                 detalles["trafico"], detalles["sna"], detalles["concesionado"], 
                                 detalles["provincia"])

df_detalles_vuelo=df_detalles_vuelo.na.fill(value=0, subset=["distancia_ref"])

df_detalles_vuelo.columns

data_vuelo.write.insertInto("aeropuerto.vuelos")

df_detalles_vuelo=df_detalles_vuelo.select(col("local").alias("aeropuerto"), 
                        col("oaci").alias("oac"), 
                        col("iata"), 
                        col("tipo"), 
                        col("denominacion"), 
                        col("coordenadas"), 
                        col("latitud"), 
                        col("longitud"), 
                        col("elev").cast("float"), 
                        col("uom_elev"), 
                        col("ref"), 
                        col("distancia_ref").cast("float"), 
                        col("direccion_ref"), 
                        col("condicion"), 
                        col("control"),
                        col("region"), 
                        col("uso"), 
                        col("trafico"), 
                        col("sna"), 
                         col("concesionado"),
                        col("provincia")
                        )

df_detalles_vuelo.printSchema()

df_detalles_vuelo.write.insertInto("aeropuerto.detalle_vuelo")
