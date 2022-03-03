# Databricks notebook source
import pandas as pd
#suponiendo que la fuente es el link y que este se actualiza en un periodo de tiempo lo importo usando pandas 
#para automatizar esta descarga cada vez que se ejecute la notebook

url = 'https://francisadbteststorage.blob.core.windows.net/challenge/nuevas_filas.csv?sp=r&st=2021-07-08T18:53:40Z&se=2022-12-09T02:53:40Z&spr=https&sv=2020-08-04&sr=b&sig=AK7dCkWE1xR28ktHfdSYU2RSZITivBQmv83U51pyJMo%3D'
pandasDf = pd.read_csv(url)  
pandasDf

# COMMAND ----------

import time  #de esta forma puedo obtener la fecha del sistema para copiar al campo  FECHA_COPIA
import datetime
timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

#considero la fecha de insersion como la fecha en la que se hizo la ingesta.
#uso pandas para poder asignar el timestamp de la hora en la que se insertan
pandasDf['FECHA_COPIA'] = pandasDf['FECHA_COPIA'].fillna(timestamp)
pandasDf

# COMMAND ----------

dfFromLink = spark.createDataFrame(pandasDf) #convierto el dataframe de Pandas en uno de Spark
display(dfFromLink)

# COMMAND ----------

dfFromLink.select('FECHA_COPIA').show() #me aseguro que se conserven los datos copiados como timestamp

# COMMAND ----------

#Creo la conexion a Azure Data Base
jdbcHostname = "leandroserver.database.windows.net"
jdbcDatabase = "PiDB"
jdbcPort = 1433
username = "leandro"
password = "39776048Isi"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, username, password)

connectionProperties = {
  "user" : username,
  "password" : password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}



# COMMAND ----------

#me traigo los datos existentes en la DB de Azure
data = "(select * from dbo.outPut) fromOutPut" 
dfFromDB = spark.read.jdbc(url=jdbcUrl, table=data, properties=connectionProperties)
display(dfFromDB)

# COMMAND ----------

#Concateno los datos provenientes de la BD con los nuevos de la ingesta desde el Link
dfToDb = dfFromLink.union(dfFromDB)
display(dfToDb)

# COMMAND ----------

#Saco todos aquellos registros que tengan duplicados en las columnas 'ID', 'MUESTRA', 'RESULTADO'

dfToDb = dfToDb.dropDuplicates(['ID', 'MUESTRA', 'RESULTADO'])
display(dfToDb)

# COMMAND ----------

#Finalmente el data frame dfToDb esta listo para insertarse en Azure sql Data Base
dfToDb.write.jdbc(url=jdbcUrl, table="outPut", mode = "overwrite",properties=connectionProperties)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


