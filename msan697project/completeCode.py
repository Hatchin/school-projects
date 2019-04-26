#sparklight: Cara Qin, Qian Li, Sangyu Shen

from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.tuning import ParamGridBuilder

def toInt(inval):
    try:
        return int(inval)
    except ValueError:
        return None

def toFloat(inval):
    try:
        return float(inval)
    except ValueError:
        return None

def toLong(inval):
    try:
        return long(float(inval))
    except ValueError:
        return None

def trip_to_time(inval):
    try:
    return datetime.strptime(inval, "%m/%d/%Y %H:%M").date()
        except ValueError:
            return None

def weather_to_time(inval):
    try:
    return datetime.strptime(inval, "%m/%d/%Y")
        except ValueError:
            return None

def dow(x):
    v = x.strftime('%w')
    return v
dow = udf(dow, StringType())

def impute(df):
    cols =df.columns
    if 'events' in df.columns:
        cols.remove('events')
    for col in cols:
        mode = df.groupby(df[col]).count().orderBy('count', ascending=False).select(df[col]).collect()[0][0]
        df = df.na.fill({col:mode})
    return df

def indexStringColumns(df, cols):
    newdf = df
    
    for c in cols:
        si = StringIndexer(inputCol=c, outputCol=c+"-num")
        sm = si.fit(newdf)
        newdf = sm.transform(newdf).drop(c)
        newdf = newdf.withColumnRenamed(c+"-num", c)
    return newdf

def oneHotEncodeColumns(df, cols):
    newdf = df
    for c in cols:
        onehotenc = OneHotEncoder(inputCol=c, outputCol=c+"-onehot", dropLast=False)
        newdf = onehotenc.transform(newdf).drop(c)
        newdf = newdf.withColumnRenamed(c+"-onehot", c)
    return newdf

tripSchema = StructType([
                         StructField('cus_id',LongType(), True),
                         StructField('duration',LongType(), True),
                         StructField('date',DateType(), True),
                         StructField('start_station_name',StringType(), True),
                         StructField('start_station_id',LongType(), True),
                         StructField('end_date',DateType(), True),
                         StructField('end_station_name',StringType(), True),
                         StructField('end_station_id',LongType(), True),
                         StructField('bike_id',LongType(), True),
                         StructField('subscription_type',StringType(), True),
                         StructField('zip_code',LongType(), True)
                         ])

weatherSchema = StructType([
                            StructField("date", DateType(), True),
                            StructField("max_temperature_f", FloatType(), True),
                            StructField("mean_temperature_f", FloatType(), True),
                            StructField("min_temperature_f", FloatType(), True),
                            StructField("max_dew_point_f", FloatType(), True),
                            StructField("mean_dew_point_f", FloatType(), True),
                            StructField("min_dew_point_f", FloatType(), True),
                            StructField("max_humidity", FloatType(), True),
                            StructField("mean_humidity", FloatType(), True),
                            StructField("min_humidity", FloatType(), True),
                            StructField("max_sea_level_pressure_inches", FloatType(), True),
                            StructField("mean_sea_level_pressure_inches", FloatType(), True),
                            StructField("min_sea_level_pressure_inches", FloatType(), True),
                            StructField("max_visibility_miles", FloatType(), True),
                            StructField("mean_visibility_miles", FloatType(), True),
                            StructField("min_visibility_miles", FloatType(), True),
                            StructField("max_wind_Speed_mph", FloatType(), True),
                            StructField("mean_wind_Speed_mph", FloatType(), True),
                            StructField("max_gust_speed_mph", FloatType(), True),
                            StructField("precipitation_inches", FloatType(), True),
                            StructField("cloud_cover", FloatType(), True),
                            StructField("events", StringType(), True),
                            StructField("wind_dir_degrees", FloatType(), True),
                            StructField("zip_code", IntegerType(), True)
                            ])

stationSchema = StructType([
                            StructField('station_id',LongType(), True),
                            StructField('name',StringType(), True),
                            StructField('lat',FloatType(), True),
                            StructField('long',FloatType(), True),
                            StructField('dock_count',LongType(), True),
                            StructField('city',StringType(), True),
                            StructField('installation_date',DateType(), True)
                            ])

def stringToPost_trip(r):
    return (
            toLong(r[0]),
            toLong(r[1]),
            trip_to_time(r[2]),
            r[3],
            toLong(r[4]),
            trip_to_time(r[5]),
            r[6],
            toLong(r[7]),
            toLong(r[8]),
            r[9],
            toLong(r[10]))

def stringToPost_weather(r):
    return (
            weather_to_time(r[0]),
            toFloat(r[1]),
            toFloat(r[2]),
            toFloat(r[3]),
            toFloat(r[4]),
            toFloat(r[5]),
            toFloat(r[6]),
            toFloat(r[7]),
            toFloat(r[8]),
            toFloat(r[9]),
            toFloat(r[10]),
            toFloat(r[11]),
            toFloat(r[12]),
            toFloat(r[13]),
            toFloat(r[14]),
            toFloat(r[15]),
            toFloat(r[16]),
            toFloat(r[17]),
            toFloat(r[18]),
            toFloat(r[19]),
            toFloat(r[20]),
            r[21],
            toFloat(r[22]),
            toInt(r[23]))

def stringToPost_station(r):
    return (
            toLong(r[0]),
            r[1],
            toFloat(r[2]),
            toFloat(r[3]),
            toLong(r[4]),
            r[5],
            weather_to_time(r[6]))

# trip
trip_raw = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://54.244.14.54/msan697.trip").load()
trip_rdd = trip_raw.rdd
trip_new = trip_rdd.map(lambda x: [str(x[6]), str(x[2]),str(x[7]), str(x[9]),str(x[8]),str(x[3]),str(x[5]),str(x[4]),str(x[1]),str(x[10]),str(x[11])])
trip = trip_new.map(lambda x: stringToPost_trip(x))
tripdf = sqlContext.createDataFrame(trip, tripSchema)
tripdf = tripdf.withColumn("day_of_week",dow(tripdf['date']))

# weather
weatherRDD = sc.textFile('/Users/yangyangyang/Desktop/MSAN/697DistributedDataSystem/FinalProject/sf-bay-area-bike-share/weather.csv')
weatherheader = weatherRDD.first()
weather = weatherRDD.filter(lambda line: line != weatherheader).map(lambda x:  x.encode('utf8').split(",")).map(lambda x:stringToPost_weather(x))
weatherdf = sqlContext.createDataFrame(weather, weatherSchema)

# station
station_raw = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://54.244.14.54/msan697.station").load()
station_rdd = station_raw.rdd
station_new = station_rdd.map(lambda x: [str(x[3]),str(x[7]),str(x[5]), str(x[6]),str(x[2]),\
                                         str(x[1]),str(x[4])])

station = station_new.map(lambda x:stringToPost_station(x))
stationdf = sqlContext.createDataFrame(station, stationSchema)

# status
status_raw = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://54.244.14.54/msan697.status").load()
status_rdd = status_raw.rdd
status_new = status_rdd.map(lambda x: [str(x[3]),str(x[1]),str(x[2]), str(x[4])])

# city_zip
city_zip_raw = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://54.244.14.54/msan697.city_zip").load()
city_zip = city_zip_raw['city','zip_code']

weather_city = weatherdf.join(city_zip, 'zip_code').drop('zip_code')
trip_station = tripdf.join(stationdf, (tripdf.start_station_name == stationdf.name))
complete_table = trip_station.join(weather_city, ['date', 'city'])
sqlContext.sql("DROP TABLE IF EXISTS raw_data")
complete_table.write.saveAsTable('raw_data')

data = spark.sql("select count(1) as num_trips, day_of_week, city,\
                 max_temperature_f, mean_temperature_f, \
                 min_temperature_f, max_dew_point_f, mean_dew_point_f, min_dew_point_f, \
                 max_humidity, mean_humidity, min_humidity, max_sea_level_pressure_inches,\
                 mean_sea_level_pressure_inches, min_sea_level_pressure_inches, \
                 max_visibility_miles, mean_visibility_miles, min_visibility_miles, \
                 max_wind_Speed_mph, mean_wind_Speed_mph, \
                 precipitation_inches, cloud_cover, events, wind_dir_degrees\
                 from raw_data group by date, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24")

clean_data = impute(data)
clean_data = clean_data.na.replace("", '0', ['events'])

dfnumeric = indexStringColumns(clean_data, ["city", "events", 'day_of_week'])
dfhot = oneHotEncodeColumns(dfnumeric, ["city", "events", 'day_of_week'])
input_cols=dfhot.columns[1:]
va = VectorAssembler(outputCol="features", inputCols=input_cols)
transformed_data = va.transform(dfhot).select("features", "num_trips").withColumnRenamed("num_trips", "label")

sets = transformed_data.randomSplit([0.6, 0.2, 0.2], 42)
train =  sets[0].cache()
valid = sets[1].cache()
test = sets[2].cache()

#linear regresssion
lr = LinearRegression()
evaluator = RegressionEvaluator()

cv = CrossValidator().setEstimator(lr).setEvaluator(evaluator).setNumFolds(5)
paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]).build()

cv.setEstimatorParamMaps(paramGrid)
cvmodel = cv.fit(train)
RegressionEvaluator().setMetricName("rmse").evaluate(cvmodel.bestModel.transform(valid))

#random forest
rf = RandomForestRegressor(maxDepth=20)
evaluator = RegressionEvaluator()

cv = CrossValidator().setEstimator(lr).setEvaluator(evaluator).setNumFolds(5)
paramGrid = ParamGridBuilder().addGrid(rf.maxDepth, [10, 15, 20, 25]).build()

cv.setEstimatorParamMaps(paramGrid)
cvmodel = cv.fit(train)
RegressionEvaluator().setMetricName("rmse").evaluate(cvmodel.bestModel.transform(valid))

#gradient boosting
gbt = GBTRegressor()
evaluator = RegressionEvaluator()
gbtmodel = gbt.fit(train)
gbtpredicts = gbtmodel.transform(valid)
RegressionEvaluator().setMetricName("rmse").evaluate(gbtpredicts)

feature = ['max_temperature_f','mean_temperature_f','min_temperature_f',
           'max_dew_point_f', 'mean_dew_point_f', 'min_dew_point_f', 'max_humidity',
           'mean_humidity','min_humidity','max_sea_level_pressure_inches','mean_sea_level_pressure_inches',
           'min_sea_level_pressure_inches','max_visibility_miles','mean_visibility_miles',
           'min_visibility_miles','max_wind_Speed_mph','mean_wind_Speed_mph',
           'precipitation_inches','cloud_cover','wind_dir_degrees',
           'san_francisco','city_2','city_3','city_4','city_5',
           'event_1','event_2','event_3','event_4','event_5','event_6'
           'sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']

feats = []
for feature, importance in zip(feature, gbtmodel.featureImportances):
    feats.append((feature, importance)) 

feat_impo = sc.parallelize(feats)
feat_impo.sortBy(lambda x: x[1], ascending=False).take(10)


