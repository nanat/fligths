from pyspark import pipelines as dp
from pyspark_datasources import OpenSkyDataSource
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


spark.dataSource.register(OpenSkyDataSource)

FLIGHTS_SCHEMA = StructType(
    [
        StructField("time_ingest", TimestampType()),
        StructField("icao24", StringType()),
        StructField("callsign", StringType()),
        StructField("origin_country", StringType()),
        StructField("time_position", TimestampType()),
        StructField("last_contact", TimestampType()),
        StructField("longitude", DoubleType()),
        StructField("latitude", DoubleType()),
        StructField("geo_altitude", DoubleType()),
        StructField("on_ground", BooleanType()),
        StructField("velocity", DoubleType()),
        StructField("true_track", DoubleType()),
        StructField("vertical_rate", DoubleType()),
        StructField("sensors", ArrayType(IntegerType())),
        StructField("baro_altitude", DoubleType()),
        StructField("squawk", StringType()),
        StructField("spi", BooleanType()),
        StructField("category", IntegerType()),
    ]
)


@dp.table
def ingest_flights():
    return spark.readStream.format("opensky").schema(FLIGHTS_SCHEMA).load()