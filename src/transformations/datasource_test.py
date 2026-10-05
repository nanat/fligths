from pyspark import pipelines as dp


@dp.table
def test_package():
    import pyspark_datasources

    return spark.range(1)