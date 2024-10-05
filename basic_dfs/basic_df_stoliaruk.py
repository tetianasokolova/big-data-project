import pyspark.sql.types as t
from pyspark.sql import SparkSession

def basic_test_df(spark: SparkSession):
    data = [('Yulia', 'Stoliaruk', 3)]
    schema = t.StructType([
        t.StructField('first_name', t.StringType(), True),
        t.StructField('last_name', t.StringType(), True),
        t.StructField('group', t.IntegerType(), True)
    ])
    df = spark.createDataFrame(data=data, schema=schema)
    return df