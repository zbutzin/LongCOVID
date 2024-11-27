

@transform_pandas(
    Output(rid="ri.vector.main.execute.9b0d833d-bb7b-40e1-a629-6248804b664a")
)
from pyspark.sql.types import *
def unnamed():
    schema = StructType([])
    return spark.createDataFrame([[]], schema=schema)

