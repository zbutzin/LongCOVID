

@transform_pandas(
    Output(rid="ri.vector.main.execute.3157281f-4709-46bd-918a-df9b0048a7a6")
)
from pyspark.sql.types import *
def unnamed():
    schema = StructType([])
    return spark.createDataFrame([[]], schema=schema)

