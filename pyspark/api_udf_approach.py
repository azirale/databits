import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, explode_outer
from pyspark.sql.types import ArrayType, StringType

spark = SparkSession.builder.getOrCreate()

# replace these with something that works for you
SOME_PATH_FOR_RAW_DATA_SAVE = ".data/raw_responses"
SOME_BASE_PATH_FOR_YOUR_JSONL = ".data/jsonl_data"

# set up API calls with offsets so each gets distinct data
data = [
    ("Invoices", "https://api.example.com/invoices", 0),
    ("Invoices", "https://api.example.com/invoices", 100),
    ("Invoices", "https://api.example.com/invoices", 200),
    ("PurchaseOrders", "https://api.example.com/purchaseOrders", 0),
    ("PurchaseOrders", "https://api.example.com/purchaseOrders", 150),
    ("PurchaseOrders", "https://api.example.com/purchaseOrders", 300),
]
input_df = spark.createDataFrame(data, schema="tablename STRING, endpoint STRING, offset INT")


# set up how to read data from the API based on the inputs above
## define this as its own function for easy development in REPL
def fetch_responses(endpoint: str, offset: int) -> list[str]:
    # replace with your actual request
    response_body = """[{"k":"v1"},{"k":"v2"}]"""
    as_list = [json.dumps(v) for v in json.loads(response_body)]
    return as_list


## wrap the function into a udf
fetch_responses_udf = udf(fetch_responses, ArrayType(StringType()))

# define a dataframe that will use the UDF to read from the API
responses_col = fetch_responses_udf(col("endpoint"), col("offset")).alias("response_array")
get_everything_df = input_df.select("*", responses_col)

# just immediately save the responses without any further processing
get_everything_df.write.format("parquet").save(SOME_PATH_FOR_RAW_DATA_SAVE)

# use the saved data to process to split the response arrays and save per table in the inputs
fetch_each_col = explode_outer(responses_col).alias("response")
per_element_df = get_everything_df.select("tablename", fetch_each_col)
per_element_df.write.format("text").partitionBy("tablename").save(SOME_BASE_PATH_FOR_YOUR_JSONL)


# now read back for your desired tablename - this uses the hive style partitioning to pick just one table
def read_exploded_json(tablename: str) -> DataFrame:
    json_df = spark.read.format("json").load(f"{SOME_BASE_PATH_FOR_YOUR_JSONL}/tablename={tablename}")
    return json_df
