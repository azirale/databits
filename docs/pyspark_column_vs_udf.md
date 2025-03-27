# Column Creator over UDF in Pyspark

I have seen some recommendations to use UDFs when working with Pyspark when you want to create some reusable code.
Defining the UDF once so that you can get its functionality in multiple transformations and pipelines without code duplication is good, but...

Python UDFs can make things horribly slow.
Spark has to do fair amount of extra work for a UDF to execute. It must...

* [Pickle](https://docs.python.org/3/library/pickle.html) the original code from the driver
* Send the pickled code out to the workers
* The workers must spawn a Python process for each executor
* Each python process must unpickle the code
* During execution spark must take the input data and...
  * Serialise it so it can transfer it to Python
  * Send the data from the JVM process to the Python process
  * Deserialise the data in Python
  * Run the Python code row-by-row
  * Serialise the results
  * Send the data from the Python process back to the JVM
  * Deserialise the data in the JVM to integrate back into the [RDD](https://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds)

... and this adds a lot of overhead to the overall process.
Further, it also breaks potential optimisations because Spark cannot inspect the workings of the function to understand how it might be able to do any optimisations in earlier stages.

If you're working with Pyspark there is an alternative that enables code reuse without having to resort to UDFs.

## Column-Creating Functions

With Pyspark you have access to the DataFrame API, and part of that is the ability to define Column objects.
A Column is essentially an expression for how to project a column of data in the output DataFrame. You might usually see a Column created inline like this...

```py
import pyspark.sql.functions as F

# spark:SparkSession -- you need one of these

original_df = spark.createDataFrame([('  needs trimming  ',)],'from_this STRING')
cleaned_df = original_df.select(F.trim("from_this").alias("to_this"))
```

The appearance of the Column creating function inside the `select()` function may be a bit deceiving -- if you are used to SQL then it might look like this call to `trim("from_this")` only works inside the select statement, because you're selecting from a DataFrame that has that column name.

That isn't how Pyspark works.

When you refer to a column by name, Spark won't look for it until runtime when the Column's data needs to be evaluated.
That let's you define a column ahead-of-time, and the resulting DataFrame works exactly the same...

```py
import pyspark.sql.functions as F

# spark:SparkSession -- you need one of these

original_df = spark.createDataFrame([('  needs trimming  ',)],'from_this STRING')

trimmed_from_this_col = F.trim("from_this").alias("to_this")

cleaned_df = original_df.select(trimmed_from_this_col)
```

That also means that you can create a Column using a function, which can take input column names and other values as parameters.
An - admittedly silly for how simple it is - example for the above...

```py
import pyspark.sql.functions as F
from pyspark.sql import Column

# spark:SparkSession -- you need one of these

original_df = spark.createDataFrame([('  needs trimming  ',)],'from_this STRING')

def create_trimmed_col(input_column_name:str,resulting_column_name:str)->Column:
    return F.trim(input_column_name).alias(output_column_name)

cleaned_df = original_df.select(create_trimmed_col("from_this","to_this"))
```

So when would you make use of this approach? When you have standard ways to generate columns, or reusable business logic, that you want to apply in the same parameterised way in many places without having to reimplement all the Pyspark code.

A common one when creating deltas from full snapshots, or otherwise comparing datasets, is to create a hashdiff that is effectively a summary value of all the descriptive data that can be more quickly shuffled for comparison. We want this function to be implemented the exact same way everywhere, so that we only have to resolve flaws once, and so that it always works the same way. It is also somewhat complex and dynamic if you want it to be reusable.

```py
import pyspark.sql.functions as F
from pyspark.sql import Column

# spark:SparkSession -- you need one of these

def create_hashdiff_col(input_column_names:list[str])->Column:
    """Standard way to define a hashdiff column."""
    as_stringtypes = [F.col(colname).cast("STRING") for colname in input_column_names]
    null_differentiated = [F.coalesce(col,F.lit("~~NULL~~")) for col in as_stringtypes]
    concatted = F.concat_ws("|",*null_differentiated)
    hashed = F.sha2(concatted,256)
    default_named = hashed.alias("HASHDIFF")
    return default_named

# let's make some example data...
input_df = (
    spark.createDataFrame([
        ("Sydney", "Showers increasing", 21, 27),
        ("Melbourne", "Mostly sunny", 13, 30),
        ("Brisbane", "Rain", 20, 27),
        ("Perth", "Sunny", 21, 35),
        ("Adelaide", "Mostly sunny", 19, 32),
        ("Hobart", "Mostly sunny", 11, 25),
        ("Canberra", "Showers increasing", 13, 24),
        ("Darwin", "Showers, possible storm", 25, 32),
    ],
    "city STRING, description STRING, low INT, high INT"
    )
)

# and now project the key and a hashdiff of the associated data
output_df = (
    input_df
    .select(
        "city",
        create_hashdiff_col(["description","low","high"]).alias("hashdiff")
    )
)
```

If you are using Pyspark, and your column *can* be expressed with Pyspark functions and you want to make it reusable, then have a function create the Column for you using Pyspark.
Don't just jump to a UDF.
