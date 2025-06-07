# THIS IS A DEMO ONLY
# This will not have perfect programming practices
# This does not have macro features for DBT
# This is just to show logic for how to filter/join/window to combine date effectivity ranges
# The logic *can* adjust to timestamps, it does not require dates
# Actually it only requires that the 'start_at' and 'end_at' values are correctly sortable
# It DOES ALSO RELY on use of 'exclusive' end values -- that is one 'end_at' is the next 'start_at'
# ^ this is slightly different to how some places do it and requires use of '<end_at' instead of '<=end_at'

# REQUIRES THESE MODULES
# pip install duckdb tabulate

# MAKE SURE YOU ARE RUNNING PYTHON FROM INSIDE THIS DIRECTORY
# THE FILE PATHS FOR LOADING CSV FILES ARE RELATIVE TO YOUR WORKING DIRECTORY

import duckdb
import tabulate

# we will be working with a local file for demo purposes
db = duckdb.connect("working.duckdb")


def showme(query: str) -> None:
    # just a quick way to look at the contents of a table
    as_arrow = db.execute(query).arrow()
    as_list = as_arrow.to_pylist()
    table_string = tabulate.tabulate(as_list, headers="keys", tablefmt="pipe")
    print(table_string)


def reset_alldim_table() -> None:
    # let us define the combined scd2 dim
    db.execute(
        """
        CREATE OR REPLACE TABLE dim_all (
            cust_id VARCHAR(32),
            start_at DATE,
            end_at DATE,
            dim_a_start_at DATE,
            dim_b_start_at DATE,
            dim_c_start_at DATE,
            PRIMARY KEY (cust_id,start_at)
        )
        ;
        """
    )


# import data state for dim tables
def import_dims(folder_name: str) -> None:
    # getting our starting state with a variety of scd2 dims
    # note that we are using 'exclusive' end_at values, not 'inclusive'
    for table_name in ["dim_a", "dim_b", "dim_c"]:
        _ = db.execute(f"""CREATE OR REPLACE TABLE {table_name} (cust_id VARCHAR(32),start_at DATE,end_at DATE);""")
        _ = db.execute(f"""INSERT INTO {table_name} SELECT * FROM read_csv('{folder_name}/{table_name}.csv');""")
        _ = db.commit()


# this query gets all the data in an individual dim
def new_timepoints_query(dim_tablename: str) -> str:
    query = f"""
    WITH active_rows AS (
        SELECT cust_id,start_at,end_at
        FROM {dim_tablename} AS e
        WHERE
            NOT EXISTS (
                SELECT 1
                FROM dim_all AS r
                WHERE
                    e.cust_id = r.cust_id
                    AND r.start_at > e.start_at
            )
    )
    SELECT
        cust_id,
        start_at AS time_point
    FROM active_rows
    UNION -- not UNION ALL you will get duplicates when using exclusive end dates
    SELECT
        cust_id,
        end_at AS time_point
    FROM active_rows
    WHERE end_at != '9999-12-31' -- exclude end date, we generate that automatically later
    """
    return query


def update_combined_dim_table() -> None:
    # put the scraped dates into dedicated tables to work with them more easily
    # note that according to 'new_timepoints_query' this is ONLY the data not already covered in dim_all
    db.execute(f"CREATE OR REPLACE TABLE temp_dim_a_timepoints AS SELECT * FROM ({new_timepoints_query('dim_a')})")
    db.execute(f"CREATE OR REPLACE TABLE temp_dim_b_timepoints AS SELECT * FROM ({new_timepoints_query('dim_b')})")
    db.execute(f"CREATE OR REPLACE TABLE temp_dim_c_timepoints AS SELECT * FROM ({new_timepoints_query('dim_c')})")
    # combine all the scraped dates from all dims into one big table and dedupe dates common across source tables
    db.execute(
        """
        CREATE OR REPLACE TABLE temp_dim_all_timepoints AS
        SELECT DISTINCT *
        FROM (
            SELECT * FROM temp_dim_a_timepoints
            UNION ALL
            SELECT * FROM temp_dim_b_timepoints
            UNION ALL
            SELECT * FROM temp_dim_c_timepoints
        )
        """
    )
    # we can use this 'all timepoints' to splice the combined ranges
    # this gives us every start-end across all dims for each cust_id
    db.execute(
        """
        CREATE OR REPLACE TABLE temp_dim_all_spliced AS
        SELECT
            cust_id,
            time_point AS start_at, -- each time point marks the beginning of some splice
            LEAD(time_point,1,'9999-12-31') OVER (PARTITION BY cust_id ORDER BY time_point) AS end_at
        FROM temp_dim_all_timepoints
        """
    )
    # with all the new splices, we can grab the associated start date from each individual team and put it into a column
    # if you can pull a value into a variable to send into this query you might help optimise it a little
    # find the earliest 'start_at' from temp_dim_all_spliced
    # and then you can set a filter on reading each dim where end_at must be more than that value
    # this will be true across all cust_id values and all tables, and might help limit the data being read
    db.execute(
        """
        CREATE OR REPLACE TABLE dim_all_new_data AS
        SELECT
            o.*,
            a.start_at AS dim_a_start_at,
            b.start_at AS dim_b_start_at,
            c.start_at AS dim_c_start_at
        FROM temp_dim_all_spliced AS o
        LEFT JOIN dim_a AS a
            ON o.cust_id = a.cust_id
            -- splice start has to be in the dim's range
            AND o.start_at >= a.start_at AND o.start_at < a.end_at
        LEFT JOIN dim_b AS b
            ON o.cust_id = b.cust_id
            AND o.start_at >= b.start_at AND o.start_at < b.end_at
        LEFT JOIN dim_c AS c
            ON o.cust_id = c.cust_id
            AND o.start_at >= c.start_at AND o.start_at < c.end_at
        """
    )
    # with the new/replacement data we can merge into the target table
    # this example uses duckdb's 'INSERT OR REPLACE' syntax to achieve a MERGE
    db.execute(
        """
        INSERT OR REPLACE INTO dim_all
        SELECT *
        FROM dim_all_new_data
        """
    )


# will wipe the target table so this is all re-runnable
reset_alldim_table()

# this will be empty -- no table will display
showme("SELECT * FROM dim_all ORDER BY cust_id, start_at")


# bring in the starting data for individual dims - when we haven't run the process before
import_dims("starting_data")

# run the combined dim process
update_combined_dim_table()

# see that the data has been created
# you can see it works when dim_b does not start until after dim_a and dim_c
# it also works when dim_c has an 'end date' before the end date of dim_a and dim_b
# the 'start_at' and 'end_at' for dim_all has no duplicates or overlaps
# each individual dim's 'start_at' column links directly to a start_at value in that dim table
showme("SELECT * FROM dim_all ORDER BY cust_id, start_at")

# we can update the data to a 'day two' process
# in this data only dim_a has had an update
import_dims("following_data")

# we can see that only dim_a has data being picked up by the new_timepoints_query
# all the other dims are only bringing in their 'current' data - if it exists (it does not for dim_c)
showme(new_timepoints_query("dim_a"))  # new row only
showme(new_timepoints_query("dim_b"))  # latest existing row only
showme(new_timepoints_query("dim_c"))  # no relevant data at this timepoint

# the process works exactly the same
update_combined_dim_table()

# the new data in dim_a is present, and everything else is as is
showme("SELECT * FROM dim_all ORDER BY cust_id, start_at")

# we left behind a temp table so we can see intermediate steps
# here we can see that we ONLY processed new/relevant data -- not everything
showme("SELECT * FROM dim_all_new_data ORDER BY cust_id, start_at")
