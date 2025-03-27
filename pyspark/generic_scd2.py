from pyspark.sql import DataFrame, Window as W, Column
import pyspark.sql.functions as F


def scd2(
    input_df: DataFrame,
    entity_key_colnames: list[str],
    start_colname: str,
    # if you are re-slicing an existing SCD2 specify existing end column
    # set to `None` if you just have a single 'effective as of' stamp on the row
    end_colname: str|None,
    high_end_literal: Column,
) -> DataFrame:
    descriptive_colnames = [
        colname
        for colname in input_df.columns
        if colname not in [*entity_key_colnames, start_colname, end_colname]
    ]
    window = W.partitionBy(*entity_key_colnames).orderBy("_CURRENT_START", "_CURRENT_END")
    output_df = (
        input_df
        # add the hashdiff over the descriptive column, and make copies of the start-end with consistent names within this function
        .withColumns(
            {
                "_CURRENT_HASHDIFF": hashdiff(descriptive_colnames),
                "_CURRENT_START": F.col(start_colname),
                "_CURRENT_END": F.col(end_colname) if end_colname else high_end_literal,
            }
        )
        # set ourselves up with lead and lag values that will be needed to determine new start-end timestamps
        .withColumns(
            {
                "_PREVIOUS_HASHDIFF": F.lag("_CURRENT_HASHDIFF",).over(window),
                "_PREVIOUS_END": F.lag("_CURRENT_END").over(window),
                "_NEXT_START": F.lead("_CURRENT_START").over(window),
            }
        )
        # set up markers for when the descriptives have changed, and when they expire
        .withColumns(
            {
                "_HAS_CHANGED": (
                    # first value for key
                    F.col("_PREVIOUS_HASHDIFF").isNull()
                    # hashdiff has changed from previous one
                    | (F.col("_PREVIOUS_HASHDIFF") == F.col("_HASHDIFF"))
                    # there is a gap in 'active' period since last version, even if hashdiff is the same
                    | (F.col("_PREVIOUS_END") < F.col("_CURRENT_START"))
                ),
                # override the current end value in case of overlaps or no end
                "_CURRENT_END": F.least(
                    F.col("_CURRENT_END"), F.col("_NEXT_START"), high_end_literal
                ),
            }
        )
        # we only need rows flagged as changed -- anything else is just a copy of the prior row
        .filter(F.col("_HAS_CHANGED"))
        # after filtering we can set final start and end values bridging contiguous unchanged blocks
        .withColumns(
            {
                "_NEXT_START": F.lead("_CURRENT_START"),
                # this tracks the 'previous end' as was set on the next record before filtering
                "_NEXT_PREVIOUS_END": F.lead("_PREVIOUS_END"),
            }
        )
        .withColumns(
            {
                "_CURRENT_END": F.least(
                    F.col("_CURRENT_END"),
                    F.col("_NEXT_START"),
                    F.col("_NEXT_PREVIOUS_END"),
                    high_end_literal,
                )
            }
        )
        # only keep the original column names, in order, with the new start-end overriding the originals
        .withColumns(
            {
                start_colname: F.col("_CURRENT_START"),
                end_colname: F.col("_CURRENT_END"),
            }
        )
        .select(*input_df.columns)
    )
    return output_df


def hashdiff(compare_colnames: list[str]) -> Column:
    as_strings = [F.col(colname).cast("STRING") for colname in compare_colnames]
    concatenated = F.concat_ws("|", *as_strings)
    hashed = F.sha2(concatenated, 256)
    return hashed
