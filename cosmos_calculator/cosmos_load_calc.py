import math

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

# statics
# service values from microsoft for cosmos
provisioned_min_ru = 400                        # cosmos provisioned RU/s cannot go below this value (autoscale minimum can though)
provisioned_max_ru_scale = 100                  # cosmos provisioned RU/s maximum can go this high without upscaling the minimum
provisioned_max_ru_without_service = 1000000    # Microsoft will not let you automate RU/s beyond this without calling first
autoscale_min_ru = 100                          # autoscale has a lower minimum RU/s compared to provisioned
autoscale_max_ru_scale = 10                     # autoscale maximum RU/s can be this much higher than its minimum
physical_partition_max_ru = 10000               # absolute RU/s limit for a physical partition in Cosmos -- any more and it will split
min_ru_per_stored_gb = 10                       # cosmos requires at least this much RU/s per GB of stored data
ru_rounding_multiple = 100                      # cosmos RU/s has to be set to a multiple of this number

# tested values from a ~2TB data load of ~1B json records of ~800B UTF-8 bytes each
cosmos_bytes_per_json_bytes = 2.35      # cosmos will stored this multiple of the input data, roughly, assuming some indexing, actual results will vary ~10%
cosmos_insert_json_bytes_per_ru = 105   # RU cost for each write (all 1B) was measured against the raw json bytes provided, and for new records this came out
cosmos_update_json_bytes_per_ru = 72    # RU cost for each write (another 1B) was measured against the raw json bytes provided, and for updated records this came out


# will need to know the raw json bytes to be uploaded
# assuming we have pyspark for the data source -- you'll have to get that sorted yourself
def get_raw_json_bytes(source_df):
    all_columns = source_df.columns
    cols_as_struct = F.struct(all_columns)
    struct_as_json = F.to_json(cols_as_struct)
    json_textlength = F.length(struct_as_json)
    raw_json_bytes = source_df.select(json_textlength).groupBy().sum().collect()[0][0]
    return raw_json_bytes


# easier way of round up to a multiple of something
def round_up_to_nearest(original_value,rounding_multiple):
    result = math.ceil(original_value/rounding_multiple)*rounding_multiple
    return result

def report_recommended_cosmos_scale_for_sparkdf(upload_df:DataFrame)->None:
    raw_json_bytes = get_raw_json_bytes(upload_df)
    report_recommended_cosmos_scale_for_jsonbytes(raw_json_bytes)



# you will need to know the raw json bytes you are dealing with for the full collection first
def report_recommended_cosmos_scale_for_jsonbytes(raw_json_bytes:int)->None:
    # calculate out how cosmos will measure its stored GB
    cosmos_stored_kib = raw_json_bytes * cosmos_bytes_per_json_bytes / 1024             # cosmos works with a base unit of KiB
    cosmos_stored_gb = cosmos_stored_kib / 1000 / 1000                                  # cosmos does 10^3 scaling on KiB to determine GB, not 2^10 like GiB
    min_ru_by_stored_gb = cosmos_stored_gb*min_ru_per_stored_gb                         # scale the stored GB to the service minimum RU/s per storage
    min_ru_by_stored_gb = round_up_to_nearest(min_ru_by_stored_gb,ru_rounding_multiple) # then round up the minimum RU/s to the next valid multiple
    # if the minimum per the storage is below the minimum provisioned value suggest using autoscale
    suggest_use_autoscale = min_ru_by_stored_gb < provisioned_min_ru
    if suggest_use_autoscale:
        suggest_max_provisioned = physical_partition_max_ru                     # cosmos appears to do this by design, max provisioned to min is full container to min autoscale
        suggest_min_provisioned = provisioned_min_ru                            # hardcapped min ru for provisioned
        suggest_autoscale_max = min_ru_by_stored_gb * autoscale_max_ru_scale    # autoscale is set as low as storage would allow
    else:
        suggest_max_provisioned = min_ru_by_stored_gb * provisioned_max_ru_scale    # set the max provisioned scale out such that we can still scale down to the minimum
        provisioned_max_capped = suggest_max_provisioned > provisioned_max_ru_without_service
        if provisioned_max_capped:
            suggest_max_provisioned = provisioned_max_ru_without_service            # limit provisioned top end without call to Microsoft
        suggest_min_provisioned = min_ru_by_stored_gb                               # minimum RU/s is determined by storage
        suggest_autoscale_max = suggest_min_provisioned * autoscale_max_ru_scale    # max autoscale is highest value that still scales down to minimum
    # and print out recommendation
    print(f"Raw JSON size is {raw_json_bytes:,} bytes")
    print(f"This converts to approx {cosmos_stored_gb:,.1f} GB - per cosmos RU/s/GB calculation")
    print(f"Minimum provisioned RU will be approx {suggest_min_provisioned:,} RU/s")
    print(f"Suggest setting max provisioned for loads to {suggest_max_provisioned:,} RU/s")
    print(f"If using autsocale, set the autoscale maximum to {suggest_autoscale_max:,} RU/s")
    if suggest_use_autoscale:
        print(f" -- Small data volume indicates better costing if autoscale minimum is used, and provisioned max was restricted to accommodate")
    if provisioned_max_capped:
        print(f" -- Max provisioned RU/s is soft capped. Would need to contact Microsoft to remove cap for collection.")
    print(f"When loading fresh data the saturation bandwidth will be approx {suggest_max_provisioned*cosmos_insert_json_bytes_per_ru/1024/1024:,.2f} MB/s")
    print(f"When loading update data the saturation bandwidth will be approx {suggest_max_provisioned*cosmos_update_json_bytes_per_ru/1024/1024:,.2f} MB/s")
