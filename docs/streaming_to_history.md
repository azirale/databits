# Combine Streaming Updates with SCD2 in Lakehouse

I was nerd sniped by the following question (paraphrased)...

> 'For analytics, we need to start refreshing our data more often, under 5 minutes. The output table is a Slowly Changing Dimension Type 2 (SCD2) table in Iceberg format.
>  
> I haven't seen any implementation of a SCD2 table using a streaming-processor, so I'm starting to feel it might be an anti-pattern.
>  
> Anyone has any thoughts or recommendations?

## Is this an anti-pattern?

It is a little.
You don't generally need up-to-minute data and full time sliced history for all data.
How are unchanging values from months and years ago important to the processing of constantly shifting data from minutes ago?

## What is better than lakehouse? Why?

This could be done a bit better with a row-based RDBMS, where you can update individual rows at a time by setting an expiry for the current record and adding its superseding record all in one transaction, doing this for one entity at a time.

With a columnar format in large files like iceberg+parquet you're going to be doing a lot of re-processing as you can only effectively read whole files at a time.
Also, without a writer that can properly handle deletion vectors or row-level-deletes, you're going to be duplicating a lot of data.
Either way, you're going to end up with a lot of files and versions on the table, which will require compact and vacuum steps to clean out.

## What can we do to adapt this to lakehouse?

There are other structures and setups you can use to help.
Your SCD2 table should have an 'is_active' flag as a partition, so that all the merge processing can completely skip old expired data.
It would likely also be more efficient to have the new incoming data just go to an append-only table with a date partition, then have the history for that portion be calculated on query**, rather than constantly merging it.
Then you could do a larger bulk merge process periodically, so that the append-only portion doesn't get too large.

## Don't we still need a lot of files to handle streaming writes?

You can use Kinesis firehose to automatically batch your writes to an append-only table every 5 minutes, so everything up to there is a relatively easy and reliable way to get streaming updates available without creating an absurd number of files.

## How do we handle this lakehouse setup?

Say you keep a full-history SCD2 table with start_date, end_date, and an is_current flag.
Make sure the data is partitioned on 'is_current', as that will ensure that queries that only need currently active data can completely skip partitions with superseded or expired data.

You would only update into that table periodically.
Let's say you do daily updates -- any query that doesn't need up-to-the-minute data, and can run off of the state from the end of the previous day, can just use this full SCD2 table directly and doesn't need anything else.
That makes those queries easier, and a bit faster.

Now to support 'up-to-the-minute' data you would need another table that is an append-only of all incoming streaming data.
You don't do SCD2 on this because there will be a lot of duplicated data and files and versions as things are rewritten to support updates to existing values.
This table is only needed for data in the current day however, as anything older is in your full SCD2 history table.
So, you can partition this data by the incoming event date and only query for the current day's data, to completely skip any older files.
Yesterday's data can be read and filtered by partition for updating the SCD2 full history table, and anything older can be dropped by partition easily.

To get 'current state' for each entity in the data... (In no specific SQL dialect)

```sql
-- get the current data from full history *if* not superseded in today's append log
SELECT ...
FROM full_history AS f
LEFT ANTI JOIN append_log AS a ON f.entity_key = a.entity_key
WHERE is_current = 'Y'
-- then union just the most recent entry from the append log
UNION ALL
SELECT ...
FROM append_log
WHERE event_date >= {start_of_today()}
QUALIFY event_date = MAX(event_date) OVER (PARTITION BY entity_key)
;
```

To get the 'full history' for every entity requires a bit more...

```sql
-- get all the old data that is already expired, it cannot be altered by append log
SELECT ...
FROM full_history
WHERE is_current = 'N'
-- add current data from history, override end date if superseded from append log
UNION ALL
SELECT ...
    COALESCE(s.first_event_date,f.end_date) AS end_date
FROM full_history AS f
WHERE is_current = 'Y'
LEFT JOIN (
    SELECT entity_key, MIN(event_date) as first_event_date
    FROM append_log
    WHERE event_date >= {start_of_today()}
    GROUP BY entity_key
    ) AS s ON f.entity_key = s.entity_key
-- add in the append log data with calculated effectivity ranges
UNION ALL
SELECT ...
    event_date AS start_date,
    LEAD(event_date,1,{high_date}) OVER (PARTITION BY entity_key ORDER BY event_date)
;
```

This assumes your start-end ranges use an 'exclusive' end value.
That is: the 'end' of a prior record is the same value as the 'start' of the next.
I set up the code this way because it means you never have to tweak the values by some arbitrary constant each time the context switches between start and end, and it works with any ordinal type - including lexicographically ordered strings, if you needed that.
