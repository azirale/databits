# Cosmos Scaling Calculator

When doing bulk data transfers to Cosmos we want to maximise performance of the load while minimising overall running costs.
Cosmos has a minimum RU/s per the amount of data stored, so we want to predict the final data storage and determine sizing from there.

This module has methods to calculate how far we can scale out Cosmos without permanently increasing the minimum scale, either by providing the raw json bytes directly, or providing a Spark DataFrame and calculating the JSON representation of it.
