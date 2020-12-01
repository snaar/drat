chopper is a streaming time series manipulation framework

##### Goals
1. Primary use case - ad-hoc usage by humans.
2. Production environment friendly:
    1. Single process - minimal cpu impact.
    2. Strictly streaming - minimal memory impact.
3. Time series focused:
    1. Input data is assumed to have time column.
    2. Time is monotonically non-decreasing within each of the inputs.

##### Notable design choices
1. Generated csv output is not guaranteed to be usable as csv input except for simple data types - there is no standard for csv serialization in general and csv output is expected to be used for either debugging or final output, not intermediate results.
