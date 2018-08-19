drat is a simple streaming time series inspection and manipulation tool

##### Goals
1. Primary use case - ad-hoc usage by humans.
2. Production environment friendly:
    1. Single process - minimal cpu impact.
    2. Strictly streaming - minimal memory impact.
3. Time series focused:
    1. Input data is assumed to have time column.
    2. Time is monotonically non-decreasing within each of the inputs.
