# benchspark

Typical distribute dataflow jobs for performance benchmarking.

Currently available Spark jobs (including dataset generators):

| Data Type |  Algorithm
| --- | ---
| Vector | KMeans |
| Vector | LinearRegression |
| Vector | LogisticRegression |
| Tabular | GroupByCount |
| Tabular | Join | Spark |
| Tabular | SelectWhereOrderBy |
| Text | Grep |
| Text | Sort |
| Text | WordCount |

Perhaps Flink equivalents will be added at some point

## Compilation

To compile the jobs to a jar file:

```bash
cd spark
sbt package
```

