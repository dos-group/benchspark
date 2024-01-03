# benchspark

An extensible tool set for Spark performance benchmarking.

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

## Compilation

To compile the jobs to a jar file:

```bash
cd spark
sbt package
```

## Getting Started

1. Adjust `run_scripts/submit_local_job` to your local setup and execute it.
2. Later you can extend the script to submit jobs to a cluster that is available to you, be that in a public cloud or an on-premise setup.

