import subprocess as sp
import time
import os

from jobs_config import get_all_job_args


package = "berlin.tu.dos.benchspark."

spark_submit = ['spark-submit']  # Adjust path based on your preferred installed spark version
conf_spec = []
# conf_spec =  ['--conf', 'spark.default.parallelism=1']  # Creates at most one output file (but also slower due to single core computation)
memory_spec =  ["--executor-memory", "4g", "--driver-memory", "4g"]  # Adjust based on your preference and system
# memory_spec = []


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
jobs_jar_location = parent_dir + "/spark/target/scala-2.12/benchspark_2.12-1.0.jar"  # local jar file
# experiments = f"hdfs://130.149.xxx.xxx:9000/c5-experiments/experiments_2.12-1.0.jar"  # remote jar file

user_home_dir = os.path.expanduser("~")
data_dir = user_home_dir + "/data/benchspark/" # Adjust path based on your preference
# data_dir = "hdfs://130.xxx.xxx.xxx:9000/sparkbenchmarks/data/"  # remote data_dir

args = get_all_job_args(data_dir)


# def run(algo, args, silent=True):
def run(algo, args, silent=False):

    print(f"\n{'@'*35} {algo} {'@'*35}")

    t1 = time.time()
    cmd = spark_submit+conf_spec+memory_spec+["--class", package+algo, jobs_jar_location]+args
    print(f"\n{' '.join(cmd)}\n")

    try:
        result = sp.check_output(cmd, stderr=(sp.STDOUT if silent else None)).decode('utf-8')
        return result
    except sp.CalledProcessError as e:
        print(e)
    finally:
        t2 = time.time()
        print(f"gross_runtime[seconds]:{t2-t1:.3f}")


algorithms = (
    'Grep',
    'GroupByCount',
    'Join',
    'KMeans',
    'LinearRegression',
    'LogisticRegression',
    'SelectWhereOrderBy',
    'Sort',
    'WordCount',
)

def generate_all_data():

    for algorithm in algorithms:
        run(f'{algorithm}DataGeneration', args[f'{algorithm}DataGeneration'][0])

def process_all_data():

    for algorithm in algorithms:
        run(algorithm, args[algorithm][0])

generate_all_data()
process_all_data()
