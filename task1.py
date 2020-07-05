from pyspark import SparkContext, SparkConf
import time, sys, itertools
from graphframes import GraphFrame
from pyspark.sql import SparkSession




st = time.time()

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")


start = time.time()
threshold = sys.argv[1]
input = sys.argv[2]
output = sys.argv[3]

input_ = sc.textFile(input)
h = input_.first()
input_1 = input_.filter(lambda x: x != h)
input_1 = input_1.map(lambda x: (x.split(',')[0], [x.split(',')[1]])).reduceByKey(lambda x, y: x + y)

input_dictionary = input_1.collectAsMap()


def makepairs(x):
    return (itertools.combinations(x, 2))


edge_pairs = input_1.map(lambda x: (1, [x[0]])).reduceByKey(lambda x, y: x + y).flatMap(lambda x: makepairs(x[1])) \
    .filter(lambda x: len(set(input_dictionary[x[0]]).intersection(set(input_dictionary[x[1]]))) >= int(threshold)) \
    .flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])])

vertex = edge_pairs.flatMap(lambda x: [x[0], x[1]]).distinct()

edge_1 = edge_pairs.toDF(["src", "dst"])
vertex_1 = vertex.map(lambda x: (x, )).toDF(["id"])
graph_frame = GraphFrame(vertex_1, edge_1)

result = graph_frame.labelPropagation(maxIter=5)

final_result = result.rdd.coalesce(1) \
        .map(lambda x: (x[1], x[0])) \
        .groupByKey().map(lambda x: sorted(list(x[1]))) \
        .sortBy(lambda x: (len(x), x))


with open(output, 'w+') as output_file:
    for i in final_result.collect():
        output_file.writelines(str(i)[1:-1] + "\n")
    output_file.close()

print("time :",time.time()-st)

