import itertools
import sys
import time
from collections import deque, OrderedDict, defaultdict
import heapq
from pyspark.sql import SparkSession

st = time.time()

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

start = time.time()
threshold = sys.argv[1]
input = sys.argv[2]
output = sys.argv[3]
output_1 = sys.argv[4]

input_ = sc.textFile(input)
h = input_.first()
input_1 = input_.filter(lambda x: x != h)
input_1 = input_1.map(lambda x: (x.split(',')[0], [x.split(',')[1]])).reduceByKey(lambda x, y: x + y)

input_dictionary = input_1.collectAsMap()

dic_1 = defaultdict(set)
dict_11 = {}
dic_2 = defaultdict(set)
dict_22 = {}


def makepairs(x):
    return (itertools.combinations(x, 2))


edge_pairs = input_1.map(lambda x: (1, [x[0]])).reduceByKey(lambda x, y: x + y).flatMap(lambda x: makepairs(x[1])) \
    .filter(lambda x: len(set(input_dictionary[x[0]]).intersection(set(input_dictionary[x[1]]))) >= int(threshold))

M = edge_pairs.count()

twoM = 2*M
print("this is twoEM",twoM)

edges_1 = edge_pairs.flatMap(lambda x: [(x[0], {x[1]}), (x[1], {x[0]})]).reduceByKey(lambda x, y: x.union(y))

edge_li_1 = edges_1.collect()

for ii in edge_li_1:
    dict_11[ii[0]] = ii[1]
    dict_22[ii[0]] = ii[1]


edges = edge_pairs.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])])

edge_li = edges.collect()

for jj in edge_li:
    dic_1[jj[0]].add(jj[1])
    dic_2[jj[0]].add(jj[1])

degree = {}
for i in dic_1:
    value = float(len(dic_1[i]))
    degree[i] = value

vertex_1 = edge_pairs.flatMap(lambda x: [x[0], x[1]]).distinct()

vetex = vertex_1.count()


def countbeetweenness(reverse_li, mapping_, node_count_1, leaf_1):       # -------------------bottom up
    default_values = defaultdict(float)

    final_result = []
    for jj in reverse_li:
        temp = mapping_[jj]
        for part in temp:
            if jj not in leaf_1:
                numerator = ((default_values[jj] + 1) * node_count_1[part])
                denominator = node_count_1[jj]
                result_1 = numerator / denominator  # slide 15-16
            else:
                numerator = node_count_1[part]
                denominator = node_count_1[jj]
                result_1 = numerator/denominator
            default_values[part] += result_1
            final_result.append((tuple(sorted([jj, part])), result_1))

    return final_result


def BFS(root):
    queue = deque()
    leaf = []
    mapping_dic = defaultdict(list)
    node_count = defaultdict(float)
    queue.appendleft(root)
    visited = OrderedDict()  # to maintian graph feature need ordered dict
    visited[root] = True
    node_count[root] = 1
    while len(queue) != 0:
        n = len(queue)
        same_level = []
        for i in range(n):  # To find which nodes are at same level
            node = queue.pop()
            same_level.append(node)
        for node in list(set(same_level)):  # Iterate through that nodes
            visited[node] = True
            pool_of_childrens = dic_1[node]
            flag = 0
            for ii in pool_of_childrens:  # chilllllllllllllldrennnnnnnnnnnnnnnnn
                if (ii not in same_level) and (ii not in visited.keys()):  # slide 15
                    mapping_dic[ii].append(node)
                    node_count[ii] += node_count[node]  # ---------parent's
                    queue.appendleft(ii)
                    flag = 1
            if flag == 0:
                leaf.append(node)

    return countbeetweenness(reversed(list(visited)), mapping_dic, node_count, list(set(leaf)))


bet_result = vertex_1.flatMap(lambda x: BFS(x)).reduceByKey(lambda x, y: x + y)\
    .map(lambda x: (x[0], x[1] / 2)).sortBy(lambda x: (-x[1], x[0][0], x[0][1]))       # ------------piazza post # #406

with open(output, 'w+') as output_file:
    for i in bet_result.collect():
        output_file.write(str(i[0]) + ", " + str(i[1]) + "\n")
    output_file.close()

# ______________________________________________________________________________________________________________________

betweeness_calculation = bet_result
this_community = []
community = []
reserv = {}
rr = 0
# Looping over by removing edges and finding best modularity
while 1:
    rr+=1
    max_ = betweeness_calculation.max(key=lambda x: x[1])
    val1 = max_[0][0]
    val2 = max_[0][1]

    a1 = list(dic_1[val1])
    a1.remove(val2)
    dic_1[val1] = a1
    b2 = list(dic_1[val2])
    b2.remove(val1)
    dic_1[val2] = b2

    this_community = []  # ----------------- get all nodes in one communitiy
    visited_node = {}
    for x in dic_1:                                        # ------------- normal BFS
        if x not in visited_node.keys():
            ans_this = set()
            queue_1 = deque()
            visited_node[x] = True
            queue_1.appendleft(x)

            while len(queue_1) != 0:
                point_1 = queue_1.pop()
                ans_this.add(point_1)
                visited_node[point_1] = True
                for jj in dic_1[point_1]:
                    if jj not in visited_node.keys():
                        queue_1.appendleft(jj)
            if ans_this is not None:
                this_community.append(ans_this)

    value_ = 0
    for ij in this_community:  # ----------------------check moduliarity by the equation given in the slides/assignment
        KI = 0
        KJ = 0
        AIJ = 0
        for i in ij:
            KI = degree[i]
            for j in ij:
                KJ = degree[j]
                if j in dic_2[i]:
                    AIJ = 1
                else:
                    AIJ = 0
                value_ += AIJ - float((KI * KJ) / twoM)
    value_1 = value_ / twoM  # ----------------------- normalizing factor

    if len(this_community) == vetex:  # breaking condition
        break

    reserv[value_1] = this_community # -----------saving all the iteration with key : moduliarity and value : community

    rdd = sc.parallelize(dic_1.keys())
    betweeness_calculation = rdd.flatMap(lambda x: BFS(x)).reduceByKey(lambda x, y: x + y).map(
        lambda x: (x[0], x[1] / 2))  # -----------------------------check betweenness again (piazza post)

output_11 = []

ll = max(list(reserv.keys()))
community = reserv[ll]

print("Modularity",ll)
print("NUmber of iteration",rr)

for i in range(len(community)):
    community[i] = sorted(community[i])
community.sort(key=lambda x: (len(x), x))

with open(output_1, "w+") as output_fie_write:
    for i in community:
        ll_1 = i
        for j in range(len(ll_1) - 1):
            output_fie_write.write(str("\'" + ll_1[j] + "\'") + ", ")
        output_fie_write.write(str("\'" + ll_1[-1] + "\'"))
        output_fie_write.write("\n")
end = time.time()

print("Duration:", time.time() - start)
