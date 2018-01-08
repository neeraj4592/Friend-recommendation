rdd = sc.textFile(“higgs-social_network.edgelist.from.LT.4000.txt”)

rdd1 = rdd.map(lambda x : (x.split()[0], x.split()[1]))

rdd2 = rdd1.map(lambda (x,y) : (x,[y])).reduceByKey(lambda x,y : x+y)

import follows
rdd3 = rdd2.flatMap( lambda x : follows.func(x[0], x[1]) )

# Making a list of followed by:
rdd4 = rdd3.filter(lambda x : x[1][0] == “followed_by”).map(lambda (x,y) : (x, (y[0], [y[1]]) )) .reduceByKey(lambda x,y : (x[0], x[1]+y[1]) )

# Combining the two rdds:
rdd5 = rdd3.filter(lambda x : x[1][0] == “follows”).union(rdd4)

rdd6 = rdd5.groupByKey().mapValues(lambda x : list(x))

# Calling the recommend function to emit recommendations in (key,value) pairs:
rdd7 = rdd6.map(lambda x : follows.recommend(x[0], x[1]))

# Making a list of all the recommendations for each key:
rdd8 = rdd7.filter(lambda x : x!=0).flatMap(lambda x : x).map(lambda (x,y) : (x,[y])) .reduceByKey(lambda x,y : x+y)

# Filtering according to the desired output keys:
rdd9 = rdd8.map(lambda x : ((x[0], “should_follow”), list(set(x[1]))) ).filter(lambda x : x[0][0]==”1” or x[0][0]==”27” or x[0][0]==”31” or x[0][0]==”137” or x[0][0]==”3113”)

# Taking only first three recommendations:
rdd10 = rdd9.mapValues(lambda x : x[:3])