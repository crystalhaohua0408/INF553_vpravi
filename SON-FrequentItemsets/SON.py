from pyspark import SparkContext
import itertools
import sys
from collections import defaultdict


sc = SparkContext(appName="inf553")

casevalue = int(sys.argv[1])

lines1=sc.textFile(sys.argv[2])
lines2= sc.textFile(sys.argv[3])
support = int(sys.argv[4])

def generatecandidate(frequent_sets,k):
    combine=set()
    if(k>2):
        for sets in frequent_sets:
            for s in sets:
                combine.add(s)
    elif(k==2):
        for sets in frequent_sets:
            combine.add(sets)
                
    sets = [set(sorted(x)) for x in itertools.chain(*[itertools.combinations(combine, k)])]
    return sets

def generatefrequentsets(candidates,sup,baskets):
    freq_one = dict()
    frequent_sets=set()
    for basket in baskets:
        for item in candidates:
            i = set(item)
            newset = set(basket);
            if i.issubset(newset):
                if i in freq_one.keys():
                    freq_one[frozenset(i)]+=1
                else:
                    freq_one[frozenset(i)] = 1
    for freq in freq_one:
        if(freq_one[freq]>=sup):
            frequent_sets.add(freq)
    return frequent_sets
            
def Apriori(chunk):
    frequent_sets=set()
    baskets = []
    freq_single = dict()
    b = 0
    for item in chunk:
        b+=1
        baskets.append(set(item)) 
        for i in item:
            if i in freq_single.keys():
                freq_single[i]+=1
            else:
                freq_single[i] = 1
    supp = (float(b)/float(n))*support
    for freq in freq_single:
        if(freq_single[freq]>=supp):
            frequent_sets.add(freq)
    k=2
    results = list(frequent_sets)
    while(len(frequent_sets)!=0):
        candidates = generatecandidate(frequent_sets,k)
        nextfrequent = generatefrequentsets(candidates,supp,baskets)
        if(len(nextfrequent)!=0):
            results.append(frozenset(nextfrequent))
        frequent_sets=nextfrequent
        k=k+1
    return iter(results)


result1 = lines1.map(lambda line: line.split('::')).map(lambda x: (x[0],x[1]))
result2 = lines2.map(lambda line: line.split('::')).map(lambda y: (y[0],y[1]))
Reduceoutput1 = result1.join(result2)
Joinoutput = Reduceoutput1.map(lambda x: (int(x[0]),(int(x[1][0]),str(x[1][1]))))
FinalsortedReduce1=Joinoutput.sortByKey(ascending=True)

if casevalue == 1:
    maleuser = FinalsortedReduce1.filter(lambda x: 'M' in x[1][1])
    newkvpair = maleuser.map(lambda y: (y[0],y[1][0]))
    ans = newkvpair.groupByKey().map(lambda x : (x[0], list(x[1]))).sortByKey(ascending=True)
    baskets=ans.map(lambda x:x[1]).collect()
    n = len(baskets)
    la = sc.parallelize(baskets).mapPartitions(Apriori).distinct().collect()
    frequent_items=[]
    for i in la:
        if type(i) != int:
            for j in i:
                frequent_items.append(frozenset(sorted(j)))
        else:
            frequent_items.append(frozenset({i}))
    frequent_items2 = sc.parallelize(frequent_items).map(lambda x: (x,1))
    reduce_one=frequent_items2.reduceByKey(lambda x,y:x)
    item_reduce=reduce_one.map(lambda (x,y): x).collect()
    broadcasting_global_count=sc.broadcast(item_reduce)
    map_two=sc.parallelize(baskets).flatMap(lambda line:[(count,1) for count in broadcasting_global_count.value if set(line).issuperset(set(count))])
    

elif casevalue == 2:
    femaleuser = FinalsortedReduce1.filter(lambda x: 'F' in x[1][1])
    #k,v pair is userID, movie ID of only female users
    newkvpair = femaleuser.map(lambda y: (y[1][0],y[0]))
    #grouping all the movies rated by one user
    ans = newkvpair.groupByKey().map(lambda x : (x[0], list(x[1]))).sortByKey(ascending=True)
    baskets=ans.map(lambda x:x[1]).collect()
    n = len(baskets)
    la = sc.parallelize(baskets).mapPartitions(Apriori).distinct().collect()
    frequent_items=[]
    for i in la:
        if type(i) != int:
            for j in i:
                frequent_items.append(frozenset(sorted(j)))
        else:
            frequent_items.append(frozenset({i}))
    frequent_items2 = sc.parallelize(frequent_items).map(lambda x: (x,1))
    reduce_one=frequent_items2.reduceByKey(lambda x,y:x)
    item_reduce=reduce_one.map(lambda (x,y): x).collect()
    broadcasting_global_count=sc.broadcast(item_reduce)
    map_two=sc.parallelize(baskets).flatMap(lambda line:[(count,1) for count in broadcasting_global_count.value if set(line).issuperset(set(count))])


reduce_two=map_two.reduceByKey(lambda x,y: x+y)
global_count=reduce_two.filter(lambda x: x[1]>=support)
output=global_count.map(lambda (x,y):sorted(x)).collect()
finalresult = defaultdict(list)
for item in output:
    k = len(item)
    finalresult[k].append(item)


op = open(sys.argv[5],"w")    
for i in finalresult.keys():
    finallist = sorted(finalresult[i])
    out = ""
    for j in finallist:
        if i == 1:
            out+="("+ str(j[0]) +"), "
        else:
            out+= str(tuple(j))+", "
    op.write(out[:-2]+"\n")

