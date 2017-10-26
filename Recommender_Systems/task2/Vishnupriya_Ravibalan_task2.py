from pyspark.sql.functions import col
from pyspark import SparkContext
from pyspark.sql import SQLContext
import sys
import math

sc = SparkContext(appName="inf553")
sqlc = SQLContext(sc)

df = sc.textFile(sys.argv[1]).map(lambda line:[(line.strip().split(',')[0]),(line.strip().split(',')[1]),(line.strip().split(',')[2])])
header = df.first()
full_df = df.filter(lambda line:line!=header).toDF()
full_df = full_df.select(col("_1").alias('userID'),col("_2").alias('movieID'),col("_3").alias('ratings'))
df_1 = sc.textFile(sys.argv[2],6).map(lambda l:[(l.strip().split(',')[0]),(l.strip().split(',')[1])])
header = df_1.first()
test_df = df_1.filter(lambda line:line!=header).toDF()
test_df = test_df.select(col("_1").alias('userID'),col("_2").alias('movieID'))
la = test_df.join(full_df,["userID","movieID"])
la.sort("userID")
training_df = full_df.subtract(la).sort("userID")
training_rdd = training_df.rdd.map(list)
training_data = training_rdd.map(lambda x: (int(x[0]),(int(x[1]),float(x[2]))))
vals = training_data.groupByKey().map(lambda y:(y[0],list(y[1]))).sortByKey(ascending=True).map(lambda g:g[1]).collect()

def cosine_similarity(v1,v2):
    "compute cosine similarity of v1 to v2: (v1 dot v2)/{||v1||*||v2||)"
    sumxx, sumxy, sumyy = 0, 0, 0
    for i in range(len(v1)):
        x = v1[i]; y = v2[i]
        sumxx += x*x
        sumyy += y*y
        sumxy += x*y
    if(sumxy != 0):
        return float(sumxy)/float(math.sqrt(sumxx*sumyy))
    else:
        return 0

def predictionvalues(weights,ratings,avgs,useraverage):
    sumvals = 0
    new = []
    for k in range(len(weights)):
        new.append([weights[k],ratings[k],avgs[k]])
    new.sort(key = lambda tup:tup[0],reverse = True)
    nweights = []
    nratings = []
    navgs = []
    nbh = int(0.2*len(weights))
    for i in new[:nbh]:
        nweights.append(i[0])
        nratings.append(i[1])
        navgs.append(i[2])
    absweights = [abs(x) for x in nweights ]
    if (sum(absweights) == 0):
        return useraverage
    else:
        for k in range(len(nweights)):
            sumvals += (nratings[k]-navgs[k])*nweights[k]
        prediction = useraverage+(float(sumvals)/float(sum(absweights)))
        return prediction

uservals = []
for user in vals:
    userdict = {}
    for x in range(len(user)):
        movieID = user[x][0]
        rating = user[x][1]
        userdict[movieID]=rating
    uservals.append(userdict)
weightsdict = {}
avgdict = {}
for i in range(len(uservals)):
    for j in range(i+1,len(uservals)):
        keys1 = set(uservals[i])
        keys2 = set(uservals[j])
        ratings1 = 0
        ratings2 = 0
        array1 = []
        array2 = []
        intersect = keys1.intersection(keys2) 
        for mid in intersect:
            ratings1 += uservals[i][mid]  
            array1.append(uservals[i][mid])
            ratings2 += uservals[j][mid]
            array2.append(uservals[j][mid])
        if (len(intersect)==0):
            weightsdict[i,j]=0
            avgdict[i,j]=[0,0]
            continue
        avg1 = float(ratings1)/float(len(intersect))
        avg2 = float(ratings2)/float(len(intersect))
        avgdict[i,j]=[avg1,avg2]
        array1[:]=[x-avg1 for x in array1]
        array2[:]=[x-avg2 for x in array2]
        cosine=cosine_similarity(array1,array2)
        weightsdict[i,j]=cosine
moviedict = {}
for user in range(len(uservals)):
    for movie in uservals[user]:
        if movie in moviedict:
            moviedict[movie].append(user)
        else:
            moviedict[movie] = [user]

def predict(x):
    user = x[0]-1
    lofm = x[1]
    pdict = []
    useraverage = float(sum(uservals[user].values()))/float(len(uservals[user]))
    for mid in lofm:
        if mid in moviedict:
            otherusers = moviedict[mid]
            weights = []
            ratings = []
            avgs = []
            for each in otherusers:
                if (user,each) in weightsdict:
                    weights.append(weightsdict[(user,each)])
                    avgs.append(avgdict[user,each][1])
                else:
                    weights.append(weightsdict[(each,user)])
                    avgs.append(avgdict[each,user][0])
                ratings.append(uservals[each][mid])
            prediction = predictionvalues(weights,ratings,avgs,useraverage)
            if prediction > 5:
                prediction = 5
            elif prediction < 0:
                prediction = 0
            pdict.append((mid,prediction))
        else:
            pdict.append((mid,useraverage))
    return [user+1,pdict]       

testing_rdd = test_df.rdd.map(list)
testing_data = testing_rdd.map(lambda l:(int(l[0]),int(l[1])))
testvals = testing_data.groupByKey().map(lambda y:(y[0],list(y[1]))).sortByKey(ascending=True)
final = testvals.map(lambda x:predict(x)).collect()
ratingcompare = [0,0,0,0,0]
la_rdd = la.rdd.map(list)
la_data = la_rdd.map(lambda x: (int(x[0]),(int(x[1]),float(x[2]))))
la_vals = la_data.groupByKey().map(lambda y:(y[0],sorted(list(y[1])))).sortByKey(ascending=True).collect()
se = []
finallist = []

for i in range(len(final)):
    for j in range(len(final[i][1])):
        finallist.append(((i+1,final[i][1][j][0]),final[i][1][j][1]))
final_rdd = sc.parallelize(finallist).sortByKey(ascending=True).collect()

op = open(sys.argv[3],"w")    
header = "userID,movieID,rating"  
op.write(header+"\n")
for i in final_rdd:
    out = ""
    out+=str(i[0][0])+","+str(i[0][1])+","+str(i[1])
    op.write(out+"\n")

for i in range(len(final)):
    for j in range(len(final[i][1])):
        rating = abs(final[i][1][j][1]-la_vals[i][1][j][1])
        se.append(math.pow(rating,2))
        if rating >= 0 and rating < 1:
            ratingcompare[0] += 1
        elif rating >=1 and rating < 2:
            ratingcompare[1] += 1
        elif rating >=2 and rating < 3:
            ratingcompare[2] += 1
        elif rating >=3 and rating < 4:
            ratingcompare[3] += 1
        elif rating >= 4:
            ratingcompare[4] += 1

print ">=0 and <1: ",ratingcompare[0]
print ">=1 and <2: ",ratingcompare[1]
print ">=2 and <3: ",ratingcompare[2]
print ">=3 and <4: ",ratingcompare[3]
print ">=4: ",ratingcompare[4]

rmse = math.pow(float(sum(se))/float(len(se)),-2)
print "RMSE =",rmse








