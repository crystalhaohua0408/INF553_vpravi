from pyspark import SparkContext
import sys

sc = SparkContext(appName="inf553")

lines1=sc.textFile(sys.argv[1])
lines2= sc.textFile(sys.argv[2])
lines3=sc.textFile(sys.argv[3])

#mapphase for files user and rating
result1 = lines1.map(lambda line: line.split('::')).map(lambda x: (x[0],(x[1],x[2])))
result2 = lines2.map(lambda line: line.split('::')).map(lambda y: (y[0],y[1]))

#reducephase1 for files user and rating
Reduceoutput1 = result1.join(result2)
FinalsortedReduce1=Reduceoutput1.sortByKey(ascending=True)

#mapphase2 for files user and rating
values = FinalsortedReduce1.map(lambda (x,y): y)
mapphaseinput = values.map(lambda z: (z[0][0],(z[0][1],z[1]))).sortByKey(ascending=True)

#mapphase for file movies 
result3 = lines3.map(lambda line: line.split('::')).map(lambda k: (k[0],k[2]))

#reducephase 1 for files user,rating and movies
Reduceoutput2 = mapphaseinput.join(result3).sortByKey(ascending=True)

#mapphase2 for files user,rating and movies
values2 =  Reduceoutput2.map(lambda (x,y): y)
mapphaseresult2 = values2.map(lambda z:((z[1],z[0][1]),z[0][0])).sortByKey(ascending=True)

#reduce phase 2
FinalReduce2input = mapphaseresult2.groupByKey().sortByKey().map(lambda x: (x[0], map(float,list(x[1]))))
FinalReduce2output = FinalReduce2input.map(lambda (x,y): (x,(float(sum(y))/float(len(y)))))
Result = FinalReduce2output.map(lambda z: map(str,(z[0][0],z[0][1],z[1])))
Output = Result.collect()

#write into a textfile
op = open(sys.argv[4],"w")
for i in Output:
	line = "%s,%s,%s" % (i[0],i[1],i[2])
	op.write(line)
	op.write("\n")