from pyspark import SparkContext
import sys

sc = SparkContext(appName="inf553")

lines1=sc.textFile(sys.argv[1])
lines2= sc.textFile(sys.argv[2])

#map phase 1
result1 = lines1.map(lambda line: line.split('::')).map(lambda x: (x[0],(x[1],x[2])))
result2 = lines2.map(lambda line: line.split('::')).map(lambda y: (y[0],y[1]))

#reduce phase 1
Reduceoutput1 = result1.join(result2)
FinalsortedReduce1=Reduceoutput1.sortByKey(ascending=True)

#map phase 2
values = FinalsortedReduce1.map(lambda (x,y): y)
mapphaseinput = values.map(lambda z: (z[0][0],z[0][1],z[1]))
mapphaseoutput = mapphaseinput.map(lambda x: ((int(x[0]),x[2]),x[1]))

#reducephase2
reduceinput = mapphaseoutput.groupByKey().sortByKey().map(lambda x: (x[0], map(float,list(x[1]))))
Result = reduceinput.map(lambda (x,y): (x,(float(sum(y))/float(len(y)))))
Final = Result.map(lambda z: map(str,(z[0][0],z[0][1],z[1])))
Output = Final.collect()

#write into a textfile
op = open(sys.argv[3],"w")
for i in Output:
	line = "%s,%s,%s" % (i[0],i[1],i[2])
	op.write(line)
	op.write("\n")