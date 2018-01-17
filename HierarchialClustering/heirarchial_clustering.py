import sys
import math
import heapq
import copy

f = open(sys.argv[1])
k = int(sys.argv[2])
data = f.readlines()
input = []
for line in data:
    if line.strip()!= '':
        line = line.split(',')
        line = [i.strip() for i in line]
        line[0] = float(line[0])
        line[1] = float(line[1])
        line[2] = float(line[2])
        line[3] = float(line[3])
        input.append(line)

inputdict={}
for i in range(len(input)):
    inputdict[i]=[input[i],[i]]
n = len(inputdict)    

def findEuclideanDist(vectora,vectorb):
    x1=vectora[0][0]
    x2=vectorb[0][0]
    y1=vectora[0][1]
    y2=vectorb[0][1]
    z1=vectora[0][2]
    z2=vectorb[0][2]
    w1=vectora[0][3]
    w2=vectorb[0][3]
    diffx = x2-x1
    diffy = y2-y1
    diffz = z2-z1
    diffw = w2-w1
    Sum = (diffx**2)+(diffy**2)+(diffz**2)+(diffw**2)
    distance = math.sqrt(Sum)
    return distance

def findcentroid(vectora,vectorb):
    x1=vectora[0][0]
    x2=vectorb[0][0]
    y1=vectora[0][1]
    y2=vectorb[0][1]
    z1=vectora[0][2]
    z2=vectorb[0][2]
    w1=vectora[0][3]
    w2=vectorb[0][3]
    cluster1 = vectora[1]
    l1=len(cluster1)
    cluster2 = vectorb[1]
    l2 = len(cluster2)
    cluster1.extend(cluster2)
    length = len(cluster1)
    avgx = ((l1*x1)+(l2*x2))/length
    avgy = ((l1*y1)+(l2*y2))/length
    avgz = ((l1*z1)+(l2*z2))/length
    avgw = ((l1*w1)+(l2*w2))/length
    return [[avgx,avgy,avgz,avgw,""],cluster1]

originalinputdict = copy.deepcopy(inputdict)

while(len(inputdict)>k):
    heaplist = [] 
    distancedict = {}
    listt = []
    for a in range(len(originalinputdict)-1):
        for b in range(a+1,len(originalinputdict)):
            if a in inputdict and b in inputdict:
                vec1 = inputdict[a]
                vec2 = inputdict[b]
                ed = findEuclideanDist(vec1, vec2)
                if ed not in distancedict.keys():
                    distancedict[ed] = (a,b)
                    heapq.heappush(heaplist,ed)

    smallestdistance = heapq.heappop(heaplist)
    #coordinates to merge to one cluster
    coord = distancedict[smallestdistance]
    value1 = inputdict[coord[0]]
    value2 = inputdict[coord[1]]
    newcoord = findcentroid(value1,value2)
    del(inputdict[coord[0]])
    del(inputdict[coord[1]])
    inputdict[n]= newcoord
    originalinputdict[n] = newcoord
    n = n+1

file = open("Ravibalan_Vishnupriya_result.txt","w") 
wrong = 0
for each in inputdict:
    cluster = inputdict[each][1]
    names = {}
    answer = []
    for i in cluster:
        names[originalinputdict[i][0][4]] = names.get(originalinputdict[i][0][4],0)+1
        clustername = max(names,key=names.get)
        answer.append(originalinputdict[i][0])
    for j in names:
        if j!=clustername:
            wrong+=names[j]
    file.write('cluster:'+str(clustername)+'\n')
    for k in answer:
        file.write(str(k)+'\n')
    file.write('Number of points in this cluster:'+str(len(cluster))+'\n\n')
if wrong>0:
    file.write('Number of points wrongly assigned:'+str(wrong)+'\n')
file.close() 
    