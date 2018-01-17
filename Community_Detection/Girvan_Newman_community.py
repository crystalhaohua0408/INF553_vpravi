import networkx as nx
from pyspark import SparkContext
from networkx import number_connected_components
from networkx import connected_components
import copy
import itertools
import math
import sys

G = nx.Graph()
G1 = nx.Graph()

sc = SparkContext(appName="inf553")
input = sc.textFile(sys.argv[1]).map(lambda line:[(line.strip().split(',')[0]),(line.strip().split(',')[1])]).collect()
edgelist = []
userdict = {}
for i in range(1,len(input)):
    userid = int(input[i][0])
    movieid = int(input[i][1])
    if userid not in userdict:
        userdict[userid] = set()
    userdict[userid].add(movieid)

#Find Edges
for i in itertools.combinations(userdict.keys(), 2):
    if len(userdict[i[0]].intersection(userdict[i[1]])) >= 3:
        edgelist.append(tuple(sorted(i)))

G.add_edges_from(edgelist)
G1.add_edges_from(edgelist)

def betweenness(G,sourcenode,Betweennessdict):
    predicted ={}
    new = {}
    bfstrees = []
    Q = [sourcenode]
    val = dict.fromkeys(G,0.0)
    val[sourcenode] = 1.0
    new[sourcenode] = 0
    for i in G:
        predicted[i] = []
    while Q:
        v =Q.pop(0)
        c = new[v]
        valv = val[v]
        bfstrees.append(v)
        for childnode in G[v]:
            if childnode not in new:
                Q.append(childnode)
                new[childnode] = c + 1
                
            if new[childnode] is c + 1: 
                predicted[childnode].append(v) 
                val[childnode] += valv

    edge_contribution = dict.fromkeys(bfstrees, 0)
    while bfstrees:
        n = bfstrees.pop() 
        weights = (1.0 + edge_contribution[n]) / val[n]

        for parent in predicted[n]:
            children = val[parent] * weights 
            edge =  (parent, n) 
            if edge in Betweennessdict:
                Betweennessdict[edge] += children
            else:
                Betweennessdict[tuple(reversed(edge))] += children              
            edge_contribution[parent] += children

    return Betweennessdict

#Find Betweeness values
Betweennessdict = dict.fromkeys(G.edges(), 0.0)
for source in G:
    Betweennessdict = betweenness(G,source,Betweennessdict)

Betweennessdict.update((k,v/2.0) for k, v in Betweennessdict.items())
order = sorted(Betweennessdict.keys())

#Print Betweenness values to file
op = open(sys.argv[2], 'w')
order = sorted(Betweennessdict.keys())
output_betweenness = []
for i in order:
    output_betweenness.append((i[0], i[1],(Betweennessdict[i]//0.1)/10))
output_betweenness = sorted(output_betweenness, key = lambda x: (x[0], x[1]))

for j in output_betweenness:
    op.write("("+str(j[0])+","+str(j[1])+","+str(j[2])+")")
    op.write("\n")
op.close()

def findedges(userdict):  
    for i in userdict.keys():
        for j in userdict.keys():
            if(i!=j):
                movieIDlist1 = userdict[i]
                moviedIDlist2 = userdict[j]
                val = set(movieIDlist1).intersection(moviedIDlist2)
                if (len(val)>=3):
                    edgelist.append((i,j));


def findmodularity(G,Betweendict):
    communities = connected_components(G)
    communities = list(communities)
    nnodes = G.number_of_nodes()
    nedges = G.number_of_edges()
    subterm = 0;
    degreedict = {}
    communitiesdict = {}
    for c in range(len(communities)):
        for j in communities[c]:
            communitiesdict[j] = c;

    for i in range(1,nnodes+1):
        degreedict[i] = G.degree(i);

    for each in range(1,nnodes+1):
        for value in range(1,nnodes+1):
            if (communitiesdict[each] == communitiesdict[value]):
                if ((each,value) in Betweendict) or ((value,each) in Betweendict):
                # if tuple(sorted([each,value])) in Betweendict:
                    A = 1;
                else:
                    A = 0;
                degreeofeach = degreedict[each]
                degreeofvalue = degreedict[value]
                degree = degreeofvalue*degreeofeach
                subterm += A-((degree*0.5)/nedges)
             
    modularity = (0.5*subterm)/nedges
    return modularity,communities

#Round values in betweennessdict
for i in sorted(Betweennessdict.keys()):
    Betweennessdict[i] = (Betweennessdict[i]//0.1)/10


#Find modularity and get communities
lmod = [];
ansmod = 0
while(len(Betweennessdict)>1000):
    ansdict = {}
    deledges = sorted(Betweennessdict, key=Betweennessdict.get, reverse=True)
    maxvalue = Betweennessdict[deledges[0]]
    newdellist = []
    for i in deledges:
        if Betweennessdict[i] == maxvalue:
            newdellist.append(i)
    for e in newdellist:
        Betweennessdict.pop(e,None)
    G1.remove_edges_from(newdellist)
    if (G1.number_of_edges() == 0):
        modularity = 0
    else:
        modularity,communities = findmodularity(G1,Betweennessdict)
        if modularity > ansmod:
            ansmod = modularity
            anscommunity = communities

#print modularities to file
op1 = open(sys.argv[3], 'w')
communitylist = []
for i in anscommunity:
    communitylist.append(sorted(list(i)))
for j in sorted(communitylist):
    op1.write(str(j))
    op1.write("\n")
op1.close()




