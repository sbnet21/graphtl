# read file
# write file several times

import subprocess
import sys

query_edge = [
    "_(a,b,c)<-edge0(a,b,c).",
    "_(a,b,c)<-ans_edge(a,b,c)."
]
query_node = [
    "_(a,b)<-node0(a,b).",
    "_(a,b)<-ans_node(a,b)."
]

outputs = [
    "origin", "ans"
]

def genGraphviz(query_edge, query_node, outputfile):
    fw = open('temp.gv', 'w')
    fw.write("digraph G {\n")

    # node
    cmd = ['/Users/sbnet21/Tools/logicblox-4.10.0/bin/lb', 'query', 'prov', query_node]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE) # input=input)
    out, err = p.communicate()

    outs = out.split("\n")

    shape="box"
    style="filled"
    fillcolor="lightgray"

    for i in range(1,len(outs)-2): # remove the first and the last line
        row = outs[i].split()
        fw.write(row[0])
        #fw.write("\""+str(row[0])+"L"+str(row[1])+"\"")
        #row[2] = row[2].replace("\"","")

        # if (row[3] == "3"): #"ACTIVITY"):
        #     shape="box"
        #     style="filled"
        #     fillcolor="deepskyblue"
        # elif (row[3] == "2"): # "ENTITY"):
        #     shape="ellipse"
        #     style="filled"
        #     fillcolor="yellow"
        # elif (row[3] == "1"): #"AGENT"):
        #     shape="house"
        #     style="filled"
        #     fillcolor="orange"
        
        fw.write(" [shape="+shape+",style="+style+",fillcolor="+fillcolor+"];\n")

    fw.write("\n")
    # edge
    cmd = ['/Users/sbnet21/Tools/logicblox-4.10.0/bin/lb', 'query', 'prov', query_edge]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE) # input=input)
    out, err = p.communicate()

    outs = out.split("\n")

    style="bold"
    color="blue"
    label="-"

    for i in range(1,len(outs)-2): # remove the first and the last line
        row = outs[i].split()
        #fw.write("\""+str(row[0])+"L"+str(row[1])+"\""+"->"+"\""+str(row[2])+"L"+str(row[3])+"\"")
        fw.write(row[0]+"->"+row[1])
        #row[2] = row[2].replace("\"","")
        # if (row[4] == "12"): #"USED"):
        #     style="bold"
        #     color="red"
        #     label="USED"
        # elif (row[4] == "11"): #"WAS_ASSOCIATED_WITH"):
        #     style="bold"
        #     color="blue"
        #     label="WAS_ASSOCIATED_WITH"
        # elif (row[4] == "13"): #"WAS_GENERATED_BY"):
        #     style="bold"
        #     color="pink"
        #     label="WAS_GENERATED_BY"
        # elif (row[4] == "14"): #"WAS_DERIVED_FROM"):
        #     style="bold"
        #     color="green"
        #     label="WAS_DERIVED_FROM"
        
        fw.write(" [style="+style+",color="+color+",label="+row[2]+"];\n")

    fw.write("}")
    fw.close()

    # run graphviz
    cmd = ['/usr/local/bin/dot', '-Tpdf', 'temp.gv', '-o', outputfile+".pdf"]
    p = subprocess.Popen(cmd) # input=input)
    out, err = p.communicate()





for i in range(0, len(query_edge)):
    genGraphviz(query_edge[i], query_node[i], "graph/graph"+outputs[i])

