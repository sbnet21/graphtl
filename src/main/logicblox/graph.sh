#!/bin/bash

lb query prov "_(a,b) <- ans_node(a,b)." --csv
mv _.csv ans_node.csv
lb query prov "_(a,b,c) <- ans_edge(a,b,c)." --csv
mv _.csv ans_edge.csv

python createGraphJson.py
