import graphviz
from datetime import datetime

import json

with open('./graphs/smallRandom.json', 'r') as f:
    data = json.load(f)

nodes = data['nodes']


def weight(node):
    pt = node['Data'].split(':')
    time_s = float(pt[0])*3600 + float(pt[1])*60 + float(pt[2])
    return time_s


node = nodes['1']

for node in nodes:
    nodes[node]['rank'] = 0
    nodes[node]['rank_calculated'] = False


def calculate_rank(node):
    if not nodes[node]['rank_calculated']:
        depend = nodes[node]['Dependencies']
        for i in depend:
            if not nodes[str(i)]['rank_calculated']:
                return 1
        weights = [weight(nodes[str(i)]) for i in depend]
        print(nodes[node])
        rank = weight(nodes[node])
        if len(weights) > 0:
            rank += max(weights)
        nodes[node]['rank'] = rank
        nodes[node]['rank_calculated'] = True
    return 0


def graph_update():
    count = 0
    for node in nodes:
        count += calculate_rank(node)
    return count


def calculate_ranks():
    count = 1
    while count > 0:
        count = graph_update()
    print('rank calculation done')
    for node in nodes.values():
        print(node['rank'])


def priority_list():
    n = len(nodes)
    node_list = []
    rank_list = []
    for node in nodes:
        node_list.append(node)
        rank_list.append(nodes[node]['rank'])
