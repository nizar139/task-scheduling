#!/usr/bin/env python3
import json

names = ['smallRandom','xsmallComplex','smallComplex','mediumRandom','MediumComplex','largeComplex','xlargeComplex','xxlargeComplex']
def create_path(names):
    paths = []
    for name in names : 
        path = './graphs/{}.json'.format(name)
        paths.append(path)
    return paths
paths = create_path(names)
print(paths[0])

print(paths)
file_num = 0

with open(paths[file_num], 'r') as f:
    data = json.load(f)
nodes = data['nodes']

def create_successors():
    ready_to_start = {}
    for node in nodes:
        nodes[node]['Successors']=[]
    for node in nodes :
        depend = nodes[node]['Dependencies']
        if len(depend)>0:
            for i in depend :
                nodes[str(i)]['Successors'].append(node)
        else :
            ready_to_start[node]=0
            # print(node)
    return ready_to_start

def weight(node):
    pt = node['Data'].split(':')
    time_s = float(pt[0])*3600 + float(pt[1])*60 + float(pt[2])
    return time_s

def add_ranks():
    for node in nodes:
        nodes[node]['rank'] = weight(nodes[node])
        nodes[node]['rank_calculated'] = False
        nodes[node]['finished'] = False

def calculate_rank(node):
    if not nodes[node]['rank_calculated']:
        succ = nodes[node]['Successors']     
        rank = weight(nodes[node])
        if len(succ) > 0:
            for i in succ:
                if not nodes[str(i)]['rank_calculated']:
                    # print("couldn't calculate rank")
                    return 1  
            weights = [nodes[str(i)]['rank'] for i in succ]
            # print('node weight:',rank)
            # print('successors:',succ)
            # print('succ weights:',weights)
            max_weights = max(weights)
            # print('max:',max_weights)
            rank += max_weights
        nodes[node]['rank'] = rank
        # print('calculated rank:', rank)
        nodes[node]['rank_calculated'] = True
    return 0

def graph_update():
    count = 0
    for node in reversed(nodes.keys()):
        # print('node:',node)
        count += calculate_rank(node)
    # print('not calculated ranks:',count)    
    return count

def calculate_ranks():
    epoch = 1
    count = 1
    while count > 0:
        # print('epoch:',epoch)
        count = graph_update()
        epoch+=1
    # print('rank calculation done')

def make_priority_list():
    node_list = []
    rank_list = []
    for node in nodes:
        node_list.append(node)
        rank_list.append(nodes[node]['rank'])

    # print(node_list)
    # print(rank_list)
    # indexes = np.flip(np.argsort(rank_list))
    # sorted_nodes = [node_list[i] for i in indexes]
    sorted_nodes = [x for _,x in sorted(zip(rank_list,node_list), reverse=True)]
    return sorted_nodes

def process_graph():
    ready_to_start = create_successors()
    add_ranks()
    calculate_ranks()
    priority_list = make_priority_list()
    return ready_to_start, priority_list
    

if __name__ == "__main__" :
    ready_to_start, priority_list = process_graph()
    print("priority list", priority_list)
    print("ready to start", ready_to_start)