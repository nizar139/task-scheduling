from mpi4py import MPI
import json
import numpy as np
# import time


def open_graph(file_num):
    names = ['smallRandom', 'xsmallComplex', 'smallComplex', 'mediumRandom',
             'MediumComplex', 'largeComplex', 'xlargeComplex', 'xxlargeComplex']
    paths = []
    for name in names:
        path = './graphs1/{}.json'.format(name)
        paths.append(path)
    print(paths[0])
    with open(paths[file_num], 'r') as f:
        data = json.load(f)
        nodes = data['nodes']
    return nodes


nodes = open_graph(0)


def create_successors():
    ready_to_start = {}
    for node in nodes:
        nodes[node]['Successors'] = []
    for node in nodes:
        depend = nodes[node]['Dependencies']
        if len(depend) > 0:
            for i in depend:
                nodes[str(i)]['Successors'].append(node)
        else:
            ready_to_start[node] = 0
            # print(node)
    return ready_to_start


def weight(node):
    pt = node['Data'].split(':')
    time_s = float(pt[0])*3600 + float(pt[1])*60 + float(pt[2])
    return time_s


def preprocess_graph():
    ready_to_start = create_successors()
    leaves = set()
    for node in nodes:
        nodes[node]['Weight'] = weight(nodes[node])
        nodes[node]['finished'] = False
        lenth = len(nodes[node]['Successors'])
        nodes[node]['Succ_count'] = lenth
        if lenth == 0:
            leaves.add(node)
    return ready_to_start, leaves


def update_nodes(update):
    for node in update:
        nodes[node] = update[node].copy()


def calculate_rank_lvl(nodes_queue):
    update = dict()
    new_leaves = set()
    for node in nodes_queue:
        condition = False
        M = 0
        for succ in nodes[node]['Successors']:
            if 'Rank' in nodes[succ]:
                u = nodes[succ]['Rank']
                if u > M:
                    M = u
            else:
                condition = True
                break
        if condition:
            continue
        update[node] = nodes[node].copy()
        update[node]['Rank'] = nodes[node]['Weight'] + M
        new_leaves = new_leaves + set(nodes[node]['Dependencies'])

    return update, new_leaves


def divide_leaves(leaves, n):
    q = len(leaves)//n
    count = 0
    leaves_list = [list() for i in range(n)]
    for leave in leaves:
        id = count % q
        if id < n:
            leaves_list[id].add(leave)
        else:
            leaves_list[n-1].add(leave)
        count += 1
    return leaves_list


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
    sorted_nodes = [x for _, x in sorted(
        zip(rank_list, node_list), reverse=True)]
    return sorted_nodes


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    ready_to_start, leaves = preprocess_graph()
    nodes_update = dict()
    while len(leaves) > 0:
        if rank == 0:
            leaves_list = divide_leaves(leaves, rank)
            nodes_queue = leaves_list[0]
            for core in len(1, size):
                data_to_send = [nodes_update, leaves_list[core]]
                comm.send(obj=data_to_send, dest=core)

        else:
            nodes_update, nodes_queue = comm.recv(source=0)
        update_nodes(nodes_update)
        nodes_update, leaves = calculate_rank_lvl(nodes_queue)

        if rank != 0:
            data_to_send = [nodes_update, leaves]
            comm.send(obj=data_to_send, dest=0)

        else:
            for core in range(1, size):
                new_update, new_leaves = comm.recv(source=core)
                nodes_update.update(new_update)
                leaves = leaves + new_leaves

    update_nodes(nodes_update)

    priority_list = make_priority_list()
    print(priority_list)
    return (priority_list)


if __name__ == "__main__":
    main()
