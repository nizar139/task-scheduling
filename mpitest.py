from mpi4py import MPI
import json
import numpy as np
import time


def open_graph(file_num):
    names = ['smallRandom', 'xsmallComplex', 'smallComplex', 'mediumRandom',
             'MediumComplex', 'largeComplex', 'xlargeComplex', 'xxlargeComplex']
    paths = []
    for name in names:
        path = './graphs1/{}.json'.format(name)
        paths.append(path)
    # print(paths[0])
    with open(paths[file_num], 'r') as f:
        data = json.load(f)
        nodes = data['nodes']
    return nodes


nodes = open_graph(5)


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
    if len(nodes_queue) > 0:
        for node_number in nodes_queue:
            node = str(node_number)
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
            new_leaves = new_leaves | set(nodes[node]['Dependencies'])

    return update, new_leaves


def divide_leaves(leaves, n, master):
    count = master + 1
    leaves_list = [set() for i in range(n)]
    # leaves_copy = leaves.copy()
    for leave in leaves:
        id = count % n
        leaves_list[id].add(leave)
        count += 1

    return leaves_list


def make_priority_list():
    node_list = []
    rank_list = []
    for node in nodes:
        node_list.append(node)
        rank_list.append(nodes[node]['Rank'])

    # print(node_list)
    # print(rank_list)
    # indexes = np.flip(np.argsort(rank_list))
    # sorted_nodes = [node_list[i] for i in indexes]
    sorted_nodes = [x for _, x in sorted(
        zip(rank_list, node_list), reverse=True)]
    return sorted_nodes


def main():
    master = 0
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # print(rank)

    nodes_update = dict()

    if rank == master:
        # print('test from 0')
        start_time = time.time()
        ready_to_start, leaves = preprocess_graph()
        # print('leaves :', leaves)
        while True:
            if len(leaves) == 0:
                for core in range(size):
                    if core != master:
                        tag_num = 1 + 10*core
                        data_to_send = 'Stop'
                        comm.send(obj=data_to_send, dest=core, tag=tag_num)
                break

            leaves_list = divide_leaves(leaves, size, master)
            # print('list of leaves', leaves_list)
            nodes_queue = leaves_list[0]
            for core in range(size):
                if core != master:
                    tag_num = 1 + 10*core
                    data_to_send = [nodes_update, leaves_list[core]]
                    comm.send(obj=data_to_send, dest=core, tag=tag_num)
                # req.wait()
            update_nodes(nodes_update)
            nodes_update, leaves = calculate_rank_lvl(nodes_queue)
            for core in range(size):
                if core != master:
                    tag_num = 2 + 10*core
                    new_update, new_leaves = comm.recv(
                        source=core, tag=tag_num)
                    nodes_update.update(new_update)
                    leaves = leaves | new_leaves
            # print('leaves :', leaves)

    else:
        preprocess_graph()
        while True:
            # print(rank, 'getting data from master')
            tag_num = 1 + 10*rank
            data_received = comm.recv(source=master, tag=tag_num)
            if data_received == 'Stop':
                # print(rank, 'received stop')
                break
            # print('{} got data'.format(rank))
            nodes_update, nodes_queue = data_received
            update_nodes(nodes_update)
            nodes_update, leaves = calculate_rank_lvl(nodes_queue)
            tag_num += 1
            data_to_send = [nodes_update, leaves]
            comm.send(obj=data_to_send, dest=master, tag=tag_num)
        # print(rank, 'stopped')

    update_nodes(nodes_update)

    if rank == master:
        print('ranks calculated')
        # print('nodes', nodes)

        priority_list = make_priority_list()
        duration = time.time() - start_time
        print('finished job, duration :', duration)
        # first = priority_list[0]
        # print(first)
        # print(nodes[first])
        # print(priority_list)
        # return (priority_list)


if __name__ == "__main__":
    main()
