from mpi4py import MPI
import json
import numpy as np
import time
import os.path
path = os.path.realpath(__file__)
parpath = os.path.join(path, os.pardir)


def open_graph(file_num):

    names = ['smallRandom', 'xsmallComplex', 'smallComplex', 'mediumRandom',
             'MediumComplex', 'largeComplex', 'xlargeComplex', 'xxlargeComplex']
    path_end = 'graphs\\{}.json'.format(names[file_num])
    file_path = os.path.join(parpath, path_end)
    # print('path :', file_path)
    # print(paths[0])
    with open(file_path, 'r') as f:
        data = json.load(f)
        nodes = data['nodes']
    return nodes


loading_time = time.time()
nodes = open_graph(3)


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
            leaves.add(int(node))
    return ready_to_start, leaves


def update_nodes(index_update, rank_update):
    n = len(index_update)
    for i in range(n):
        int_node = index_update[i]
        if int_node > 0:
            node = str(int_node)
            rank = rank_update[i]
            nodes[node]['Rank'] = rank
    # for node in update:
    #     nodes[node] = update[node].copy()


def calculate_rank_lvl(nodes_queue):
    rank_update = np.zeros(len(nodes_queue), dtype=np.float32)
    index_update = np.zeros(len(nodes_queue), dtype=np.int32)
    id_count = 0
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
            index_update[id_count] = int(node)
            rank_update[id_count] = nodes[node]['Weight'] + M
            id_count += 1
            new_leaves = new_leaves | set(nodes[node]['Dependencies'])

    return index_update, rank_update, new_leaves


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
    load_time = time.time()-loading_time
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    print('hi from', rank)

    # print(rank)

    rank_update = np.array([])
    index_update = np.array([])

    if rank == master:
        # print('test from 0')
        print('loading time :', load_time)
        start_time = time.time()
        ready_to_start, leaves = preprocess_graph()
        # print('leaves :', leaves)
        while True:
            if len(leaves) == 0:
                for core in range(size):
                    if core != master:
                        tag_num = 1 + 10*core
                        data_to_send = np.array([-1])
                        comm.ssend(obj=data_to_send, dest=core, tag=tag_num)
                break

            leaves_list = divide_leaves(leaves, size, master)
            # print('list of leaves', leaves_list)
            nodes_queue = leaves_list[0]
            for core in range(size):
                if core != master:
                    tag_num = 1 + 10*core
                    # data_to_send = [nodes_update, leaves_list[core]]
                    # comm.send(obj=data_to_send, dest=core, tag=tag_num)
                    comm.ssend(obj=index_update, dest=core, tag=tag_num)
                    tag_num += 1
                    comm.ssend(obj=rank_update, dest=core, tag=tag_num)
                    tag_num += 1
                    comm.ssend(obj=leaves_list[core], dest=core, tag=tag_num)
                # req.wait()
            update_nodes(index_update, rank_update)
            index_update, rank_update, leaves = calculate_rank_lvl(nodes_queue)
            # print('from master', nodes_update)
            for core in range(size):
                if core != master:
                    tag_num = 4 + 10*core
                    new_index = comm.recv(source=core, tag=tag_num)
                    tag_num += 1
                    new_rank = comm.recv(source=core, tag=tag_num)
                    tag_num += 1
                    new_leaves = comm.recv(source=core, tag=tag_num)
                    # print(nodes_update)
                    # print(new_update)
                    index_update = np.concatenate(
                        (index_update, new_index), axis=None)
                    rank_update = np.concatenate(
                        (rank_update, new_rank), axis=None)
                    leaves = leaves | new_leaves
            # print('leaves :', leaves)

    else:
        preprocess_graph()
        while True:
            # print(rank, 'getting data from master')
            tag_num = 1 + 10*rank
            data_received = comm.recv(source=master, tag=tag_num)
            if len(data_received) == 1 and data_received[0] == -1:
                # print(rank, 'received stop')
                break
            else:

                # print('{} got data'.format(rank))
                index_update = data_received
                tag_num += 1
                rank_update = comm.recv(source=master, tag=tag_num)
                tag_num += 1
                nodes_queue = comm.recv(source=master, tag=tag_num)
                # print(nodes_queue)
                update_nodes(index_update, rank_update)
                index_update, rank_update, leaves = calculate_rank_lvl(
                    nodes_queue)
                # print('from slave', nodes_update)
                tag_num += 1
                # data_to_send = [nodes_update, leaves]
                comm.ssend(obj=index_update, dest=master, tag=tag_num)
                tag_num += 1
                comm.ssend(obj=rank_update, dest=master, tag=tag_num)
                tag_num += 1
                comm.ssend(obj=leaves, dest=master, tag=tag_num)
        # print(rank, 'stopped')

    update_nodes(index_update, rank_update)

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
