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
    path_end = 'graphs1\\{}.json'.format(names[file_num])
    file_path = os.path.join(parpath, path_end)
    # print('path :', file_path)
    # print(paths[0])
    with open(file_path, 'r') as f:
        data = json.load(f)
        nodes = data['nodes']
    return nodes


nodes = {}


def create_successors(nodes):
    nodes_cop = nodes.copy()
    ready_to_start = {}
    for node in nodes_cop:
        nodes_cop[node]['Successors'] = []
    for node in nodes_cop:
        depend = nodes_cop[node]['Dependencies']
        if len(depend) > 0:
            for i in depend:
                nodes_cop[str(i)]['Successors'].append(int(node))
        else:
            ready_to_start[int(node)] = 0
            # print(node)
    return nodes_cop, ready_to_start


def weight(data):
    pt = data.split(':')
    time_s = float(pt[0])*3600 + float(pt[1])*60 + float(pt[2])
    return time_s


def preprocess_graph(nodes):
    nodes_cop, ready_to_start = create_successors(nodes)
    leaves = set()
    for node in nodes_cop:
        data = nodes_cop[node].pop('Data')
        nodes_cop[node]['Weight'] = weight(data)
        nodes_cop[node]['finished'] = False
        length = len(nodes_cop[node]['Dependencies'])
        nodes_cop[node]['dep_count'] = length
        if length == 0:
            leaves.add(int(node))
    return nodes_cop, ready_to_start, leaves


def update_nodes(nodes, index_update, rank_update):
    node_copy = nodes.copy()
    n = len(index_update)
    for i in range(n):
        int_node = index_update[i]
        if int_node > 0:
            node = str(int_node)
            rank = rank_update[i]
            node_copy[node]['Rank'] = rank
    return node_copy


def calculate_rank_lvl(nodes, nodes_queue):
    rank_update = np.zeros(len(nodes_queue), dtype=np.float64)
    index_update = np.zeros(len(nodes_queue), dtype=np.int32)
    id_count = 0
    new_leaves = set()
    if len(nodes_queue) > 0:
        for node_number in nodes_queue:
            if node_number > 0:
                node = str(node_number)
                condition = False
                M = 0
                for succ in nodes[node]['Successors']:
                    if 'Rank' in nodes[str(succ)]:
                        u = nodes[str(succ)]['Rank']
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
    new_leaves = np.array(list(new_leaves), dtype=np.int32)
    return index_update, rank_update, new_leaves


def divide_leaves(leaves, n, master, p):
    count = master
    leaves_list = [set() for i in range(n)]
    for leave in leaves:
        id = count % n
        leaves_list[id].add(leave)
        count += 1
    if len(leaves_list) < p:
        leaves_list[:len(leaves)]
    return leaves_list


def find_counts(n, p, master):
    q, r = divmod(n, p)
    counts = [q]*p
    for i in range(r):
        counts[(master + i) % p] += 1
    return counts


def find_displs(p, counts):
    displs = [0]*p
    for i in range(p-1):
        displs[i+1] = displs[i] + counts[i]
    return displs


def make_priority_list(nodes):
    node_list = []
    rank_list = []
    for node in nodes:
        node_list.append(node)
        rank_list.append(nodes[node]['Rank'])

    sorted_nodes = [x for _, x in sorted(
        zip(rank_list, node_list), reverse=True)]
    return sorted_nodes


def main():
    master = 0
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()
    print('hi from', rank)

    # print(rank)

    rank_update = np.array([])
    index_update = np.array([])

    leaves_len = np.array([1], dtype=np.int16)

    if rank == master:
        loading_time = time.time()
    nodes = open_graph(5)
    nodes, ready_to_start, leaves = preprocess_graph(nodes)
    leaves = np.array(list(leaves), dtype=np.int32)
    leaves_len[0] = len(leaves)
    # print('after leaves', leaves)

    if rank == 0:
        start_time = time.time()
        print('loading time :', start_time-loading_time)
    else:
        leaves = None

    while leaves_len[0] > 0:
        if leaves_len[0] < size:
            index_update = np.empty(leaves_len[0], dtype=np.int32)
            rank_update = np.empty(leaves_len[0], dtype=np.float64)
            if rank == master:
                # print(rank, 'leaves to distribute', leaves)
                index_update, rank_update, new_leaves = calculate_rank_lvl(
                    nodes, leaves)
            comm.Bcast(index_update, root=master)
            comm.Bcast(rank_update, root=master)

            new_len = np.empty(1, dtype=np.int16)
            if rank == master:
                # print('indices', index_update)
                # print('ranks', rank_update)

                leaves = np.unique(new_leaves, return_counts=False)
                new_len = np.array([len(leaves)], dtype=np.int16)
            nodes = update_nodes(nodes, index_update, rank_update)
        else:
            sendcounts = find_counts(leaves_len[0], size, master)
            # print(rank, 'leaves to distribute', leaves)
            displs = find_displs(size, sendcounts)
            maxcount = int(np.ceil(leaves_len[0]/size))
            # print('maxcount', maxcount)
            leaves_buf = np.full(maxcount, 0, dtype=np.int32)
            comm.barrier()
            comm.Scatterv([leaves, sendcounts, displs, MPI.INT],
                          leaves_buf, master)
            # print(rank, 'leaves buff', leaves_buf)
            index_update, rank_update, new_leaves = calculate_rank_lvl(
                nodes, leaves_buf)
            # print(rank, 'found new elements', new_leaves)
            leaves_len_local = np.array([len(new_leaves)], dtype=np.int16)
            leaves_len_list = np.empty(size, dtype=np.int16)
            comm.Allgather(leaves_len_local, leaves_len_list)
            # print(rank, 'leaves len received', leaves_len_list)

            index_buf = np.empty(maxcount*size, dtype=np.int32)
            rank_buf = np.empty(maxcount*size, dtype=np.float64)

            disps = None
            if rank == master:
                leaves_buf = np.empty(np.sum(leaves_len_list), dtype=np.int32)
                disps = find_displs(size, leaves_len_list)
                # print('counts', leaves_len_list)
                # print('disps', disps)

            # comm.barrier()
            comm.Gatherv(new_leaves, [leaves_buf, leaves_len_list,
                                      disps, MPI.INT], root=master)
            # comm.barrier()
            comm.Allgather(index_update, index_buf)
            # comm.barrier()
            comm.Allgather(rank_update, rank_buf)

            new_len = np.empty(1, dtype=np.int16)
            if rank == master:
                # print('indices', index_buf)
                # print('ranks', rank_buf)

                leaves = np.unique(leaves_buf, return_counts=False)
                # print('new leaves to do', leaves_buf)
                # print('Leaves at iter', leaves)
                new_len = np.array([len(leaves)], dtype=np.int16)
            nodes = update_nodes(nodes, index_buf, rank_buf)

        comm.Bcast(new_len, root=master)
        # print(rank, 'new len received', new_len)
        leaves_len = np.copy(new_len)

    # print(rank, 'local nodes', nodes)
    # limit = 0
    # for node in nodes:
    #     if 'Rank' in nodes[node]:
    #         limit += 1
    #     else:
    #         print(rank, 'ranks not complete')
    #         break
    #     if limit == 10:
    #         print(rank, 'rank complete')
    #         break
    priority_list = make_priority_list(nodes)
    if rank == master:
        # print('nodes', nodes)
        print('ranks calculated')
        # print('nodes', nodes)
        duration = time.time() - start_time
        print('finished job, duration :', duration)
        first = priority_list[0]
        print('first element', first)
        print('rank of first', nodes[first])
        # print(priority_list)


if __name__ == "__main__":
    main()
