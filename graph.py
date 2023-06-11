import numpy as np
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Generate some data on the master process
if rank == 0:
    data = np.arange(22)
    print(data)
    chunk_size = len(data) // size
    remainder = len(data) % size
else:
    data = None
    chunk_size = None
    remainder = None

# Broadcast chunk size and remainder to all processes
chunk_size, remainder = comm.bcast((chunk_size, remainder), root=0)

# Divide the data into chunks and determine the sizes of the chunks on each process
if rank == 0:
    print(chunk_size, remainder)
    sendcounts = [chunk_size + remainder] + [chunk_size] * (size-1)
    displs = np.zeros(size, dtype=int)
    for i in range(1, size):
        displs[i] = displs[i-1] + chunk_size
    print(sendcounts, displs)
else:
    sendcounts = None
    displs = None

# Allocate memory for the chunk of data on each process
recv_data = np.empty(chunk_size + remainder if rank ==
                     0 else chunk_size, dtype=int)

# Scatter the data to each process
comm.Scatterv(data, (recv_data, sendcounts, displs), 0)

# Do something with the chunk of data on each process
print("Rank", rank, "received chunk of data:", recv_data)

 if rank == master:
        # print('test from 0')
        ready_to_start, leaves = preprocess_graph()
        # print('leaves :', leaves)
        while True:
            if len(leaves) == 0:
                for core in range(size):
                    if core != master:
                        tag_num = 1 + 10*core
                        data_to_send = np.array([-1])
                        comm.ssend(obj=data_to_send, dest=core, tag=tag_num)
                        cond = False
                break
            leaves_list = divide_leaves(leaves, size, master)
            leaves_len = [len(leaves_list[i]) for i in range(size)]
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

            leaves_list = divide_leaves(leaves, size, master)
            leaves_len = [len(leaves_list[i]) for i in range(size)]
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
                    comm.Probe(source=core, status=status)
                    count = status.Get_count(MPI.INT)
                    tag_num = 4 + 10*core
                    length = leaves_len[core]
                    buf = bytearray(4*count)
                    comm.Recv(buf, source=core, tag=tag_num)
                    new_index = np.frombuffer(buf[:4*length], dtype=np.int32)
                    new_rank = np.frombuffer(
                        buf[4*length:12*length], dtype=np.float64)
                    new_leaves = set(np.frombuffer(
                        buf[12*length:], dtype=np.int32))
                    # print(nodes_update)
                    # print(new_update)
                    index_update = np.concatenate(
                        (index_update, new_index), axis=None)
                    rank_update = np.concatenate(
                        (rank_update, new_rank), axis=None)
                    leaves = leaves | new_leaves
            # print('leaves :', leaves)
            comm.Barrier()
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
                index_bytes = index_update.tobytes()
                rank_bytes = rank_update.tobytes()
                leaves_bytes = np.array(list(leaves), dtype=np.int32).tobytes()
                # print(leaves_bytes)
                buf = index_bytes + rank_bytes + leaves_bytes
                # data_to_send = [nodes_update, leaves]
                comm.Ssend(buf, master, tag_num)
                # tag_num += 1
                # comm.ssend(obj=rank_update, dest=master, tag=tag_num)
                # tag_num += 1
                # comm.ssend(obj=leaves, dest=master, tag=tag_num)
        # print(rank, 'stopped')
            comm.Barrier()

    update_nodes(index_update, rank_update)