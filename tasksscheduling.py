# %%
import numpy as np
from mpi4py import MPI

# %%
def split_list(priority_list, n_cores):
    k, m = divmod(len(priority_list), n_cores)
    return list(priority_list[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n_cores))

# %%
def scheduling_parallel(priority_list, nb_cores):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    if rank == 0: #master a le rank 0
        list_split = split_list(priority_list, size)
        for i in range(1, size):
            comm.send(list_split[i], dest=i)
        scheduled_list = ordo(list_split[0], nb_cores)
        score = get_score_1(scheduled_list)
    else:
        sub_list = comm.recv(source=0)
        scheduled_list = ordo(sub_list, nb_cores)
        score = get_score_1(scheduled_list)

    relative_time = comm.gather(score, root=0)
    if rank==0:
        relative_time = np.cumsum(np.array(relative_time)) #relative_time[-1] est le score du schedule
    comm.broadcast(relative_time)
    if rank > 0:
        time = relative_time[rank-1] #calcul des temps de fin de chaque schedule pour actualiser les temps start et end de chaque tache en fonction du rang du processeur qui les a ordonné
        for x in scheduled_list:
            for y in scheduled_list[x]:
                y['start']+=time
                y['end']+=time
    scheduled_dict= comm.gather(scheduled_list, root=0)
    if rank == 0:
        schedule ={i: [] for i in range(nb_cores)}
        for x in scheduled_dict: #actualisation des temps Start et End    
            for i in range(nb_cores):
                schedule[i] += x[i]
        return schedule

# %%
def ordo(tasks, n_cores):
    return None
    #calcule l'ordonnancement de la liste tasks

def get_score_1(schedule):
    score = 0
    for core_list in schedule.values() :
        if len(core_list)>0 :
            new_score = core_list[-1]['end']
            if new_score>score :
                score = new_score
    return score
# %%
#scheduling_parallel([1,2,3], 2)


