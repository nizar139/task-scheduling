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

    list_split = split_list(priority_list, size)
    if rank == 0: #master a le rank 0
        for i in range(1, size):
            comm.send(list_split[i], dest=i)
        scheduled_list = ordo(list_split[0], nb_cores)
        score = get_score_1(scheduled_list)
        res = {'schedule' : scheduled_list,'score': score, 'rank': rank}
    else:
        sub_list = comm.recv(source=0)
        scheduled_list = ordo(sub_list, nb_cores)
        score = get_score_1(scheduled_list)
        res = {'schedule' : scheduled_list,'score': score, 'rank': rank}

    scheduled_dict = comm.gather(res, root=0)
    if rank == 0:
        schedule = {}
        
        print(scheduled_dict)
        relative_time = np.zeros(size) #calcul des temps de fin de chaque schedule pour actualiser les temps start et end de chaque tache en fonction du rang du processeur qui les a ordonné
        for x in scheduled_dict:
            relative_time[x['rank']]= x['score']
        relative_time = np.cumsum(relative_time) #relative_time[-1] est le score du schedule
        for x in scheduled_dict: #actualisation des temps Start et End    
            if x['rank']>0:
                x['schedule']['Start'] +=relative_time[x['rank']-1]
                x['schedule']['End'] +=relative_time[x['rank']-1]
            schedule.update(x['schedule']) #merge chaque dictionnaire
        return schedule

# %%
def ordo(tasks, n_cores):
    #calcule l'ordonnacement de la liste tasks

def get_score_1(l):
    #calcule le score de la liste l
# %%
#scheduling_parallel([1,2,3], 2)


