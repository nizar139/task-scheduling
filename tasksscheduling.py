# %%
import json
import numpy as np
from mpi4py import MPI

# %%
def split_list(priority_list, n_cores):
    k, m = divmod(len(priority_list), n_cores)
    return list(priority_list[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n_cores))

# %%
def scheduling_parallel(priority_list, nb_cores, nodes):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    if rank == 0: #master a le rank 0
        list_split = split_list(priority_list, size)
        for i in range(1, size):
            comm.send(list_split[i], dest=i)
        scheduled_list = ordo(list_split[0], nb_cores, nodes)
        score = get_score_1(scheduled_list)
    else:
        sub_list = comm.recv(source=0)
        scheduled_list = ordo(sub_list, nb_cores, nodes)
        score = get_score_1(scheduled_list)

    relative_time = comm.gather(score, root=0)
    if rank==0:
        relative_time = np.cumsum(np.array(relative_time)) #relative_time[-1] est le score du schedule
    else:
        relative_time = np.zeros(size)
    comm.Bcast(relative_time, root = 0)
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
def ordo(tasks, n_cores, nodes):
    #calcule l'ordonnacement de la liste tasks
    def cores_occupied(end_times, time):
        bo = True
        for i in end_times:
            if time >= i:
                bo = False 
        return bo
        
        
        

    def fdone(time, schedule):
        done = []
        for i in range(n_cores):
            for task in schedule[i]:
                if task['end'] <= time:
                    done.append(task['Task'])
        return done


    def feasible(task, tasks, nodes, time):
        done = fdone(time, schedule)
        for father in nodes[str(task)]['Dependencies']:
            if father in tasks:
                if father not in done:
                    return False
        return True

    def affect_core(task, schedule, end_times, time):
        #met à jour le schedule et les end_times
        core = end_times.argmin()
        schedule[core].append({'Task' : task, 'start' : time, 'end' : time + nodes[str(task)]['Data']})
        end_times[core] = schedule[core][-1]['end']

    def unfeasible(end_times, time):
        #on cherche le end_time qui est le plus proche supérieur à time
        timeplus = np.inf
        ind = 0
        for i in range(n_cores):
            if time < end_times[i] and end_times[i] < timeplus:
                ind = i
                timeplus = end_times[i]
        return timeplus, end_times.argmin()
        

    end_times = np.zeros(n_cores)
    schedule = {i : [] for i in range(n_cores)}
    time = 0
    n = len(tasks)
    i = 0
    timestamp = []

    while i < n:
        if cores_occupied(end_times, time):
            time = end_times.min()
        elif feasible(tasks[i], tasks, nodes, time):
            affect_core(tasks[i], schedule, end_times, time)
            i += 1
        else:
            time_start = time
            time, core = unfeasible(end_times, time)
            timestamp.append((time_start, core))


    def intercale(schedule, time, task, core):
        #insert the task in the schedule of the core
        i = 0
        for task_visit in schedule[core]:
            if task_visit['end'] <= time:
                i += 1
        schedule[core] = schedule[core][:i] + [{'Task' : task, 'start' : time, 'end' : time + nodes[str(task)]['Data'] }] + schedule[core][i:]
    

    print(timestamp)
    for time, core in timestamp:
        i = 0
        time_aux = 0
        while schedule[core][i]['end'] < time:
            time_aux = schedule[core][i]['end']
            i += 1
    

        free_time = time - time_aux
        bo = False
        for core_visit in schedule:
            for task in schedule[core_visit]:
                if task['end'] - task['start'] < free_time and feasible(task['Task'], tasks, nodes, time) and task['start'] > time:
                    schedule[core_visit].remove(task)
                    intercale(schedule, time, task['Task'], core)
                    print(task, time)
                    bo = True
            
                if bo:
                    break
            if bo:
                break
    
    return schedule



with open('./graphs/smallRandom.json', 'r') as f:
    data = json.load(f)

nodes = data['nodes']




def change_type_Data(graph):
    for node in graph:
        if type(nodes[node]['Data']) == float:
            break
        pt = nodes[node]['Data'].split(':')
        nodes[node]['Data'] = float(pt[0])*3600 + float(pt[1])*60 + float(pt[2])
    
change_type_Data(nodes)

def get_score_1(schedule):
    score = 0
    for core_list in schedule.values() :
        if len(core_list)>0 :
            new_score = core_list[-1]['end']
            if new_score>score :
                score = new_score
    return score
# %%
print(scheduling_parallel([1,2,5,4,7,8,9,6,10,3], 2, nodes))


