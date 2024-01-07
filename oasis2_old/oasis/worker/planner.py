from copy import deepcopy

from oasis.db.models import model_query
from oasis.db.models.task import TaskModel
from oasis.worker.tasks import BaseTask


class Planner:
    def __init__(self, task_graph=None):
        self.task_graph = {} if not task_graph else task_graph
        self._arrangement = []

    # task_graph_1 = {
    #     'A': ['D', 'E'],
    #     'B': ['D', 'E'],
    #     'C': ['D', 'E'],
    #     'D': ['F'],
    #     'E': ['G'],
    #     'F': ['G'],
    #     'G': [],
    # }
    def set_task_graph(self, task_graph):
        self.task_graph = task_graph

    def generate_task_graph(self, task_list: [BaseTask]):
        self.task_graph = {task: task.next_tasks
                           for task in task_list}

    def arrange_tasks(self):
        task_graph = deepcopy(self.task_graph)
        task_stack = []
        while task_graph:
            tmp_task_list = [k for k, v in task_graph.items() if not v]
            for tmp_task in tmp_task_list:
                task_graph.pop(tmp_task)
            task_stack.append(tmp_task_list)
            for k, v in task_graph.items():
                task_graph[k] = [v_task for v_task in v if v_task not in tmp_task_list]

        task_stack.reverse()
        self._arrangement = task_stack
        return self._arrangement

    def get_head_tasks(self):
        task_graph = deepcopy(self.task_graph)
        head_tasks = []
        while task_graph:
            tmp_task_list = [k for k, v in task_graph.items() if not v]
            if not tmp_task_list:
                return head_tasks
            for tmp_task in tmp_task_list:
                task_graph.pop(tmp_task)
            head_tasks.extend(tmp_task_list)
            for k, v in task_graph.items():
                for v_task in v:
                    if v_task in tmp_task_list and v_task in head_tasks:
                        head_tasks.remove(v_task)
                task_graph[k] = [v_task for v_task in v if v_task not in tmp_task_list]

        return head_tasks


async def save_task_graph(job_id, new_task_graph):
    """
    Because task id is only generated while commit to db,
    we should save next task id from tail to head.
    Before task id is generated, use id(task) for tmp use.
    """
    task_tmp_id_dict = {
        id(new_task): new_task for new_task in new_task_graph.keys()
    }

    task_tmp_graph = {}
    for new_task, new_task_next in new_task_graph.items():
        tmp_next_list = [id(tmp_next) for tmp_next in new_task_next]
        task_tmp_graph.setdefault(id(new_task), tmp_next_list)

    planner = Planner(task_tmp_graph)
    arrangements = planner.arrange_tasks()
    # print('arrangement, ', arrangements)

    task_name_id_dict = {}
    while arrangements:
        arr = arrangements.pop()
        for a_id in arr:
            a = task_tmp_id_dict.get(a_id)
            a.job_id = job_id
            a.next_tasks = [task_name_id_dict.get(next_task_id)
                            for next_task_id in task_tmp_graph.get(a_id)]
            await a.save()
            task_name_id_dict.setdefault(a_id, a.id)


async def get_next_tasks_from_db(job_id):
    query = model_query(TaskModel)
    res = await query.where(TaskModel.job_id == job_id).where(
        TaskModel.status != TaskModel.STATUS.Done).query_all()
    if len(res) == 0:
        return 'All Done'
    task_status_dict = {task.id: task.status for task in res}
    for status in task_status_dict.values():
        if status in [TaskModel.STATUS.Failed, TaskModel.STATUS.Rolling]:
            return []
    task_graph = {task.id: task.next_tasks for task in res}
    planner = Planner(task_graph)
    next_tasks = planner.get_head_tasks()
    # Rolled tasks can re-send after error is fixed
    next_tasks = [task for task in next_tasks
                  if task_status_dict.get(task, '') in [TaskModel.STATUS.Init, TaskModel.STATUS.Rolled]]
    return next_tasks


def get_rolling_back_tasks(task_status_graph):
    while task_status_graph:
        next_rollback_tasks = []
        done_tasks = []
        tmp_task_list = [(k, v.get('status')) for k, v in task_status_graph.items()
                         if not v.get('next_tasks', None)]

        for tmp_task, tmp_task_status in tmp_task_list:
            task_status_graph.pop(tmp_task)
            if tmp_task_status in [TaskModel.STATUS.Doing, TaskModel.STATUS.Rolling]:
                return []
            elif tmp_task_status in [TaskModel.STATUS.Failed, TaskModel.STATUS.Done]:
                next_rollback_tasks.append(tmp_task)
            elif tmp_task_status in [TaskModel.STATUS.Rolled, TaskModel.STATUS.Init]:
                done_tasks.append(tmp_task)
            else:
                return []

        if next_rollback_tasks:
            return next_rollback_tasks

        # The final level of graph, nothing to roll
        if not task_status_graph:
            return 'All Rolled'

        for k, v in task_status_graph.items():
            task_status_graph[k] = {
                'status': v.get('status'),
                'next_tasks': [v_task for v_task in v.get('next_tasks', []) if v_task not in done_tasks]
            }


async def get_next_rollbacks_from_db(job_id):
    query = model_query(TaskModel)
    res = await query.where(TaskModel.job_id == job_id).query_all()
    task_status_graph = {task.id: {'next_tasks': task.next_tasks,
                                   'status': task.status} for task in res}
    next_tasks = get_rolling_back_tasks(task_status_graph)
    return next_tasks
