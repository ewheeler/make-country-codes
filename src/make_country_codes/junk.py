from luigi.task import logger as luigi_logger
from salted.salted_demo import get_salted_version


class Requires:
    """Composition to replace :meth:`luigi.task.Task.requires`
    """

    def __get__(self, task, cls):
        if task is None:
            return self

        # Bind self/task in a closure
        return lambda : self(task)

    def __call__(self, task):
        """Returns the requirements of a task

        Assumes the task class has :class:`.Requirement` descriptors, which
        can clone the appropriate dependences from the task instance.

        :returns: requirements compatible with `task.requires()`
        :rtype: dict
        """
        """
        #props = type(task).__dict__.items()
        props = dir(type(task))

        #params = {k: getattr(task, k) for k,v in task.to_str_params().items()}
        #luigi_logger.debug(params)
        """
        requirements = {k: v for k,v in type(task).__dict__.items() if isinstance(v, Requirement)}
        luigi_logger.debug(requirements)

        req_dict = {k: getattr(task, k) for k,v in requirements.items()}
        luigi_logger.debug(req_dict)
        """
        obj = task
        luigi_logger.debug(obj)
        #luigi_logger.debug(dir(obj))
        req = {k: getattr(obj, k) for k in dir(obj) if isinstance(getattr(obj, k), Requirement)}
        luigi_logger.debug(req)

        req_dict = {k: v for k, v in req.items()}
        luigi_logger.debug(req_dict)


        req_dict = {k: getattr(task, k) for k in dir(type(task)) if isinstance(k, Requirement)}
        req_dict = {k: getattr(task, k) for k in dir(task) if k in requirements}
        req_dict = {k: v.task_class(**params) for k,v in props if isinstance(v, Requirement)}
        req_dict = {k: getattr(v, 'params') for k,v in requirements.items()}
        import pdb;pdb.set_trace()
        for name, req in requirements.items():
            p = {}
            r = getattr(task, name)
            for k, v in params.items():
                p.update({k: getattr(r, k)})
            req_dict.update({name: req.task_class(**p)})
        """
        return req_dict


class Requirement:
    def __init__(self, task_class, **params):
        self.task_class = task_class
        self.params = params

    def __get__(self, task, cls):
        if task is None:
            return self
        luigi_logger.debug('cloning')

        return task.clone(
            self.task_class,
            **self.params)
