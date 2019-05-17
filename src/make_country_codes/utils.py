import os
import random
import hashlib
from functools import reduce
from contextlib import contextmanager

from luigi import LocalTarget
from luigi import Parameter
from luigi.local_target import atomic_file
from salted.salted_demo import get_salted_version
from luigi.task import logger as luigi_logger

replacements = (u'\xa0', u''), (u'\n', u''), (u'\r', u'')


def clean(word):
    reduce(lambda a, kv: a.replace(*kv), replacements, word)
    return " ".join(word.split())


def bytes_pls(some_val):
    """Convenience function that returns bytes when given str or bytes

    :param str or bytes some_val: thing desired as bytes

    :rtype: bytes
    """
    if isinstance(some_val, bytes):
        return some_val
    return some_val.encode()


def convert_numeric_code_with_pad(x):
    """
    Codes like M49 and ISO 3166 numeric should be
    treated as strings, as their leading zeros are
    meaningful and must be preserved.
    """
    try:
        return str(int(x)).zfill(3)
    except ValueError:
        return ''

def convert_numeric_code(x):
    """
    Some numeric codes should be treated as strings,
    but don't need leading 0s
    """
    try:
        return str(int(x))
    except ValueError:
        return ''


class sha256sum:
    """
        Hacky descriptor used for salting upstream data.

        After fetching/scraping and saving an upstream dataset,
        we then use hash of file contents as version salt in target filenames.

        Our salting tasks simply create a copy of the file with a new
        filename that includes salt. Since file contents are identical, we
        can use both/either the hash of file contents of the salted target
        and/or the hash of the file contents of salted target's requires()

        In cases where the salted target's required task is not complete,
        descriptor returns a placeholder ('tk') so luigi will know to run the task.
    """
    def __get__(self, task, cls):
        if task is None:
            return self
        if task.requires().get('source').complete():
            return get_salt_for_source(task.requires())
        else:
            return 'tk'

    def __call__(self, task):
        """Returns the salt (chars of sha256 checksum) of task's output file

        :returns: first six chars of sha256 hexdigest of contents of `task.output()`
        :rtype: str
        """
        return get_salt_for_task(task)


def get_salt_for_source(task):
    checksum = hashlib.sha256()
    # TODO read and hash in chunks

    source = task.get('source').output()
    with open(source.path, 'rb') as f:
        checksum.update(bytes_pls(f.read()))
    return checksum.hexdigest()[:6]


class TargetOutput:
    def __init__(self, file_pattern='{task.__class__.__name__}',
                 ext='.txt', base_dir='data/', target_class=LocalTarget, **target_kwargs):
        self.file_pattern = file_pattern
        self.ext = ext
        self.base_dir = base_dir
        self.target_class = target_class
        self.target_kwargs = target_kwargs

    def __get__(self, task, cls):
        if task is None:
            return self
        return lambda: self(task)

    def __call__(self, task):
        target_path = self.base_dir + self.file_pattern.format(task=task) + self.ext
        return self.target_class(target_path, **self.target_kwargs)


class SaltedOutput(TargetOutput):
    def __init__(self, file_pattern='{task.__class__.__name__}-{salt}',
        ext='.txt', base_dir='data/', target_class=LocalTarget, **target_kwargs):
        self.file_pattern = file_pattern
        self.ext = ext
        self.base_dir = base_dir
        self.target_class = target_class
        self.target_kwargs = target_kwargs

    def __call__(self, task):
        if hasattr(task, 'salt'):
            # if the task has its own salt, use it
            salt = task.salt
        else:
            # otherwise compute based on task graph versions
            salt = get_salted_version(task)[:6]
        target_path = self.base_dir + self.file_pattern.format(task=task, salt=salt) + self.ext
        return self.target_class(target_path, **self.target_kwargs)


def salted_SPLT(task, file_pattern, format=None, **kwargs):
    """A local target with a file path formed with a 'salt' kwarg

    :rtype: SuffixPreservingLocalTarget
    """
    return SuffixPreservingLocalTarget(file_pattern.format(
        salt=get_salted_version(task)[:6], self=task, **kwargs
    ), format=format)


class suffix_preserving_atomic_file(atomic_file):
    def generate_tmp_path(self, path):
        root, ext = os.path.splitext(path)
        rand = random.randrange(0, 1e10)
        return  f'{path}-luigi-tmp-{rand}{ext}'


class BaseAtomicProviderLocalTarget(LocalTarget):
    # Allow some composability of atomic handling
    atomic_provider = atomic_file

    def open(self, mode='r'):
        # leverage super() as well as modifying any code in LocalTarget
        # to use self.atomic_provider rather than atomic_file
        rwmode = mode.replace('b', '').replace('t', '')
        if rwmode == 'w':
            self.makedirs()
            return self.format.pipe_writer(self.atomic_provider(self.path))
        super(LocalTarget, self).open(mode=mode)

    @contextmanager
    def temporary_path(self):
        # NB: unclear why LocalTarget doesn't use atomic_file in its implementation
        self.makedirs()
        with self.atomic_provider(self.path) as af:
            yield af.tmp_path


class SuffixPreservingLocalTarget(BaseAtomicProviderLocalTarget):
    atomic_provider = suffix_preserving_atomic_file


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

        return {key : getattr(task, key) for key in dir(task.__class__)
                if isinstance(getattr(task.__class__, key), Requirement)}


class Requirement:
    def __init__(self, task_class, **params):
        self.task_class = task_class
        self.params = params

    def __get__(self, task, cls):
        if task is None:
            return self

        if self.params:
            for v in self.params.values():
                if isinstance(v, Parameter):
                    return task.clone(self.task_class, **task.to_str_params())
        return task.clone(self.task_class, **self.params)
