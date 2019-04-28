import os
import hashlib
from functools import update_wrapper

from luigi import format
from luigi import LocalTarget
from luigi import Parameter
from luigi import Task
from luigi import WrapperTask
from luigi.task import logger as luigi_logger

import requests
import pandas as pd

from pset_utils.luigi.task import Requires
from pset_utils.luigi.task import Requirement
from pset_utils.luigi.task import TargetOutput


FILE_SOURCES = {
    'unterm': 'https://protocol.un.org/dgacm/pls/site.nsf/files/Country%20Names%20UNTERM2/$FILE/UNTERM%20EFSRCA.xlsx',
    'iso4166': 'https://www.currency-iso.org/dam/downloads/lists/list_one.xml',
    'cldr': 'https://raw.githubusercontent.com/unicode-cldr/cldr-localenames-full/master/main/en/territories.json',
    'geonames': 'http://download.geonames.org/export/dump/countryInfo.txt',
}

SCRAPE_SOURCES = {
    'edgar': 'https://www.sec.gov/edgar/searchedgar/edgarstatecodes.htm',
    'm49': 'https://unstats.un.org/unsd/methodology/m49/overview/',
}

class wraprepr:
    def __init__(self, new_repr, func):
        self._repr = new_repr
        self._func = func
        update_wrapper(self, func)

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def __repr__(self):
        return self._repr(self._func)


class Salt:
    def __get__(self, task, cls):
        if task is None:
            return self

        # Bind self/task in a closure
        # ...
        # and wrap with another to override its  __repr__
        # why? since we cannot hash a file's contents until we have the file,
        # our descriptor's __repr__ will be returned by `task.salt` and used
        # as part of `file_pattern` for the temporary file. this would look
        # like this: data/FileSource-iso4166-<function Salt.__get__.<locals>.<lambda> at 0x117b086a8>
        # which is a valid but odd looking filename.
        # however, since our descriptor binds self/task in a closure each time
        # that `__get__` is executed, the lambda function's hash/identity used by
        # __repr__ may not be the same, which means that self.output().move()
        # cannot find a matching temporary filename
        # TODO should we memoize or cache these instead of wrapping?
        return wraprepr(lambda x: "<function: Salt>", lambda x: self(task))

    def __call__(self, task):
        """Returns the salt (chars of sha256 checksum) of task's output file

        :returns: first six chars of sha256 hexdigest of contents of `task.output()`
        :rtype: str 
        """
        checksum = hashlib.sha256()
        # TODO read and hash in chunks
        with task.output().open('r') as f:
            checksum.update(f.read())
        return checksum.hexdigest()[:6]

    def __repr__(self):
        return "<function Salt.__get__.<locals>.<lambda>...>"


class FileSource(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/'

    source = Parameter()
    salt = Salt()

    pattern = '{task.__class__.__name__}-{task.source}-{task.salt}'
    output = TargetOutput(file_pattern=pattern, ext='',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget,
                          format=format.Nop)

    def run(self):
        url = FILE_SOURCES.get(self.source)

        luigi_logger.debug(url)

        root, ext = os.path.splitext(url)

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with self.output().open('w') as f:
                for chunk in r.iter_content(chunk_size=128):
                    f.write(chunk)

        checksum = hashlib.sha256()
        with self.output().open('r') as f:
            checksum.update(f.read())
        salt = checksum.hexdigest()[:6]

        self.output().move(f'{self.DATA_ROOT}{self.__class__.__name__}-{self.source}-{salt}{ext}')


class FileSources(WrapperTask):
    def requires(self):
        for slug in FILE_SOURCES.keys():
            yield FileSource(slug)
