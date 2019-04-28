import os
import shutil
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


REMOTE_FILE_SOURCES = {
    'unterm': 'https://protocol.un.org/dgacm/pls/site.nsf/files/Country%20Names%20UNTERM2/$FILE/UNTERM%20EFSRCA.xlsx',
    'iso4166': 'https://www.currency-iso.org/dam/downloads/lists/list_one.xml',
    'cldr': 'https://raw.githubusercontent.com/unicode-cldr/cldr-localenames-full/master/main/en/territories.json',
    'geonames': 'http://download.geonames.org/export/dump/countryInfo.txt',
}

SCRAPE_SOURCES = {
    'edgar': 'https://www.sec.gov/edgar/searchedgar/edgarstatecodes.htm',
    'm49': 'https://unstats.un.org/unsd/methodology/m49/overview/',
}


class Salt:
    def __get__(self, task, cls):
        if task is None:
            return self
        return get_salt_for_task(task.requires())

    def __call__(self, task):
        """Returns the salt (chars of sha256 checksum) of task's output file

        :returns: first six chars of sha256 hexdigest of contents of `task.output()`
        :rtype: str 
        """
        return get_salt_for_task(task)


def get_salt_for_task(task):
    checksum = hashlib.sha256()
    # TODO read and hash in chunks
    with task.output().open('r') as f:
        checksum.update(f.read())
    return checksum.hexdigest()[:6]


class FileSource(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/'

    source = Parameter()
    ext = Parameter()

    pattern = '{task.__class__.__name__}-{task.source}{task.ext}'
    output = TargetOutput(file_pattern=pattern, ext='',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget,
                          format=format.Nop)

    def run(self):
        url = REMOTE_FILE_SOURCES.get(self.source)
        luigi_logger.debug(url)

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with self.output().open('w') as f:
                for chunk in r.iter_content(chunk_size=128):
                    f.write(chunk)


class SaltedLocalFile(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/'

    source = Parameter()
    ext = Parameter()
    salt = Salt()

    def requires(self):
        return FileSource(source=self.source, ext=self.ext)

    pattern = '{task.__class__.__name__}-{task.source}-{task.salt}{task.ext}'
    output = TargetOutput(file_pattern=pattern, ext='',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget,
                          format=format.Nop)

    def run(self):
        salt = get_salt_for_task(self.requires())
        self.requires().output().copy(self.output().path)


class FileSources(WrapperTask):

    def requires(self):
        for slug, url in REMOTE_FILE_SOURCES.items():
            _, ext = os.path.splitext(url)
            yield SaltedLocalFile(source=slug, ext=ext)
