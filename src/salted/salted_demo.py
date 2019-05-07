# Copyright 2018 Scott Gorlin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A demo implementation of a Salted Graph workflow"""

from hashlib import sha256

from luigi import LocalTarget, format
from luigi.task import flatten


def get_salted_version(task):
    """Create a salted id/version for this task and lineage

    :returns: a unique, deterministic hexdigest for this task
    :rtype: str
    """

    msg = ""

    # Salt with lineage
    for req in flatten(task.requires()):
        # Note that order is important and impacts the hash - if task
        # requirements are a dict, then consider doing this is sorted order
        msg += get_salted_version(req)

    # Uniquely specify this task
    msg += ','.join([

            # Basic capture of input type
            task.__class__.__name__,

            # Change __version__ at class level when everything needs rerunning!
            task.__version__,

        ] + [
            # Depending on strictness - skipping params is acceptable if
            # output already is partitioned by their params; including every
            # param may make hash *too* sensitive
            '{}={}'.format(param_name, repr(task.param_kwargs[param_name]))
            for param_name, param in sorted(task.get_params())
            if param.significant
        ]
    )
    return sha256(msg.encode()).hexdigest()


def salted_target(task, file_pattern, format=None, **kwargs):
    """A local target with a file path formed with a 'salt' kwarg

    :rtype: LocalTarget
    """
    return LocalTarget(file_pattern.format(
        salt=get_salted_version(task)[:6], self=task, **kwargs
    ), format=format)
