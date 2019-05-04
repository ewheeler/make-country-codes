from luigi import Task
from luigi import LocalTarget
from luigi.task import logger as luigi_logger

import pandas as pd

from pset_utils.luigi.task import TargetOutput

from .data import SaltedFileSource
from .data import SaltedM49Source


class UNCodes(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/'

    pattern = '{task.__class__.__name__}'
    output = TargetOutput(file_pattern=pattern, ext='.csv',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    def requires(self):
        return {'unterm': SaltedFileSource('unterm', '.xlsx'),
                'M49': SaltedM49Source('M49')}

    def run(self):
        m49 = pd.read_csv(self.requires().get('M49').output().path,
                          converters={'Country or Area_en':
                                      lambda x: x.replace("Côte d’Ivoire", "Côte d'Ivoire")})
        luigi_logger.debug(m49)

        def norm(text):
            return text.replace('(the)', '').replace('*', '').strip()

        def swap(text):
            better = text
            for wrong, right in unterm_replacements.items():
                better = better.replace(wrong, right)
            return better

        unterm_converters = {'English Short': 
                             lambda x: swap(norm(x))
                           }
        # UN Protocol liason office needs to update their names!
        unterm_replacements = {
            "Czech Republic": "Czechia",
            "Swaziland": "Eswatini",
            "the former Yugoslav Republic of Macedonia": "North Macedonia"}
        unterm = pd.read_excel(self.requires().get('unterm').output().path,
                               converters=unterm_converters)
                               
        luigi_logger.debug(unterm)

        un = pd.merge(m49, unterm, how='outer', indicator=True,
                      left_on='Country or Area_en',
                      right_on='English Short')
        luigi_logger.debug(un)

        with self.output().open('w') as f:
            un.to_csv(f, index=False)
