import shelve
import os
import csv
import urllib
from functools import reduce

from luigi import format
from luigi import LocalTarget
from luigi import Parameter
from luigi import Task
from luigi import WrapperTask
from luigi.task_register import load_task
from luigi.task import logger as luigi_logger

import requests
from lxml import html
import pandas as pd

from ..utils import bytes_pls
from ..utils import clean
from ..utils import sha256sum
from ..utils import TargetOutput
from ..utils import Requires
from ..utils import Requirement

#USE_SHELVE = os.environ.get('USE_SHELVE')
#DEV_MODE = os.environ.get('DEV_MODE')
USE_SHELVE = False
DEV_MODE = False

REMOTE_FILE_SOURCES = {
    'unterm': 'https://protocol.un.org/dgacm/pls/site.nsf/files/Country%20Names%20UNTERM2/$FILE/UNTERM%20EFSRCA.xlsx',
    'iso4217': 'https://www.currency-iso.org/dam/downloads/lists/list_one.xml',
    'marc': 'http://www.loc.gov/standards/codelists/countries.xml',
    'cldr': 'https://raw.githubusercontent.com/unicode-cldr/cldr-localenames-full/master/main/en/territories.json',
    'geonames': 'http://download.geonames.org/export/dump/countryInfo.txt',
    'usa-census': 'https://www.census.gov/foreign-trade/schedules/c/country2.txt',
    'exio-wiod-eora': 'https://raw.githubusercontent.com/konstantinstadler/country_converter/master/country_converter/country_data.tsv',
    'ukgov': 'https://country.register.gov.uk/records.json'
}

CUSTOM_SCRAPE_SOURCES = {
    'Edgar': 'https://www.sec.gov/edgar/searchedgar/edgarstatecodes.htm',
    'M49': 'https://unstats.un.org/unsd/methodology/m49/overview/',
}


SIMPLE_TABLE_SCRAPE_SOURCES = {
    'itu-glad': 'https://www.itu.int/gladapp/GeographicalArea/List',
    'fao': 'http://www.fao.org/countryprofiles/iso3list/en/',
    'fifa-ioc': 'https://simple.wikipedia.org/wiki/Comparison_of_IOC,_FIFA,_and_ISO_3166_country_codes',
}

SOURCES = [
    {
        'name': 'unterm',
        'title': 'United Nations Protocol and Liason Service',
        'path': 'https://protocol.un.org/dgacm/pls/site.nsf/files/Country%20Names%20UNTERM2/$FILE/UNTERM%20EFSRCA.xlsx',
    },
    {
        'name': 'iso4217',
        'title': 'Swiss Association for Standardization',
        'path': 'https://www.currency-iso.org/dam/downloads/lists/list_one.xml',
    },
    {
        'name': 'marc',
        'title': 'USA Library of Congress',
        'path': 'http://www.loc.gov/standards/codelists/countries.xml',
    },
    {
        'name': 'cldr',
        'title': 'Unicode Common Locale Data Repository',
        'path': 'https://raw.githubusercontent.com/unicode-cldr/cldr-localenames-full/master/main/en/territories.json',
    },
    {
        'name': 'geonames',
        'title': 'GeoNames',
        'path': 'http://download.geonames.org/export/dump/countryInfo.txt',
    },
    {
        'name': 'usa-census',
        'title': 'USA Census Bureau',
        'path': 'https://www.census.gov/foreign-trade/schedules/c/country2.txt',
    },
    {
        'name': 'exio-wiod-eora',
        'title': 'Secondary source providing EXIOBASE, WIOD, Eora, and other country codes',
        'path': 'https://raw.githubusercontent.com/konstantinstadler/country_converter/master/country_converter/country_data.tsv',
    },
    {
        'name': 'ukgov',
        'title': 'Government of the United Kingdom',
        'path': 'https://country.register.gov.uk/records.json'
    },
    {
        'name': 'Edgar',
        'title': 'USA Security and Exchange Commission',
        'path': 'https://www.sec.gov/edgar/searchedgar/edgarstatecodes.htm',
    },
    {
        'name': 'M49',
        'title': 'United Nations Statistics Division',
        'path': 'https://unstats.un.org/unsd/methodology/m49/overview/',
    },
    {
        'name': 'itu-glad',
        'title': 'International Telecommunications Union',
        'path': 'https://www.itu.int/gladapp/GeographicalArea/List',
    },
    {
        'name': 'fao',
        'title': 'Food and Agriculture Organization',
        'path': 'http://www.fao.org/countryprofiles/iso3list/en/',
    },
    {
        'name': 'fifa-ioc',
        'title': 'Wikipedia (FIFA and IOC)',
        'path': 'https://simple.wikipedia.org/wiki/Comparison_of_IOC,_FIFA,_and_ISO_3166_country_codes',
    },
]



class FileSource(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/sources/'

    slug = Parameter()
    ext = Parameter()

    pattern = '{task.__class__.__name__}-{task.slug}{task.ext}'
    output = TargetOutput(file_pattern=pattern, ext='',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget,
                          format=format.Nop)

    def run(self):
        url = REMOTE_FILE_SOURCES.get(self.slug)

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            if r.encoding is None:
                r.encoding = 'utf-8'
            with self.output().open('w') as f:
                for chunk in r.iter_content(chunk_size=128):
                    f.write(bytes_pls(chunk))


class SaltedFileSource(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/sources/'

    slug = Parameter()
    ext = Parameter()
    salt = sha256sum()

    requires = Requires()
    source = Requirement(FileSource, slug=slug, ext=ext)

    pattern = '{task.__class__.__name__}-{task.slug}-{task.salt}{task.ext}'
    output = TargetOutput(file_pattern=pattern, ext='',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget,
                          format=format.Nop)

    def run(self):
        self.requires().get('source').output().copy(self.output().path)


class EdgarSource(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/sources/'
    slug = Parameter(default='Edgar')
    ext = Parameter(default='.csv')

    pattern = '{task.__class__.__name__}-{task.slug}{task.ext}'
    output = TargetOutput(file_pattern=pattern, ext='',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    def run(self):
        url = CUSTOM_SCRAPE_SOURCES.get('Edgar')

        content = urllib.request.urlopen(url).read()
        doc = html.fromstring(content)
        rows = doc.xpath('//table')[3].getchildren()

        seen_other_countries = False
        data = []

        for row in rows:
            if seen_other_countries is not True:
                if clean(row.text_content()) != 'Other Countries':
                    continue
                else:
                    seen_other_countries = True
                    continue

            cells = row.getchildren()
            if len(cells) != 2:
                luigi_logger.debug('ERROR IN CELL COUNT')
                for cell in cells:
                    luigi_logger.debug(cell)
                    luigi_logger.debug(cell.text_content())
                continue
            code = clean(cells[0].text_content())
            name = clean(cells[1].text_content())
            data.append((code, name))

        with self.output().open('w') as f:
            writer = csv.writer(f)
            writer.writerows(data)


class SaltedEdgarSource(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/sources/'

    slug = Parameter(default='Edgar')
    ext = Parameter(default='.csv')
    salt = sha256sum()

    requires = Requires()
    source = Requirement(EdgarSource, slug='Edgar', ext='.csv')

    pattern = '{task.__class__.__name__}-{task.slug}-{task.salt}{task.ext}'
    output = TargetOutput(file_pattern=pattern, ext='',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    def run(self):
        self.requires().get('source').output().copy(self.output().path)


class M49Source(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/sources/'
    slug = Parameter(default='M49')
    ext = Parameter(default='.csv')

    pattern = '{task.__class__.__name__}-{task.slug}{task.ext}'
    output = TargetOutput(file_pattern=pattern, ext='',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    def run(self):
        url = CUSTOM_SCRAPE_SOURCES.get('M49')

        if USE_SHELVE:
            luigi_logger.debug('USING SHELVE')
            try:
                shelf = shelve.open('tmpdb')
                content = shelf['M49']
            except KeyError:
                content = urllib.request.urlopen(url).read()
                shelf['M49'] = content
            finally:
                shelf.close()
        else:
            content = urllib.request.urlopen(url).read()

        tables = {'en': 'downloadTableEN', 'cn': 'downloadTableZH',
                  'ru': 'downloadTableRU', 'fr': 'downloadTableFR',
                  'es': 'downloadTableES', 'ar': 'downloadTableAR'}

        def read_table(table_language, table_id):
            l = table_language
            attrs = {'id': table_id}
            converters = {
                    'Region Code': lambda x: str(x),
                    'Intermediate Region Code': lambda x: str(x),
                    'Least Developed Countries (LDC)': lambda x: bool(x) if bool(x) else '',
                    'Land Locked Developing Countries (LLDC)': lambda x: bool(x) if bool(x) else '',
                    'Small Island Developing States (SIDS)': lambda x: bool(x) if bool(x) else '',
                    'Developed /  Developing Countries': lambda x: x.replace('\r\n', ''),
                    }
            df = pd.read_html(content, encoding='utf-8', attrs=attrs,
                              header=0, converters=converters)
            return df[0]

        if USE_SHELVE:
            luigi_logger.debug('USING SHELVE')
            try:
                shelf = shelve.open('tmpdb')
                frames_tuples = shelf['m49-frames_tuples']
            except KeyError:
                # read the 6 html tables into dataframes,
                # arrange dataframes in list of 2-item tuples
                # like [('language', dataframe),...]
                frames_tuples = [(lang, read_table(lang, table)) for lang, table in tables.items()]
                shelf['m49-frames_tuples'] = frames_tuples
            finally:
                shelf.close()
        else:
            frames_tuples = [(lang, read_table(lang, table)) for lang, table in tables.items()]

        # values in these columns are the same in any language
        # (excluding `M49 Code` bc we need it to merge dataframes)
        non_local = ['Global Code', 'Region Code', 'Sub-region Code',
                     'Intermediate Region Code',
                     'ISO-alpha3 Code', 'Least Developed Countries (LDC)',
                     'Land Locked Developing Countries (LLDC)',
                     'Small Island Developing States (SIDS)',
                     'Developed / Developing Countries']

        # remove repeated, non-localized columns
        # from all dataframes except one
        pruned = [(lang, frame.drop([c for c in non_local],
                                     inplace=True, axis=1))
                   for lang, frame in frames_tuples if lang != 'en']

        # reduce frames_tuples:
        # - merge dataframes on 'M49 Code' column
        # - add language suffixes to column names
        merged = reduce(lambda left, right:
                        (right[0], pd.merge(left[1], right[1],
                                            on='M49 Code',
                                            suffixes=('_' + left[0], '_' + right[0]))),
                        frames_tuples)

        with self.output().open('w') as f:
            merged[1].to_csv(f, index=False)


class SaltedM49Source(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/sources/'

    slug = Parameter(default='M49')
    ext = Parameter(default='.csv')
    salt = sha256sum()

    requires = Requires()
    source = Requirement(M49Source, slug='M49', ext='.csv')

    pattern = '{task.__class__.__name__}-{task.slug}-{task.salt}{task.ext}'
    output = TargetOutput(file_pattern=pattern, ext='',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    def run(self):
        self.requires().get('source').output().copy(self.output().path)


class SimpleTableScrapeSource(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/sources/'
    slug = Parameter()
    ext = Parameter(default='.csv')

    pattern = '{task.__class__.__name__}-{task.slug}{task.ext}'
    output = TargetOutput(file_pattern=pattern, ext='',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    def run(self):
        luigi_logger.debug(['STSS', str(self.slug), dir(self.slug)])
        url = SIMPLE_TABLE_SCRAPE_SOURCES.get(self.slug)
        if USE_SHELVE:
            luigi_logger.debug('USING SHELVE')
            try:
                shelf = shelve.open('tmpdb')
                content = shelf[self.slug]
            except KeyError:
                content = urllib.request.urlopen(url).read()
                shelf[self.slug] = str(content)
            finally:
                shelf.close()
        else:
            content = urllib.request.urlopen(url).read()

        df = pd.read_html(content, header=0)
        with self.output().open('w') as f:
            df[0].to_csv(f, index=False)


class SaltedSTSSource(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/sources/'

    slug = Parameter()
    ext = Parameter(default='.csv')
    salt = sha256sum()

    requires = Requires()
    source = Requirement(SimpleTableScrapeSource, slug=slug, ext=ext)

    pattern = '{task.__class__.__name__}-{task.slug}-{task.salt}{task.ext}'
    output = TargetOutput(file_pattern=pattern, ext='',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    def run(self):
        self.requires().get('source').output().copy(self.output().path)


class SaltedSources(WrapperTask):

    def requires(self):
        for slug, url in REMOTE_FILE_SOURCES.items():
            _, ext = os.path.splitext(url)
            luigi_logger.debug(['SaltedFileSource', slug, ext])
            yield SaltedFileSource(slug=slug, ext=ext)

        for slug, url in SIMPLE_TABLE_SCRAPE_SOURCES.items():
            luigi_logger.debug(['SaltedSTSSource', slug, '.csv'])
            yield SaltedSTSSource(slug=slug, ext='.csv')

        for slug, url in CUSTOM_SCRAPE_SOURCES.items():
            luigi_logger.debug(['Salted__Source', slug, '.csv'])
            yield load_task(__name__, f'Salted{slug}Source', {'slug': slug, 'ext': '.csv'})
