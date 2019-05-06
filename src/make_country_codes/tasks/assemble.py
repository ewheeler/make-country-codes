import re
import json
from functools import reduce
from collections import namedtuple
from operator import attrgetter
from itertools import filterfalse

from luigi import Task
from luigi import LocalTarget
from luigi.task import logger as luigi_logger

import pandas as pd
import lxml
from lxml import objectify

from pset_utils.luigi.task import TargetOutput

from .data import SaltedFileSource
from .data import SaltedSTSSource
from .data import SaltedM49Source
from .data import DEV_MODE


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
        # Namibia's 2 letter codes are often `NA`, so setting
        # `keep_default_na=False` and clearing `na_values` is essential!
        m49 = pd.read_csv(self.requires().get('M49').output().path,
                          keep_default_na=False, na_values=['_'],
                          converters={'Country or Area_en':
                                      lambda x: x.replace("Côte d’Ivoire", "Côte d'Ivoire"),
                                      'M49 Code': convert_numeric_code_with_pad,
                                      'Global Code': convert_numeric_code,
                                      'Region Code': convert_numeric_code,
                                      'Sub-region Code': convert_numeric_code,
                                      'Intermediate Region Code': convert_numeric_code})
        m49 = m49.add_suffix(' (M49)')

        # UN Protocol liason office needs to update their names!
        unterm_replacements = {
            "Czech Republic": "Czechia",
            "Swaziland": "Eswatini",
            "the former Yugoslav Republic of Macedonia": "North Macedonia"}

        def fix_english_short(text):
            better = text.replace('(the)', '').replace('*', '').strip()
            for old, new in unterm_replacements.items():
                better = better.replace(old, new)
            return better

        unterm_converters = {'English Short': fix_english_short}
        unterm = pd.read_excel(self.requires().get('unterm').output().path,
                               converters=unterm_converters)

        unterm = unterm.add_suffix(' (unterm)')

        include_indicator = bool(DEV_MODE)

        un = pd.merge(m49, unterm, how='outer', indicator=include_indicator,
                      left_on='Country or Area_en (M49)',
                      right_on='English Short (unterm)')

        with self.output().open('w') as f:
            un.to_csv(f, index=False, float_format='%.0f')


class iso4217(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/'

    pattern = '{task.__class__.__name__}'
    output = TargetOutput(file_pattern=pattern, ext='.csv',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    def requires(self):
        return {'iso4217': SaltedFileSource(slug='iso4217', ext='.xml'),}

    def run(self):
        as_xml = objectify.parse(self.requires().get('iso4217').output().path)

        # currencies to skip
        skip = ['EUROPEAN UNION', 'MEMBER COUNTRIES OF THE AFRICAN DEVELOPMENT BANK GROUP',\
                'SISTEMA UNITARIO DE COMPENSACION REGIONAL DE PAGOS "SUCRE"']
        # some country names to fix
        currency_country_name_map = {
            "CABO VERDE": "CAPE VERDE",
            "CONGO (THE DEMOCRATIC REPUBLIC OF THE)": "DEMOCRATIC REPUBLIC OF THE CONGO",
            "HEARD ISLAND AND McDONALD ISLANDS": "HEARD ISLAND AND MCDONALD ISLANDS",
            "HONG KONG": 'CHINA, HONG KONG SPECIAL ADMINISTRATIVE REGION',
            "KOREA (THE DEMOCRATIC PEOPLE’S REPUBLIC OF)": "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA",
            "KOREA (THE REPUBLIC OF)": "REPUBLIC OF KOREA",
            "MACAO": "China, Macao Special Administrative Region",
            "MACEDONIA (THE FORMER YUGOSLAV REPUBLIC OF)": "NORTH MACEDONIA",
            "MOLDOVA (THE REPUBLIC OF)": "REPUBLIC OF MOLDOVA",
            "PALESTINE, STATE OF": "State of Palestine",
            "SAINT HELENA, ASCENSION AND TRISTAN DA CUNHA": "Saint Helena",
            "SVALBARD AND JAN MAYEN": "Svalbard and Jan Mayen Islands",
            "TANZANIA, UNITED REPUBLIC OF": "United Republic of Tanzania",
            "VIRGIN ISLANDS (U.S.)": "UNITED STATES VIRGIN ISLANDS",
            "VIRGIN ISLANDS (BRITISH)": "BRITISH VIRGIN ISLANDS",
            "WALLIS AND FUTUNA": "Wallis and Futuna Islands",
        }

        as_lists = []
        for iso_currency_table in as_xml.iter():
            if isinstance(iso_currency_table, objectify.ObjectifiedElement):
                # get strings rather than an ObjectifiedElements
                # because they can be truthy while their .text is None!
                currency = [c.text for c in iso_currency_table.getchildren()]
                if currency:
                    if currency[0]:
                        if currency[0] in skip or currency[0].startswith('ZZ'):
                            continue
                        if currency[0] in currency_country_name_map:
                            # correct the few names that do not match M49 names
                            currency[0] = currency_country_name_map.get(currency[0])
                        as_lists.append(currency)

        columns = ['Country Name (iso4217)', 'Currency Name (iso4217)',\
                   'Currency Code Alpha (iso4217)', 'Currency Code Numeric (iso4217)',\
                   'Currency Minor Units (iso4217)']
        df = pd.DataFrame(data=as_lists, columns=columns)

        with self.output().open('w') as f:
            df.to_csv(f, index=False, float_format='%.0f')


class marc(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/'

    pattern = '{task.__class__.__name__}'
    output = TargetOutput(file_pattern=pattern, ext='.csv',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    def requires(self):
        return {'marc': SaltedFileSource(slug='marc', ext='.xml'),}

    def run(self):
        as_xml = objectify.parse(self.requires().get('marc').output().path)
        as_lists = []
        for territory in as_xml.getroot().countries.iterchildren():
            if isinstance(territory, objectify.ObjectifiedElement):
                stuff = [c.text for c in territory.getchildren()
                         if c.text and not c.text.startswith('info')]
                if stuff:
                    if stuff[2] in ['North America'] and stuff[0] not in ['Greenland', 'Canada', 'United States', 'Mexico']:
                        # skip US States and Canadian Provinces
                        continue
                    if stuff[2] in ['Australasia'] and stuff[0] not in ['Australia', 'New Zealand', 'Tazmania']:
                        # skip Australian States and Territories
                        continue
                    if len(stuff) != 3:
                        if stuff[0] in ['Indonesia', 'Yemen']:
                            # names are repeated twice for these
                            stuff.pop(0)
                        if stuff[0] in ['Anguilla']:
                            # discard extra obsolete code
                            stuff.pop()
                    as_lists.append(stuff)
        columns = ['Country Name (marc)', 'Marc Code (marc)',\
                   'Continent (marc)']
        df = pd.DataFrame(data=as_lists, columns=columns)

        with self.output().open('w') as f:
            df.to_csv(f, index=False, float_format='%.0f')


class ukgov(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/'

    pattern = '{task.__class__.__name__}'
    output = TargetOutput(file_pattern=pattern, ext='.csv',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    def requires(self):
        return {'ukgov': SaltedFileSource(slug='ukgov', ext='.json'),}

    def run(self):
        as_json = json.load(self.requires().get('ukgov').output().open())
        as_list_of_dicts  = {k: v['item'][0] for k,v in as_json.items()}.values()
        df = pd.DataFrame(as_list_of_dicts)
        df = df.add_suffix(' (ukgov)')

        with self.output().open('w') as f:
            df.to_csv(f, index=False, float_format='%.0f')


class cldr(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/'

    pattern = '{task.__class__.__name__}'
    output = TargetOutput(file_pattern=pattern, ext='.csv',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    def requires(self):
        return {'cldr': SaltedFileSource(slug='cldr', ext='.json'),}

    def run(self):
        as_json = json.load(self.requires().get('cldr').output().open())
        territories = as_json['main']['en']['localeDisplayNames']['territories']

        # find countries with alternate short/variant names
        alt = {k: v for k,v in territories.items() if len(k) > 2 and not k.isdigit()}

        # instead of separate items, append alternates to name separated by semicolon
        for k, v in alt.items():
            code, alt, kind = k.split('-')
            # find original name
            orig = territories.get(code)
            # combine cldr name and alternate
            edit = f'{orig}; {v}'
            # update with alternate name
            territories.update({code: edit})
            # remove alternate item
            territories.pop(k)

        # discard continents/regions (which have numeric codes)
        countries = filterfalse(lambda x: x[0].isdigit(), territories.items())

        columns = ['Locale Code (cldr)', 'Locale Display Name (cldr)']
        df = pd.DataFrame(countries, columns=columns)

        with self.output().open('w') as f:
            df.to_csv(f, index=False, float_format='%.0f')


class MergeTabular(Task):
    __version__ = '0.1'
    DATA_ROOT = 'data/'

    pattern = '{task.__class__.__name__}'
    output = TargetOutput(file_pattern=pattern, ext='.csv',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    def requires(self):
        return {'UNCodes': UNCodes(),
                'iso4217': iso4217(),
                'marc': marc(),
                'ukgov': ukgov(),
                'cldr': cldr(),
                'geonames': SaltedFileSource(slug='geonames', ext='.txt'),
                'usa-census': SaltedFileSource(slug='usa-census', ext='.txt'),
                'exio-wiod-eora': SaltedFileSource(slug='exio-wiod-eora', ext='.tsv'),
                'fao': SaltedSTSSource(slug='fao'),
                'fifa-ioc': SaltedSTSSource(slug='fifa-ioc')}

    def run(self):
        # load pre-cleaned datasets (e.g., sources with their own named tasks)
        UNCodes = pd.read_csv(self.requires().get('UNCodes').output().path,
                              keep_default_na=False, na_values=['_'])
        if DEV_MODE:
            UNCodes.drop('_merge', inplace=True, axis=1)
        iso4217 = pd.read_csv(self.requires().get('iso4217').output().path,
                              keep_default_na=False, na_values=['_'])
        marc = pd.read_csv(self.requires().get('marc').output().path,
                           keep_default_na=False, na_values=['_'])
        ukgov = pd.read_csv(self.requires().get('ukgov').output().path,
                            keep_default_na=False, na_values=['_'])
        cldr = pd.read_csv(self.requires().get('cldr').output().path,
                           keep_default_na=False, na_values=['_'])

        # load and clean tabular sources
        geonames = pd.read_csv(self.requires().get('geonames').output().path,
                               keep_default_na=False, na_values=['_'],
                               converters={'ISO-Numeric': convert_numeric_code_with_pad},
                               header=50, sep='\t')
        geonames = geonames.astype(dtype={'ISO-Numeric': 'object'})
        geonames = geonames.add_suffix(' (geonames)')

        # TODO this source has some errors...
        # ISO code column has incorrect codes for Kosovo, DRC, and Myanmar
        usacensus = pd.read_csv(self.requires().get('usa-census').output().path,
                                converters={' ISO Code': lambda x: x.replace(' ', '')},
                                header=3, skiprows=[4, 246, 247, 248, 249],
                                sep='|')
        usacensus = usacensus.add_suffix(' (usa-census)')

        exio = pd.read_csv(self.requires().get('exio-wiod-eora').output().path,
                           keep_default_na=False, na_values=['_'], sep='\t',
                           converters={'ISOnumeric': convert_numeric_code_with_pad,
                                       'UNcode': convert_numeric_code_with_pad})
        exio = exio.astype(dtype={'ISOnumeric': 'object',
                                  'UNcode': 'object'})
        exio = exio.add_suffix(' (exio-wiod-eora)')

        fao = pd.read_csv(self.requires().get('fao').output().path,
                          keep_default_na=False, na_values=['_'],
                          converters={'Short name': lambda x: re.sub(r'\\n', '', x)})
        fao = fao.add_suffix(' (fao)')

        fifa = pd.read_csv(self.requires().get('fifa-ioc').output().path,
                           keep_default_na=False, na_values=['_'],
                           converters={'ISO': lambda x: re.sub(r'\\n|\[\d+\]', '', x),
                                       'Country': lambda x: re.sub(r'\[\d+\]', '', x),
                           })
        fifa.drop('Flag', inplace=True, axis=1)
        fifa = fifa.add_suffix(' (fifa-ioc)')

        # join on ISO 3166 alpha 3 codes
        combined = pd.merge(UNCodes, exio, how='outer',
                            left_on='ISO-alpha3 Code (M49)',
                            right_on='ISO3 (exio-wiod-eora)')

        combined = pd.merge(combined, fao, how='outer',
                            left_on='ISO-alpha3 Code (M49)',
                            right_on='ISO3 (fao)')

        combined = pd.merge(combined, fifa, how='outer',
                            left_on='ISO-alpha3 Code (M49)',
                            right_on='ISO (fifa-ioc)')

        # join on ISO 3166 alpha 2 codes
        combined  = pd.merge(combined, geonames, how='outer',
                              left_on='ISO2 (exio-wiod-eora)',
                              right_on='#ISO (geonames)')
        combined = pd.merge(combined, usacensus, how='outer',
                           left_on='#ISO (geonames)',
                           right_on=' ISO Code (usa-census)')

        combined = pd.merge(combined, ukgov, how='outer',
                           left_on=' ISO Code (usa-census)',
                           right_on='country (ukgov)')

        combined = pd.merge(combined, cldr, how='outer',
                           left_on='#ISO (geonames)',
                           right_on='Locale Code (cldr)')
        # TODO join on country names
        # itu-glad
        # marc
        # iso4217
        with self.output().open('w') as f:
            combined.to_csv(f, index=False, float_format='%.0f')
