import re
import json
from itertools import filterfalse

from luigi import Task
from luigi.task import logger as luigi_logger

import pandas as pd
from lxml import objectify
from datapackage import Package

from ..utils import TargetOutput
from ..utils import SuffixPreservingLocalTarget as LocalTarget
from ..utils import convert_numeric_code_with_pad
from ..utils import convert_numeric_code
from ..utils import Requires
from ..utils import Requirement

from .data import SaltedFileSource
from .data import SaltedSTSSource
from .data import SaltedM49Source
from .data import SaltedEdgarSource
from .data import SOURCES
from .data import DEV_MODE


class UNCodes(Task):
    __version__ = '0.1'
    DATA_ROOT = 'build/'

    pattern = '{task.__class__.__name__}'
    output = TargetOutput(file_pattern=pattern, ext='.csv',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    requires = Requires()
    unterm = Requirement(SaltedFileSource, slug='unterm', ext='.xlsx')
    m49 = Requirement(SaltedM49Source, slug='M49', ext='.csv')

    def run(self):
        # Namibia's 2 letter codes are often `NA`, so setting
        # `keep_default_na=False` and clearing `na_values` is essential!
        m49 = pd.read_csv(self.requires().get('m49').output().path,
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
    DATA_ROOT = 'build/'

    pattern = '{task.__class__.__name__}'
    output = TargetOutput(file_pattern=pattern, ext='.csv',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    requires = Requires()
    iso4217 = Requirement(SaltedFileSource, slug='iso4217', ext='.xml')

    def run(self):
        as_xml = objectify.parse(self.requires().get('iso4217').output().path)

        # currencies to skip
        skip = ['EUROPEAN UNION', 'MEMBER COUNTRIES OF THE AFRICAN DEVELOPMENT BANK GROUP',\
                'SISTEMA UNITARIO DE COMPENSACION REGIONAL DE PAGOS "SUCRE"']
        # some country names to fix
        currency_country_name_map = {
            "CABO VERDE": "CAPE VERDE",
            "CONGO (THE)": "CONGO",
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
        seen = []
        for iso_currency_table in as_xml.iter():
            if isinstance(iso_currency_table, objectify.ObjectifiedElement):
                # get strings rather than an ObjectifiedElements
                # because they can be truthy while their .text is None!
                currency = [c.text for c in iso_currency_table.getchildren()]
                if currency:
                    if currency[0]:
                        if currency[0] in skip or currency[0].startswith('ZZ') or currency[0].startswith('INTERNATIONAL'):
                            continue
                        if currency[0] in currency_country_name_map:
                            # correct the few names that do not match M49 names
                            currency[0] = currency_country_name_map.get(currency[0])
                        if currency[0] not in seen:
                            # source includes additional, commonly used or
                            # accepted currencies from other states.
                            # we keep only the first/most official currency
                            seen.append(currency[0])
                            as_lists.append(currency)

        columns = ['Country Name (iso4217)', 'Currency Name (iso4217)',\
                   'Currency Code Alpha (iso4217)', 'Currency Code Numeric (iso4217)',\
                   'Currency Minor Units (iso4217)']
        df = pd.DataFrame(data=as_lists, columns=columns)

        with self.output().open('w') as f:
            df.to_csv(f, index=False, float_format='%.0f')


class marc(Task):
    __version__ = '0.1'
    DATA_ROOT = 'build/'

    pattern = '{task.__class__.__name__}'
    output = TargetOutput(file_pattern=pattern, ext='.csv',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    requires = Requires()
    marc = Requirement(SaltedFileSource, slug='marc', ext='.xml')

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
        columns = ['Country Name (marc)', 'Marc Code (marc)',
                   'Continent (marc)']
        df = pd.DataFrame(data=as_lists, columns=columns)

        with self.output().open('w') as f:
            df.to_csv(f, index=False, float_format='%.0f')


class ukgov(Task):
    __version__ = '0.1'
    DATA_ROOT = 'build/'

    pattern = '{task.__class__.__name__}'
    output = TargetOutput(file_pattern=pattern, ext='.csv',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    requires = Requires()
    ukgov = Requirement(SaltedFileSource, slug='ukgov', ext='.json')

    def run(self):
        source = self.requires().get('ukgov').output()
        as_json = json.load(open(source.path, 'r'))
        as_list_of_dicts  = {k: v['item'][0] for k,v in as_json.items()}.values()
        df = pd.DataFrame(as_list_of_dicts)
        df = df.add_suffix(' (ukgov)')

        with self.output().open('w') as f:
            df.to_csv(f, index=False, float_format='%.0f')


class cldr(Task):
    __version__ = '0.1'
    DATA_ROOT = 'build/'

    pattern = '{task.__class__.__name__}'
    output = TargetOutput(file_pattern=pattern, ext='.csv',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    requires = Requires()
    cldr = Requirement(SaltedFileSource, slug='cldr', ext='.json')

    def run(self):
        source = self.requires().get('cldr').output()
        as_json = json.load(open(source.path, 'r'))
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


class CountryCodes(Task):
    __version__ = '0.1'
    DATA_ROOT = 'build/'

    pattern = 'country-codes'
    output = TargetOutput(file_pattern=pattern, ext='.csv',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    requires = Requires()
    src_UNCodes = Requirement(UNCodes)
    src_iso4217 = Requirement(iso4217)
    src_marc = Requirement(marc)
    src_ukgov = Requirement(ukgov)
    src_cldr = Requirement(cldr)
    src_edgar = Requirement(SaltedEdgarSource)
    src_geonames = Requirement(SaltedFileSource, slug='geonames', ext='.txt')
    src_usacensus = Requirement(SaltedFileSource, slug='usa-census', ext='.txt')
    src_exio = Requirement(SaltedFileSource, slug='exio-wiod-eora', ext='.tsv')
    src_fao = Requirement(SaltedSTSSource, slug='fao', ext='.csv')
    src_fifa = Requirement(SaltedSTSSource, slug='fifa-ioc', ext='.csv')
    src_itu = Requirement(SaltedSTSSource, slug='itu-glad', ext='.csv')


    def run(self):
        # load pre-cleaned datasets (e.g., sources with their own named tasks)
        UNCodes = pd.read_csv(self.requires().get('src_UNCodes').output().path,
                              keep_default_na=False, na_values=['_'])
        if DEV_MODE:
            UNCodes.drop('_merge', inplace=True, axis=1)
        iso4217 = pd.read_csv(self.requires().get('src_iso4217').output().path,
                              keep_default_na=False, na_values=['_'])
        marc = pd.read_csv(self.requires().get('src_marc').output().path,
                           keep_default_na=False, na_values=['_'])
        ukgov = pd.read_csv(self.requires().get('src_ukgov').output().path,
                            keep_default_na=False, na_values=['_'])
        cldr = pd.read_csv(self.requires().get('src_cldr').output().path,
                           keep_default_na=False, na_values=['_'])
        edgar = pd.read_csv(self.requires().get('src_edgar').output().path,
                            keep_default_na=False, na_values=['_'])
        edgar.columns = ['Edgar Code (edgar)', 'Country Name (edgar)']

        # load and clean tabular sources
        geonames = pd.read_csv(self.requires().get('src_geonames').output().path,
                               keep_default_na=False, na_values=['_'],
                               converters={'ISO-Numeric': convert_numeric_code_with_pad},
                               header=50, sep='\t')
        geonames = geonames.astype(dtype={'ISO-Numeric': 'object'})
        geonames = geonames.rename(lambda x: x.replace('#', ''), axis=1)
        geonames = geonames.add_suffix(' (geonames)')

        # TODO this source has some errors...
        # ISO code column has incorrect codes for Kosovo, DRC, and Myanmar
        usacensus = pd.read_csv(self.requires().get('src_usacensus').output().path,
                                converters={' ISO Code': lambda x: x.replace(' ', '')},
                                header=3, skiprows=[4, 246, 247, 248, 249],
                                sep='|')
        usacensus = usacensus.rename(lambda x: x.strip(), axis=1)
        usacensus = usacensus.add_suffix(' (usa-census)')

        exio = pd.read_csv(self.requires().get('src_exio').output().path,
                           keep_default_na=False, na_values=['_'], sep='\t',
                           converters={'ISOnumeric': convert_numeric_code_with_pad,
                                       'UNcode': convert_numeric_code_with_pad})
        exio = exio.astype(dtype={'ISOnumeric': 'object',
                                  'UNcode': 'object'})
        exio = exio.add_suffix(' (exio-wiod-eora)')

        fao = pd.read_csv(self.requires().get('src_fao').output().path,
                          keep_default_na=False, na_values=['_'],
                          converters={'Short name': lambda x: re.sub(r'\\n', '', x)})
        fao = fao.add_suffix(' (fao)')

        fifa = pd.read_csv(self.requires().get('src_fifa').output().path,
                           keep_default_na=False, na_values=['_'],
                           converters={'ISO': lambda x: re.sub(r'\\n|\[\d+\]', '', x),
                                       'Country': lambda x: re.sub(r'\[\d+\]', '', x),
                           })
        fifa.drop('Flag', inplace=True, axis=1)
        fifa = fifa.add_suffix(' (fifa-ioc)')

        itu = pd.read_csv(self.requires().get('src_itu').output().path,
                           keep_default_na=False, na_values=['_'])
        itu = itu.add_suffix(' (itu-glad)')

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
                              right_on='ISO (geonames)')
        combined = pd.merge(combined, usacensus, how='outer',
                           left_on='ISO (geonames)',
                           right_on='ISO Code (usa-census)')

        combined = pd.merge(combined, ukgov, how='outer',
                           left_on='ISO Code (usa-census)',
                           right_on='country (ukgov)')

        combined = pd.merge(combined, cldr, how='outer',
                           left_on='ISO (geonames)',
                           right_on='Locale Code (cldr)')

        def match_on_names(df, name_column):
            def correct(territory):
                matches = []
                for t in regex_tuples:
                    pat = re.compile(t[1], flags=re.I)
                    if pat.findall(territory):
                        matches.append(t[0])
                if any(matches):
                    return pd.Series({'ISO3': matches[0], name_column: territory})
                return pd.Series({'ISO3': '', name_column: territory})
            return pd.merge(df, df.get(name_column).apply(correct))

        # exio-wiod-eora source includes handy regexes for matching country names
        regex_tuples = combined[['ISO3 (exio-wiod-eora)',
                                 'regex (exio-wiod-eora)']].dropna().apply(tuple, axis=1).to_list()

        marc = match_on_names(marc, 'Country Name (marc)')
        iso4217 = match_on_names(iso4217, 'Country Name (iso4217)')
        edgar = match_on_names(edgar, 'Country Name (edgar)')
        # TODO errors when matching, so leaving out for now
        itu = match_on_names(itu, 'Designation (itu-glad)')

        matched = pd.merge(iso4217, marc, how='outer',
                           left_on='ISO3',
                           right_on='ISO3')
        matched = pd.merge(matched, edgar, how='outer',
                           left_on='ISO3',
                           right_on='ISO3')
        combined = pd.merge(combined, matched, how='outer',
                            left_on='ISO3 (exio-wiod-eora)',
                            right_on='ISO3')
        combined.drop('ISO3', inplace=True, axis=1)

        with self.output().open('w') as f:
            combined.to_csv(f, index=False, float_format='%.0f')


class Datapackage(Task):
    __version__ = '0.1'
    DATA_ROOT = 'build/'

    pattern = 'datapackage'
    output = TargetOutput(file_pattern=pattern, ext='.json',
                          base_dir=DATA_ROOT,
                          target_class=LocalTarget)

    requires = Requires()
    source = Requirement(CountryCodes)

    def run(self):
        package = Package()
        metadata = {"name": "country-codes",
                    "title": "Comprehensive country codes: ISO 3166, ITU, ISO 4217 currency codes and many more",
                    "licenses": [{"name": "ODC-PDDL-1.0",
                                  "path": "http://opendatacommons.org/licenses/pddl/",
                                  "title": "Open Data Commons Public Domain Dedication and License v1.0"}],
                    "repository": {
                        "type": "git",
                        "url": "https://github.com/datasets/country-codes"},
                    "contributors": [
                        {
                        "title": "Evan Wheeler",
                        "path": "https://github.com/datasets/country-codes",
                        "role": "maintainer"
                        }
                    ],
                    "related": [
                        {
                        "title": "Country list",
                        "path": "/core/country-list",
                        "publisher": "core",
                        "formats": ["CSV", "JSON"]
                        },
                        {
                        "title": "Language codes",
                        "path": "/core/language-codes",
                        "publisher": "core",
                        "formats": ["CSV", "JSON"]
                        },
                        {
                        "title": "Airport codes",
                        "path": "/core/airport-codes",
                        "publisher": "core",
                        "formats": ["CSV", "JSON"]
                        },
                        {
                        "title": "Continent codes",
                        "path": "/core/continent-codes",
                        "publisher": "core",
                        "formats": ["CSV", "JSON"]
                        }
                    ]
                   }
        metadata.update(package.infer(self.requires().get('source').output().path))
        metadata.update({'sources': SOURCES})
        with self.output().open('w') as f:
            json.dump(metadata, f)
