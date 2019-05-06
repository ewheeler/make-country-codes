========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |goodtables|
        |
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/make-country-codes/badge/?style=flat
    :target: https://readthedocs.org/projects/make-country-codes
    :alt: Documentation Status

.. |travis| image:: https://travis-ci.org/ewheeler/make-country-codes.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/ewheeler/make-country-codes

.. |goodtables| image:: https://goodtables.io/badge/github/ewheeler/make-country-codes.svg
    :alt: Goodtables Build Status
    :target: https://goodtables.io/github/ewheeler/make-country-codes

.. |version| image:: https://img.shields.io/pypi/v/make-country-codes.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/make-country-codes

.. |commits-since| image:: https://img.shields.io/github/commits-since/ewheeler/make-country-codes/v0.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/ewheeler/make-country-codes/compare/v0.0.0...master

.. |wheel| image:: https://img.shields.io/pypi/wheel/make-country-codes.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/make-country-codes

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/make-country-codes.svg
    :alt: Supported versions
    :target: https://pypi.org/project/make-country-codes

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/make-country-codes.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/make-country-codes


.. end-badges

Python package to make datapackage of standard country codes

* Free software: BSD 2-Clause License

Installation
============

::

    pip install make-country-codes

Documentation
=============


https://make-country-codes.readthedocs.io/


Development
===========

To run the all tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
