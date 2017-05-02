from setuptools import setup
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='openchemistrypy',

    version='0.0.1',

    description='',
    long_description=long_description,

    url='https://github.com/OpenChemistry/openchemistrypy',

    author='Kitware Inc',

    license='BSD 3-Clause',

    classifiers=[
        'Development Status :: 3 - Alpha',

        'Intended Audience :: ',
        'Topic :: Software Development :: ',

        'License :: OSI Approved :: BSD 3-Clause',

        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='',

    packages=['openchemistry'],

    install_requires=[
        'girder_client',
        'jinja2'
    ],

    extras_require={

    }
)
