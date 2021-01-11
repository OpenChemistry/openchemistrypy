from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

install_reqs = [
    'girder_client>=2.3.0',
    'click'
]

setup(
    name='openchemistry_client',
    setup_requires=['setuptools_scm'],
    description='',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/OpenChemistry/openchemistrypy',
    author='Kitware Inc',
    license='BSD 3-Clause',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.6'
    ],
    keywords='',
    packages=find_packages(),
    install_requires=install_reqs,
    entry_points={
        'console_scripts': [
            'occlient = openchemistry_client.client:main'
        ]
    }
)
