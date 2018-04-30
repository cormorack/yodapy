from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import os
from codecs import open

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

# Dependencies.
with open('requirements.txt') as f:
    requirements = f.readlines()
install_requires = [t.strip() for t in requirements]

with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='yodapy',
    version='0.1-alpha',
    description='Your Ocean Data Access in Python',
    long_description=long_description,
    url='',
    author='Landung Setiawan',
    author_email='landungs@uw.edu',
    maintainer='Landung Setiawan',
    maintainer_email='landungs@uw.edu',
    license='BSD',
    classifiers=[
        'Development Status :: 1 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering'
    ],
    keywords=['Ocean', 'Data', 'Access', 'OOI'],
    packages=find_packages(),
    install_requires=install_requires,
)
