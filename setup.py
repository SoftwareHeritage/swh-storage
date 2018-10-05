#!/usr/bin/env python3

import os
from setuptools import setup, find_packages


def parse_requirements(name=None):
    if name:
        reqf = 'requirements-%s.txt' % name
    else:
        reqf = 'requirements.txt'

    requirements = []
    if not os.path.exists(reqf):
        return requirements

    with open(reqf) as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            requirements.append(line)
    return requirements


setup(
    name='swh.storage',
    description='Software Heritage storage manager',
    author='Software Heritage developers',
    author_email='swh-devel@inria.fr',
    url='https://forge.softwareheritage.org/diffusion/DSTO/',
    packages=find_packages(),
    scripts=[
        'bin/swh-storage-add-dir',
    ],
    install_requires=parse_requirements() + parse_requirements('swh'),
    extras_require={
        'testing': parse_requirements('test'),
        'schemata': ['SQLAlchemy'],
        'listener': ['kafka_python'],
    },
    setup_requires=['vcversioner'],
    vcversioner={},
    include_package_data=True,
)
