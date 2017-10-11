#!/usr/bin/env python3

from setuptools import setup


def parse_requirements():
    requirements = []
    for reqf in ('requirements.txt', 'requirements-swh.txt'):
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
    packages=[
        'swh.storage',
        'swh.storage.api',
        'swh.storage.provenance',
        'swh.storage.schemata',
        'swh.storage.tests',
    ],
    scripts=[
        'bin/swh-storage-add-dir',
    ],
    install_requires=parse_requirements(),
    extras_require={
        'schemata': ['SQLAlchemy'],
        'listener': ['kafka_python'],
    },
    setup_requires=['vcversioner'],
    vcversioner={},
    include_package_data=True,
)
