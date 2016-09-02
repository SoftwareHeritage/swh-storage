#!/usr/bin/env python3

from setuptools import setup


def parse_requirements():
    requirements = []
    with open('requirements.txt') as f:
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
        'swh.storage.archiver',
        'swh.storage.api',
        'swh.storage.provenance',
        'swh.storage.tests',
    ],
    scripts=[
        'bin/swh-storage-add-dir',
    ],
    install_requires=parse_requirements(),
    setup_requires=['vcversioner'],
    vcversioner={},
    include_package_data=True,
)
