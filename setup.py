#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

with open('requirements.txt') as requirements_file:
    all_pkgs = requirements_file.readlines()
requirements = [pkg.replace('\n','')  for pkg in all_pkgs]

test_requirements = [ ]

setup(
    author="migration_postgresql_citus_pyarrow",
    python_requires='>=3.10',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.10',
    ],
    description="use pyarrow adbc and pgpq load and dump postgresql citus",
    entry_points={
        'console_scripts': [
            'migration_postgresql_citus_pyarrow=migration_postgresql_citus_pyarrow.cli:main',
        ],
    },
    install_requires=requirements,
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='migration_postgresql_citus_pyarrow',
    name='migration_postgresql_citus_pyarrow',
    packages=find_packages(include=['migration_postgresql_citus_pyarrow', 'migration_postgresql_citus_pyarrow.*']),
    test_suite='tests',
    tests_require=test_requirements,
    version='0.0.1',
    zip_safe=False,
)
