#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
   'Click>=7.0', 'pandas>=1.0.5', 'boto3>=1.24', 'cachetools_ext>=0.0.8,<0.1.0', 'botocache>=0.0.4,<0.1.0',
   'awswrangler==2.16.1', 'joblib>=1.0.0',
   'typing-extensions==4.4.0; python_version < \'3.8\''
]

test_requirements = ['pytest>=3', ]

setup(
    author="Jan Skoda",
    author_email='skoda@jskoda.cz',
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    description="API for accessing Lake crypto market data",
    entry_points={
        'console_scripts': [
            'lakeapi=lakeapi.cli:main',
        ],
    },
    install_requires=requirements,
    license="Apache 2 license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='lakeapi',
    name='lakeapi',
    packages=find_packages(include=['lakeapi', 'lakeapi.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/leftys/lakeapi',
    version='0.1.1',
    zip_safe=False,
)
