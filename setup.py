#!/usr/bin/env python
# coding: utf-8
from setuptools import setup, find_packages
  
setup(
  name='pyblux',
  version='0.0.4',
  description='A Powerful suite of fast and intuitive Python ETL utilities',
  long_description=open('README.md').read() + '\n\n' + open('CHANGELOG.md').read(),
  long_description_content_type="text/markdown",
  url="https://pypi.org/project/pyblux",
  author='Bertin Nono',
  author_email='bertin.nono@delta.com',
  license='MIT', 
  platforms=["any"],
  classifiers=[
      "Development Status :: 5 - Production/Stable",
      "Intended Audience :: Developers",
      "Intended Audience :: Information Technology",
      "Intended Audience :: Science/Research",
      'License :: OSI Approved :: MIT License',
      "Natural Language :: English",
      "Operating System :: OS Independent",
      "Programming Language :: Python :: 3",
      "Topic :: Database",
      "Topic :: Scientific/Engineering :: GIS",
  ],
  project_urls={
          'Homepage': 'https://pypi.org/project/pyblux',
          'Documentation': 'https://github.com/pyblux/pyblux#readme',
          'Code': 'https://github.com/pyblux/pyblux',
          'Issue Tracker': 'https://github.com/pyblux/pyblux/issues',
          'Download': 'https://pypi.org/project/pyblux/',
      },
  keywords=['etl', 'database', 'postgres', 'aurora', 'Oracle', 'Microsoft', 'Teams'], 
  python_requires=">= 3.6",
  packages=find_packages()
)

