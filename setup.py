from setuptools import setup
import re

with open('requirements.txt') as f:
    requirements = f.read().splitlines()
    requirements = [l for l in requirements if not l.strip().startswith('#')]

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

VERSIONFILE = "clio/__version__.py"
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

setup(
    name='clio-py',
    version=verstr,
    description="Python client utilties for interacting with the Clio platform",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Philipp Schlegel",
    author_email='pms70@cam.ac.uk',
    url='https://github.com/schlegelp/clio-py',
    packages=['clio'],
    entry_points={},
    install_requires=requirements,
    python_requires='>=3.9',
    keywords='clio',
    classifiers=[
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
    ]
)
