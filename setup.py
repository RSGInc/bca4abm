from setuptools import setup, find_packages
import os
import re

with open(os.path.join('bca4abm', '__init__.py')) as f:
    info = re.search(r'__.*', f.read(), re.S)
    exec(info[0])

def include_examples(exname):
    paths = []
    for (path, directory, filenames) in os.walk(exname, topdown=True):
        for filename in filenames:
            if not filename.startswith('.'):
                paths.append((path, [os.path.join(path, filename)]))

    return paths

setup(
    name='bca4abm',
    version=__version__,
    description=__doc__,
    author='contributing authors',
    author_email='ben.stabler@rsginc.com',
    license='BSD-3',
    url='https://github.com/RSGInc/bca4abm',
    packages=find_packages(exclude=['*.tests']),
    include_package_data=True,
    data_files=include_examples('example_4step'),
    entry_points={'console_scripts': ['bca4abm=bca4abm.cli.main:main']},
    install_requires=[
        'numpy >= 1.16.1',
        'openmatrix >= 0.3.4.1',
        'pandas >= 0.24.1',
        'pyyaml >= 5.1',
        'tables >= 3.5.1',
        'toolz >= 0.8.1',
        'zbox >= 1.2',
        'activitysim >= 0.9.1',
    ]
)
