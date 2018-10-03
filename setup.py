from setuptools import setup, find_packages
setup(
    name="bca4abm",
    version='0.3',
    author='RSG Inc',
    packages=find_packages(exclude=['*.tests']),
    install_requires=[
        'numpy >= 1.8.0',
        'openmatrix >= 0.2.2',
        'pandas >= 0.23.0',
        'pyyaml == 3.13',
        'tables >= 3.1.0',
        'toolz >= 0.7',
        'zbox >= 1.2',
        'activitysim >= 0.7',
    ]
)
