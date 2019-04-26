from setuptools import setup, find_packages
setup(
    name="bca4abm",
    version='0.4',
    description='Benefit Calculations for Travel Models',
    author='contributing authors',
    author_email='ben.stabler@rsginc.com',
    license='BSD-3',
    url='https://github.com/RSGInc/bca4abm',
    packages=find_packages(exclude=['*.tests']),
    include_package_data=True,
    install_requires=[
        'numpy >= 1.8.0',
        'openmatrix >= 0.2.2',
        'pandas >= 0.23.0',
        'pyyaml >= 3.0, <5.1',
        'tables >= 3.1.0',
        'toolz >= 0.7',
        'zbox >= 1.2',
        'activitysim == 0.7',
    ]
)
