from setuptools import setup, find_packages
setup(
    name="bca4abm",
    version='0.5',
    description='Benefit Calculations for Travel Models',
    author='contributing authors',
    author_email='ben.stabler@rsginc.com',
    license='BSD-3',
    url='https://github.com/RSGInc/bca4abm',
    packages=find_packages(exclude=['*.tests']),
    include_package_data=True,
    install_requires=[
        'numpy >= 1.16.1',
        'openmatrix >= 0.3.4.1',
        'pandas >= 0.24.1',
        'pyyaml >= 5.1',
        'tables >= 3.5.1',
        'toolz >= 0.8.1',
        'zbox >= 1.2',
        'activitysim >= 0.9.1',
        'future >= 0.16.0'
    ]
)
