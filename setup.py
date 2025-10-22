from setuptools import setup, find_packages

setup(
    name='medallion_dab',
    version='0.0.3.3',
    description='A package for medallion architecture data processing',
    author='Darwin Gavilanes',
    packages=find_packages(where='./script'),
    package_dir={'': './script'},
    install_requires=["setuptools"]
)