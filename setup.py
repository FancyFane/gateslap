from setuptools import setup, find_packages

setup(
    name='gateslap',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'PyMySQL','DBUtils'
    ],
    entry_points='''
        [console_scripts]
        gateslap=gateslap.gateslap:start
    ''',
)
