from setuptools import setup, find_packages

setup(
    name='gateslap',
    version='0.4.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'PyMySQL','DBUtils','tqdm'
    ],
    description = ("A utility designed to generate traffic for VTGate."),
    keywords = "mysql vtgate vitess traffic SQL",
    entry_points='''
        [console_scripts]
        gateslap=gateslap.gateslap:start
    ''',
)
