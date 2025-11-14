from setuptools import setup, find_packages

setup(
    name="mongo_db_interaction",
    version="0.1",
    packages=find_packages(where="MongoDb"),
    package_dir={"": "MongoDb"},
)
