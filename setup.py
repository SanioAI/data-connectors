from setuptools import setup, find_packages

setup(
    name="data-connectors",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "psycopg2-binary",
        "boto3",
        "pandas",
        "pyarrow"
    ]
)
