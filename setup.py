from setuptools import setup, find_packages

setup(
    name="aws-datalake-glue",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=[
        "boto3>=1.26.0",
        "awswrangler>=3.0.0",
        "pandas>=1.5.0",
        "pyarrow>=12.0.0",
        "pyyaml>=6.0",
        "python-dotenv>=1.0.0",
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="AWS Data Lake Framework with Medallion Architecture",
    keywords="aws, datalake, glue, s3tables, medallion",
    url="https://github.com/yourusername/AwsDataLakeGlue",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
    ],
)
