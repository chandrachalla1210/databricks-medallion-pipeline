"""Package setup — builds the medallion_pipeline wheel deployed to Databricks."""
from setuptools import setup, find_packages

setup(
    name="medallion_pipeline",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.10",
    install_requires=[
        "pyspark>=3.5.0",
        "delta-spark>=3.1.0",
    ],
    entry_points={
        "console_scripts": [
            "setup_catalog=jobs.setup_catalog:main",
            "validate_source=jobs.validate_source:main",
            "run_quality_checks=jobs.run_quality_checks:main",
            "notify_pipeline_status=jobs.notify_pipeline_status:main",
        ]
    },
    author="Data Platform Team",
    description="Medallion architecture pipeline on Databricks with DLT",
)
