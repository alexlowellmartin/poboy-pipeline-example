from setuptools import find_packages, setup

setup(
    name="poboy_pipeline_example",
    packages=find_packages(exclude=["poboy_pipeline_example_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
