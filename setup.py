from setuptools import find_packages, setup

setup(
    name="orpheus_engine",
    packages=find_packages(exclude=["orpheus_engine_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
