from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="dag_scrape_project",
        packages=find_packages(exclude=["dag_scrape_project_tests"]),
        install_requires=[
            "dagster",
        ],
        extras_require={"dev": ["dagit", "pytest"]},
    )
