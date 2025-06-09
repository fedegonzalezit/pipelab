from setuptools import setup, find_packages

with open("README.md") as readme_file:
    readme = readme_file.read()

# No HISTORY.rst, so only use README.md for long_description

with open("requirements.txt") as req_file:
    requirements = [line.strip() for line in req_file if line.strip() and not line.startswith('#')]

setup(
    author="Federico Gonzalez Itzik",
    author_email="fedelean.gon@gmail.com",
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    description="A Python package for building and managing data pipelines.",
    install_requires=requirements,
    license="MIT license",
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="pipelab pipeline data",
    name="pipelab",
    packages=find_packages(include=["pipelab", "pipelab.*"]),
    test_suite="tests",
    url="https://github.com/fedegonzalezit/pipelab",
    version="0.0.1",
    zip_safe=False,
)
