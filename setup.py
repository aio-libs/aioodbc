import os

from setuptools import find_packages, setup

install_requires = ["pyodbc>=5.0.1"]


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


extras_require = {}


classifiers = [
    "License :: OSI Approved :: Apache Software License",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Operating System :: POSIX",
    "Environment :: Web Environment",
    "Development Status :: 3 - Alpha",
    "Topic :: Database",
    "Topic :: Database :: Front-Ends",
    "Framework :: AsyncIO",
]

project_urls = {
    "Website": "https://github.com/jettify/uddsketch",
    "Documentation": "https://uddsketch.readthedocs.io",
    "Issues": "https://github.com/jettify/uddsketch/issues",
}


setup(
    name="aioodbc",
    description=("ODBC driver for asyncio."),
    long_description="\n\n".join((read("README.rst"), read("CHANGES.txt"))),
    long_description_content_type="text/x-rst",
    classifiers=classifiers,
    platforms=["POSIX"],
    author="Nikolay Novik",
    author_email="nickolainovik@gmail.com",
    url="https://github.com/aio-libs/aioodbc",
    download_url="https://pypi.python.org/pypi/aioodbc",
    license="Apache 2",
    packages=find_packages(exclude=("tests",)),
    python_requires=">=3.7",
    install_requires=install_requires,
    setup_requires=[
        "setuptools>=45",
        "setuptools_scm",
        "setuptools_scm_git_archive",
        "wheel",
    ],
    extras_require=extras_require,
    include_package_data=True,
    project_urls=project_urls,
    use_scm_version=True,
)
