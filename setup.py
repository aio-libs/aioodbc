import os
import re

from setuptools import find_packages, setup

install_requires = ["pyodbc>=4.0.35"]


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


extras_require = {}


def read_version():
    regexp = re.compile(r'^__version__\W*=\W*"([\d.abrc]+)"')
    init_py = os.path.join(os.path.dirname(__file__), "aioodbc", "__init__.py")
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError("Cannot find version in aioodbc/__init__.py")


classifiers = [
    "License :: OSI Approved :: Apache Software License",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Operating System :: POSIX",
    "Environment :: Web Environment",
    "Development Status :: 3 - Alpha",
    "Topic :: Database",
    "Topic :: Database :: Front-Ends",
    "Framework :: AsyncIO",
]


setup(
    name="aioodbc",
    version=read_version(),
    description=("ODBC driver for asyncio."),
    long_description="\n\n".join((read("README.rst"), read("CHANGES.txt"))),
    classifiers=classifiers,
    platforms=["POSIX"],
    author="Nikolay Novik",
    author_email="nickolainovik@gmail.com",
    url="https://github.com/aio-libs/aioodbc",
    download_url="https://pypi.python.org/pypi/aioodbc",
    license="Apache 2",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=install_requires,
    extras_require=extras_require,
    include_package_data=True,
)
