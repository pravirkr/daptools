import codecs
import os
from setuptools import setup


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    raise RuntimeError("Unable to find version string.")


def append_path(dirs_list, *args):
    entry = os.path.normpath(os.path.join(*args))
    if os.path.isdir(entry):
        if entry not in dirs_list:
            dirs_list.append(entry)


package_version = get_version("daptools/__init__.py")

setup(
    name="daptools",
    version=package_version,
)
