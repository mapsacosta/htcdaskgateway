import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="htcdaskgateway",
    version="0.1.20",
    author="Maria P. Acosta F./Fermilab EAF project",
    author_email="macosta@fnal.gov",
    description="Launches a Dask Gateway cluster in K8s and joins HTCondor workers to it",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='Apache',
    url="https://github.com/mapsacosta/htcdaskgateway",
    packages=setuptools.find_packages(where='src'),
    package_dir={'': 'src', },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)
