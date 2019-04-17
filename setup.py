import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gordium",
    version="0.1.0",
    author="Joe Downs",
    author_email="Joe.Downs@jhuapl.edu",
    description="A Neo4j-friendly library for graph metrics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aplbrain/gordium",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
