import os
import setuptools

_here = os.path.abspath(os.path.dirname(__file__))

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="weather_etl",
    version='0.0.1',
    author="Ian Buttimer",
    author_email="author@example.com",
    description="ETL for Weather Analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ib-da-ncirl/weather_analysis",
    license='MIT',
    packages=setuptools.find_packages(),
    install_requires=[
        'setuptools>=47.3.1',
        'pandas>=1.0.5',
        'requests>=2.24.0',
        'happybase>=1.2.0',
        'pyyaml>=5.3.1'
    ],
    dependency_links=[
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
