from setuptools import setup, find_packages

setup(
    name='PySparkAssignment',
    version='1.0',
    author='Kamil Moszczyc',
    author_email='kamil.moszczyc@capgemini.com',
    url='https://github.com/KMoszczyc/PySpark-Assignment',
    description='PySpark assignment with loading, filtering, joining and saving 2 csv files.',
    license="MIT license",
    packages=find_packages(exclude=["test"]),  # Don't include test directory in binary distribution
    install_requires='requirements.txt',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]  # Update these accordingly
)
