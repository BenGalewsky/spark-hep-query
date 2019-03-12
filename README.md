# IRIS-HEP Spark Query Server
This service provides a scalable back end to columnar analysis of high energy
physics data.

[![Build Status](https://travis-ci.org/BenGalewsky/spark-hep-query.svg?branch=master)](https://travis-ci.org/BenGalewsky/spark-hep-query)
[![codecov](https://codecov.io/gh/BenGalewsky/spark-hep-query/branch/master/graph/badge.svg)](https://codecov.io/gh/BenGalewsky/spark-hep-query)

## Application
The main entrypoint for the query service is through an instance of the `App` 
class.  It is constructed from a `Config` object specification.

## Dataset Management
We don't want analyzers to have to think about individual ROOT files. 
Consequently we hide them behind a `Dataset` abstraction. While we can imagine 
more sophisticated implementations, our first draft for testing and development 
relies on a local csv file with dataset names and paths to the files. A 
dataset can be composed of multiple files. This is represented as rows sharing 
the same dataset name.

An instance of the dataset manager is created as part of the `config` object 
and passed into the application initializer.

The first operation on this dataset manager just returns a list with each of 
the dataset names.

## How to Test
We use `unittest` to verify the system. Run the tests as 
```bash
python3 -m unittest
```


### Verify Code Coverage
To verify that the unit tests are covering most of the code, run a coverage
report with 
```bash
coverage run -m unittest
coverage report
```
