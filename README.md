# IRIS-HEP Spark Query Server
This service provides a scalable back end to columnar analysis of high energy
physics data.

[![Build Status](https://travis-ci.org/BenGalewsky/spark-hep-query.svg?branch=master)](https://travis-ci.org/BenGalewsky/spark-hep-query)
[![codecov](https://codecov.io/gh/BenGalewsky/spark-hep-query/branch/master/graph/badge.svg)](https://codecov.io/gh/BenGalewsky/spark-hep-query)

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
