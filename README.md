# IRIS-HEP Spark Query Server
This service provides a scalable back end to columnar analysis of high energy
physics data.

## How to Test


### Verify Code Coverage
To verify that the unit tests are covering most of the code, run a coverage
report with 
```bash
coverage run -m unittest
coverage report
```