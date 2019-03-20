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

### Reading a Dataset into Memory
Use the `read_dataset` method on the `App` object with the dataset name. You 
will receieve a `Dataset` object which represents all of the events. We have
implemented a `count` method on the dataset to return the number of events. 
We automatically create a new constant column in the dataframe which holds the 
dataset's name.

### Slimming Dataset
The number of columns in the analysis has a dramatic impact on performance since
the dataframe is translated into a numpy array for processing in the 
UDF. We remove columns with the `select_columns` operation on the dataframe. You 
pass in only the column names needed for your analysis.

For technical reasons we always want a few descriptive columns included in
datasets. We will include these columns if they are not in the select 
request. The columns currently are:
* dataset
* run
* luminosityBlock
* event

For now, we are assuming that we are working with a CMS NanoAOD file. This 
will need more flexibility as we gain experience with new file formats.

### Dataset Operations
There are some useful methods on dataset (which are mostly just passed
through to Spark)

`count` - Returns the number of events in the dataset

`columns` - Returns a list of string column names

`columns_with_types` - Returns a list of tuples with (column name, data type)
 
`select_columns` - Returns a new dataframe with just the specified columns in it
(along with a few automatically included columns required for technical reasons)

`show` - Print to stdout a friendly table of the first few events

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
