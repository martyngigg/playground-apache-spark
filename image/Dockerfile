# Adds delta-lake storage to Python and some simple sample data
FROM jupyter/pyspark-notebook:spark-3.3.0

RUN pip install \
  delta-spark

COPY ./sampledata/ /sampledata/
