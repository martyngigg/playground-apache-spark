# Apache Spark Playground

Uses the `pyspark-notebook` Docker image to create a local
Jupyter Notebook instance that can interact with a local Spark instance or
a remote Spark cluster.

## Getting Started

To start the notebook server first clone the repository and then run:

```sh
docker-compose up --detach
```

Wait a few seconds and run

```sh
docker-compose logs pyspark-nb | grep 127.0.0.1
```

to discover the address to access the running notebook. Open this address
in a browser.

## Examples

The [examples](./examples) directory gives demos of interacting
with a CSV data set through the PySpark API.

To use the examples in the notebook they will need to be copied to the container
once volume once it is created. Run

```sh
docker-compose cp examples/ pyspark-nb:/home/jovyan/
```

## Datasets

For example datasets to experiment with see:

- Kaggle: <https://www.kaggle.com/>
- Our World in Data (OWID): <https://github.com/owid/owid-datasets>
