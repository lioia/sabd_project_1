# SABD Project 1

## Deployment

Download dataset into `data/dataset.csv`

### Environment Setup

Execute script:

```bash
./scripts/setup.sh
```

This script will:

- launch the Docker containers
- setup HDFS (format namenode and start the distributed filesystem)
- run NiFi setup script
- setup Spark (starts master and two workers)

### Launching Application

Execute script:

```bash
./scripts/run.sh [command]
```

Where `command` can be:

- `save`: executes the entire pipeline using RDD API and saves the output;
  additional arguments:
  - `location`: where to save the output (`hdfs` or `mongo`)
- `analysis`: executes the pipeline using NiFi comparing the two APIs (RDD and
  DataFrame) for all the different file formats (`avro`, `parquet`, `csv`)
- `check`: executes the RDD and DataFrame API checking whether the results are
  the same

## Development Environment Setup

Create Python virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

Access Mongo:

- Mongo-Express at: <localhost:8081>
- Using `mongosh`:

  ```bash
  docker container exec -it mongo /bin/bash
  mongosh -u $MONGO_USERNAME -p $MONGO_PASSWORD mongo:27017
  ```

## Queries

### Query 1

For each day, for each vault (refer to the vault id field), calculate the total
number of failures. Determine the list of vaults that have experienced exactly
4, 3 and 2 failures.

### Query 2

Calculate the ranking of the 10 hard disk models that have suffered the greatest
number of failures. The ranking must report the hard disk model and the total
number of failures suffered by the hard disks of that specific model.

Calculate a second ranking of the 10 vaults that recorded the most failures.
For each vault, report the number of failures and the list (without repetitions)
of hark disk models subject to at least one failure.
