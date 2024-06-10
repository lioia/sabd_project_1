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
- setup Spark (starts master and three workers)

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

## Folder Structure

```plaintext
├── data                        folder containing the dataset
│   └── dataset.csv                 the dataset
├── grafana                     folder for Grafana configuration
│   ├── dashboards                  Grafana Dashboards
│   │   ├── query_1.json
│   │   ├── query_2_1.json
│   │   └── query_2_2.json
│   ├── dashboard.yaml              Provisioning for the Dashboards
│   └── datasource.yaml             Provisioning for the Mongo connection
├── nifi                        folder for NiFi automation
│   ├── hdfs                        HDFS configuration
│   │   ├── core-site.xml
│   │   └── hdfs-site.xml
│   ├── main.py                     Helper Script to automatically deploy NiFi
│   ├── nifi_api.py                 REST API helper
│   └── nifi_template.xml           NiFi Template
├── Report                      folder for the Report
│   └── Report.pdf
├── Results                     folder for the Results
│   ├── query_1.csv
│   └── query_2.csv
├── scripts                     folder containing the execution scripts
│   ├── clean.sh                    Clean-up environment
│   ├── run.sh                      Run Spark Application
│   └── setup.sh                    Setup environment
├── src                         folder containing the actual application
│   ├── main.py                     Application Entrypoint
│   └── spark                       Spark Specific Code
│       ├── analysis.py                 Analysis Command
│       ├── df.py                       DataFrame implementation
│       ├── __init__.py
│       ├── rdd.py                      RDD implementation
│       ├── spark.py                    Spark Entrypoint
│       └── utils.py                    Helper functions
├── compose.yaml                Docker Compose configuration
├── example.env                 Environment Variables setup
├── README.md                   This file
└── requirements.txt            Python packages for Code Editor setup and NiFi script
```
