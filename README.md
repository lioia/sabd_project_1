# SABD Project 1

## Deployment

Download dataset into `data/dataset.csv`

Execute script:

```bash
./scripts/run.sh
```

## Development Environment Setup

Create Python virtual environment:

```bash
python -m venv .venv
source ./.venv/bin/activate
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
