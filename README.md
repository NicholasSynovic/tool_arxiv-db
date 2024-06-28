# arXiv DB

> Converting the Kaggle hosted arXiv JSON metadata dump to SQLite3 database

## Table of Contents

- [arXiv DB](#arxiv-db)
  - [Table of Contents](#table-of-contents)
  - [About](#about)
  - [How to Install](#how-to-install)
    - [Dependencies](#dependencies)
    - [Installation steps](#installation-steps)
  - [How to Run](#how-to-run)

## About

This project is meant to convert the arXiv metadata dataset hosted on Kaggle to
a SQLite3 database in order to facilitate advanced searching, parsing, and
utilization.

## How to Install

This code was tested to run on x86-64 Linux

### Dependencies

- `Python 3.10`
- `bash`
- `jq`

### Installation steps

- Create the Python venv and install the tool

```shell
make create-dev
source env/bin/activate
make build
```

- Download the dataset with `scripts/download.bash`

```shell
usage: ./download.bash [OPTIONS]

OPTIONS:

        -a --kaggle-api-key:         Kaggle API key
        -o --output:                 Directory to save the file to [default:../data]
        -u --kaggle-username:        Kaggle username

        -? --help  :  usage
```

- Convert the `JSON` file to a `JSON Lines` file with `scripts/json2jl.bash`

```shell
usage: ./json2jl.bash [OPTIONS]

OPTIONS:

        -i --input:                  Input JSON file
        -o --output:                 Output JSON Lines file

        -? --help  :  usage
```

## How to Run

- Read the `JSON Lines` file into a `SQLite3 database` with `src/main.py`

```shell
Usage: main.py [OPTIONS]

  Process JSON data from an input file, transforming and loading it into a SQL
  database in chunks.

  This function takes a JSON file, reads it in chunks, processes each chunk to
  extract relevant data, and appends it to the specified database. It checks
  for the existence and validity of input and output paths, and raises
  exceptions if issues are found. It also ensures all unique IDs are processed
  only once and handles database writing through specific table mappings.

  :param inputPath: The file system path to the input JSON file. :type
  inputPath: Path :param outputPath: The file system path where the output
  database is located. :type outputPath: Path :param chunkSize: The number of
  records to process in each chunk. :type chunkSize: int :raises
  FileNotFoundError: If the input file does not exist. :raises
  FileExistsError: If the output file already exists. :raises ValueError: If
  the chunk size is less than 1.

Options:
  -i, --input PATH         Path to arXiv JSON Lines file  [required]
  -o, --output PATH        Path to output SQLite3 database  [required]
  -c, --chunksize INTEGER  Number of rows to read from the JSON Lines file
                           [default: 10000]
  --help                   Show this message and exit.

Options:
  -i, --input PATH         Path to arXiv JSON Lines file  [required]
  -o, --output PATH        Path to output SQLite3 database  [required]
  -c, --chunksize INTEGER  Number of rows to read from the JSON Lines file
                           [default: 10000]
  --help                   Show this message and exit.
```
