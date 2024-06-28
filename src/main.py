from math import ceil
from pathlib import Path
from typing import Literal

import click
import pandas
from pandas import DataFrame
from pandas.io.json._json import JsonReader
from progress.bar import Bar
from pyfs import isFile, resolvePath
from sqlalchemy.exc import IntegrityError

from src.db import DB


def countLines(filepath: Path) -> int:
    """
    Count the number of lines in a specified file.

    This function opens a file in binary read mode and counts the lines
    by iterating over each line. Using binary mode ('rb') to avoid
    potential issues with newline characters across different platforms.

    :param filepath: The path to the file for which to count the lines.
    :type filepath: Path
    :return: The number of lines in the file.
    :rtype: int

    Example usage:

    >>> from pathlib import Path
    >>> filepath = Path('/path/to/your/file.txt')
    >>> countLines(filepath)
    42
    """
    print(f"Counting lines in {filepath} ...")
    return sum(1 for _ in open(file=filepath, mode="rb"))


def computeIterations(chunkSize: int, lineCount: int) -> int:
    """
    Calculate the number of iterations required to process lines in
    specified chunk sizes.

    Given the total number of lines and the number of lines to be processed
    per iteration (chunk size), this function computes how many iterations
    will be necessary to process all lines.

    :param chunkSize: The number of lines to process in each iteration.
    :type chunkSize: int
    :param lineCount: The total number of lines to be processed.
    :type lineCount: int
    :return: The number of iterations required to process all lines.
    :rtype: int

    Example usage:

    >>> computeIterations(chunkSize=100, lineCount=450)
    5
    """
    return ceil(lineCount / chunkSize)


def explodeColumn(
    df: DataFrame,
    indexColumn: str,
    column: str,
) -> DataFrame:
    """
    Explode a column in the DataFrame where each entry in the specified
    column is split by space and then expanded into multiple rows.

    This function sets a specified column as the index, splits
    another specified column by spaces, explodes this column into multiple
    rows for each element in the split lists, and then resets the index.

    :param df: The DataFrame to process.
    :type df: DataFrame
    :param indexColumn: The name of the column to set as the index
    before exploding.
    :type indexColumn: str
    :param column: The name of the column to explode.
    :type column: str
    :return: A new DataFrame with the specified column exploded into
    multiple rows.
    :rtype: DataFrame

    Example usage:

    >>> import pandas as pd
    >>> data = {'id': [1, 2], 'text': ['hello world', 'test pandas']}
    >>> df = pd.DataFrame(data)
    >>> exploded_df = explodeColumn(df, 'id', 'text')
    >>> print(exploded_df)
       id     text
    0   1    hello
    1   1    world
    2   2     test
    3   2   pandas
    """
    df.set_index(keys=indexColumn, inplace=True)

    df = (
        df.apply(lambda x: x.str.split(" "))
        .explode(column=column)
        .reset_index()
    )

    return df


def toSQL(
    table: Literal["documents", "categories"],
    df: DataFrame,
    db: DB,
    includeIndex: bool = False,
) -> None:
    """
    Append a DataFrame to a SQL table, handling duplicate primary key issues
    by appending only new rows.

    The function attempts to append the DataFrame to a specified SQL table.
    If an IntegrityError occurs due to duplicate primary key entries,
    it fetches the existing IDs from the table, filters out the duplicate
    entries from the DataFrame, and attempts the append operation again.

    :param table: The name of the SQL table to append to. Must be
    'documents' or 'categories'.
    :type table: Literal["documents", "categories"]
    :param df: The DataFrame to append.
    :type df: DataFrame
    :param db: An object representing database configuration and connection.
    It must have an 'engine' attribute.
    :type db: DB
    :param includeIndex: Whether to include the DataFrame's index as a
    column in the SQL table. Defaults to False.
    :type includeIndex: bool
    :raises IntegrityError: Raised if the second append operation fails due
    to duplicate keys.

    Example usage:

    >>> import pandas as pd
    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy_utils import database_exists, create_database
    >>> class DB:
    ...     def __init__(self, url):
    ...         self.engine = create_engine(url)
    ...         if not database_exists(self.engine.url):
    ...             create_database(self.engine.url)
    >>> db = DB('sqlite:///example.db')
    >>> data = {'id': [1, 2], 'text': ['sample1', 'sample2']}
    >>> df = pd.DataFrame(data)
    >>> toSQL('documents', df, db, includeIndex=False)
    """
    try:
        df.to_sql(
            name=table,
            con=db.engine,
            if_exists="append",
            index=includeIndex,
            index_label="id",
        )
    except IntegrityError:
        existingSQLKeysDF: DataFrame = pandas.read_sql(
            sql=f"SELECT id FROM {table}",  # nosec
            con=db.engine,
        )

        uniqueKeysDF = df[~df["id"].isin(values=existingSQLKeysDF["id"])]

        uniqueKeysDF.to_sql(
            name=table,
            con=db.engine,
            if_exists="append",
            index=False,
        )


@click.command()
@click.option(
    "-i",
    "--input",
    "inputPath",
    help="Path to arXiv JSON Lines file",
    type=Path,
    required=True,
)
@click.option(
    "-o",
    "--output",
    "outputPath",
    help="Path to output SQLite3 database",
    type=Path,
    required=True,
)
@click.option(
    "-c",
    "--chunksize",
    "chunkSize",
    help="Number of rows to read from the JSON Lines file",
    type=int,
    default=10000,
    show_default=True,
)
def main(inputPath: Path, outputPath: Path, chunkSize: int) -> None:
    """
    Process JSON data from an input file, transforming and loading it into
    a SQL database in chunks.

    This function takes a JSON file, reads it in chunks, processes each
    chunk to extract relevant data, and appends it to the specified database.
    It checks for the existence and validity of input and output paths,
    and raises exceptions if issues are found. It also ensures all unique
    IDs are processed only once and handles database writing through
    specific table mappings.

    :param inputPath: The file system path to the input JSON file.
    :type inputPath: Path
    :param outputPath: The file system path where the output database is
    located.
    :type outputPath: Path
    :param chunkSize: The number of records to process in each chunk.
    :type chunkSize: int
    :raises FileNotFoundError: If the input file does not exist.
    :raises FileExistsError: If the output file already exists.
    :raises ValueError: If the chunk size is less than 1.
    """
    absInputPath: Path = resolvePath(path=inputPath)
    absOutputPath: Path = resolvePath(path=outputPath)

    if not isFile(path=absInputPath):
        raise FileNotFoundError(absInputPath)

    if isFile(path=absOutputPath):
        raise FileExistsError(absOutputPath)

    if chunkSize < 1:
        raise ValueError(chunkSize)

    uniqueIDs: set[str] = {}
    previousCategoriesDFMaxIndex: int = 0

    db: DB = DB(dbPath=absOutputPath)
    db.createTables()

    lineCount: int = countLines(filepath=absInputPath)
    iterationCount: int = computeIterations(
        chunkSize=chunkSize,
        lineCount=lineCount,
    )

    dfs: JsonReader[DataFrame] = pandas.read_json(
        path_or_buf=absInputPath,
        lines=True,
        chunksize=chunkSize,
    )

    with Bar(message="Writing data to database...", max=iterationCount) as bar:
        _df: DataFrame
        for _df in dfs:
            df: DataFrame = _df[~_df["id"].isin(values=uniqueIDs)]

            categoriesDF = explodeColumn(
                df=df[["id", "categories"]],
                indexColumn="id",
                column="categories",
            )
            categoriesDF.rename(
                columns={
                    "id": "arxiv_id",
                    "categories": "category",
                },
                inplace=True,
            )
            categoriesDF.index.names = ["id"]
            categoriesDF.index += previousCategoriesDFMaxIndex

            # TODO: Add the following tables
            # versionsDF: DataFrame = df[["id", "versions"]]
            # authorsDF: DataFrame = df[["id", "authors_parsed"]]

            df.drop(
                columns=["categories", "versions", "authors_parsed"],
                inplace=True,
            )

            toSQL(table="documents", df=df, db=db)
            toSQL(
                table="categories",
                df=categoriesDF,
                db=db,
                includeIndex=True,
            )

            uniqueIDs |= df["id"]
            previousCategoriesDFMaxIndex = categoriesDF.index.max() + 1

            bar.next()


if __name__ == "__main__":
    main()
