from math import ceil
from pathlib import Path
from typing import Literal

import pandas
from pandas import DataFrame
from pandas.io.json._json import JsonReader
from progress.bar import Bar
from sqlalchemy.exc import IntegrityError

from src.db import DB


def countLines(filepath: Path) -> int:
    print(f"Counting lines in {filepath} ...")
    return sum(1 for _ in open(file=filepath, mode="rb"))


def computeIterations(chunkSize: int, lineCount: int) -> int:
    return ceil(lineCount / chunkSize)


def explodeColumn(df: DataFrame, indexColumn: str, column: str) -> DataFrame:
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


def main(inputPath: Path, outputPath: Path, chunkSize: int) -> None:
    absInputPath: Path = inputPath
    absOutputPath: Path = outputPath

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
    main(
        inputPath=Path("../data/arxiv.jsonlines"),
        outputPath=Path("../data/arxiv.db"),
        chunkSize=10000,
    )
