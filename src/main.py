from pathlib import Path

import pandas
from pandas import DataFrame
from pandas.io.json._json import JsonReader
from progress.spinner import Spinner


def main(inputPath: Path, chunksize: int) -> None:
    absInputPath: Path = inputPath

    dfs: JsonReader[DataFrame] = pandas.read_json(
        path_or_buf=absInputPath, lines=True, chunksize=chunksize
    )

    with Spinner(message="Writing data to database...") as spinner:
        for df in dfs:
            print(df.columns)
            spinner.next()
            break


if __name__ == "__main__":
    main(inputPath=Path("../data/arxiv.jsonlines"), chunksize=10000)
