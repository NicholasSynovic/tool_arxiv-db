from pathlib import Path

from sqlalchemy import (
    Column,
    Date,
    Engine,
    MetaData,
    String,
    Table,
    create_engine,
)


class DB:
    def __init__(self, dbPath: Path) -> None:
        self.dbPath: Path = dbPath
        self.engine: Engine = create_engine(url=f"sqlite:///{self.dbPath}")
        self.metadata: MetaData = MetaData()

    def createTables(self) -> None:
        _: Table = Table(
            "documents",
            self.metadata,
            Column("id", String),
            Column("authors", String),
            Column("submitter", String),
            Column("title", String),
            Column("comments", String),
            Column("journal-ref", String),
            Column("doi", String),
            Column("report-no", String),
            Column("license", String),
            Column("abstract", String),
            Column("versions", String),
            Column("update_date", Date),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)
