from pathlib import Path

from sqlalchemy import (
    Column,
    Date,
    Engine,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    create_engine,
)


class DB:
    """
    A database management class for handling connections and schema
    definitions in a SQLite database.

    This class manages the creation of an engine connected to the specified
    SQLite database and defines the schema for documents and categories tables
    using SQLAlchemy's ORM capabilities.

    :param dbPath: The filesystem path to the SQLite database file.
    :type dbPath: Path
    """

    def __init__(self, dbPath: Path) -> None:
        """
        Initialize the DB class with a specific path to the SQLite database.

        Sets up the SQLAlchemy engine and binds metadata for schema definition.

        :param dbPath: The filesystem path to the SQLite database file.
        :type dbPath: Path
        """
        self.dbPath: Path = dbPath
        self.engine: Engine = create_engine(url=f"sqlite:///{self.dbPath}")
        self.metadata: MetaData = MetaData()

    def createTables(self) -> None:
        """
        Creates the database tables based on the defined metadata.

        Defines tables for storing documents and categories with appropriate
        relationships and constraints. This method checks first whether
        the tables already exist before creating them to avoid duplication.

        The tables defined are:
        - documents: Contains columns like id, authors, submitter, title, etc.
        - categories: Contains columns like id, arxiv_id (linked to documents),
            and category.
        """
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
            Column("update_date", Date),
            PrimaryKeyConstraint("id"),
        )

        _: Table = Table(
            "categories",
            self.metadata,
            Column("id", Integer),
            Column("arxiv_id", String),
            Column("category", String),
            PrimaryKeyConstraint("id"),
            ForeignKeyConstraint(
                columns=["arxiv_id"],
                refcolumns=["documents.id"],
            ),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)
