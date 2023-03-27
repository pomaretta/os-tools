#!/usr/bin/env python3

import argparse
import csv
import os
import sqlite3 as sql
from typing import List


def return_data(
    path: str,
    delimiter: str = ",",
    quotechar: str | None = '"',
    escapechar: str | None = None,
    doublequote: bool = True,
    skipinitialspace: bool = False,
    lineterminator: str = "\r\n",
    strict: bool = False,
):
    with open(path, "r") as f:
        reader = csv.reader(
            f,
            delimiter=delimiter,
            quotechar=quotechar,
            escapechar=escapechar,
            doublequote=doublequote,
            skipinitialspace=skipinitialspace,
            lineterminator=lineterminator,
            strict=strict,
        )
        next(reader)
        yield from reader


def read_file(
    path: str,
    delimiter: str = ",",
    quotechar: str | None = '"',
    escapechar: str | None = None,
    doublequote: bool = True,
    skipinitialspace: bool = False,
    lineterminator: str = "\r\n",
    strict: bool = False,
) -> tuple[List[str], List[List]]:
    """Read CSV file and return header and data"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    # Read only the header
    with open(path, "r") as f:
        reader = csv.reader(
            f,
            delimiter=delimiter,
            quotechar=quotechar,
            escapechar=escapechar,
            doublequote=doublequote,
            skipinitialspace=skipinitialspace,
            lineterminator=lineterminator,
            strict=strict,
        )
        header = next(reader)
    return header, return_data(path)


def main(args: argparse.Namespace) -> None:

    db = sql.connect(
        database=os.path.join(args.output, args.database), detect_types=sql.PARSE_DECLTYPES
    )

    for path in args.csv:

        header, data = read_file(args.csv[0])
        table_name = os.path.basename(path).split(".")[0]

        # Create table if it doesn't exist
        db.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{x} TEXT' for x in header])})"
        )
        # Insert data
        db.executemany(
            f"INSERT INTO {os.path.splitext(os.path.basename(path))[0]} VALUES ({', '.join(['?'] * len(header))})",
            data,
        )
        db.commit()

    db.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Convert CSV to SQL",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "csv", help="CSV file to convert", type=os.path.abspath, nargs=argparse.ONE_OR_MORE
    )

    parser.add_argument(
        "--output",
        help="Output file to create",
        type=os.path.abspath,
        default=os.path.join(os.path.expanduser("~"), "Documents", "packages", "csv2sql"),
    )

    parser.add_argument(
        "--database",
        help="Database file to create",
        type=str,
        default="default.db",
    )

    args = parser.parse_args()

    main(args)
