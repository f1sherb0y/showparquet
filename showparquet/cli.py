import argparse
import sys

import pyarrow.parquet as pq
from tabulate import tabulate


def show_schema(file_path):
    """
    Reads a Parquet file's schema and prints it in a table format.

    Args:
        file_path (str): The path to the Parquet file.
    """
    try:
        # Read only the schema from the Parquet file for efficiency.
        # This avoids loading the entire dataset into memory.
        schema = pq.read_schema(file_path)

        headers = ["idx", "name", "physical type", "logical type"]
        table_data = []

        for i, field in enumerate(schema):
            # The logical_type can sometimes be None, so we handle it gracefully.
            # `str(field.logical_type)` will produce 'NONE' for NoneType.
            logical_type_str = str(field.logical_type)

            table_data.append(
                [i, field.name, str(field.physical_type), logical_type_str]
            )

        if not table_data:
            print(f"No columns found in the schema of '{file_path}'.")
            return

        # Use the tabulate library to create a nicely formatted grid table.
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred while reading the Parquet file: {e}", file=sys.stderr)
        print("Please ensure it is a valid Parquet file.", file=sys.stderr)
        sys.exit(1)


def main():
    """
    Main function to parse command-line arguments and call the schema viewer.
    """
    parser = argparse.ArgumentParser(
        description="Display the schema of a Parquet file in a tabular format."
    )
    parser.add_argument("parquet_file", type=str, help="The path to the Parquet file.")
    args = parser.parse_args()

    show_schema(args.parquet_file)


if __name__ == "__main__":
    main()
