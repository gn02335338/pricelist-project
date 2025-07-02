import argparse
from pricelist_change_exportV3 import upload_pricelist_to_snowflake


def main():
    parser = argparse.ArgumentParser(description="Test uploading pricelist to Snowflake")
    parser.add_argument("excel_file", help="Path to pricelist Excel file")
    args = parser.parse_args()

    try:
        table_name = upload_pricelist_to_snowflake(args.excel_file)
    except ValueError as e:
        print(f"Upload skipped: {e}")
        return
    except Exception as e:
        print(f"Upload failed: {e}")
        return

    print(f"Uploaded to table: {table_name}")


if __name__ == "__main__":
    main()
