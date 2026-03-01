def load(data, partition_date):
    print(f"Loading {len(data)} records for partition: {partition_date}")
    print(f"Step 1: DELETE FROM partners WHERE processed_date = '{partition_date}'")
    for record in data:
        print(f"Step 2: INSERT {record}")
    print("Load complete.")
