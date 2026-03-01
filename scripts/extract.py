def extract():
    print("Extracting data...")
    data = [
        {"id": 1, "name": "Partner A", "status": "active",   "country": "DE"},
        {"id": 2, "name": "Partner B", "status": "inactive", "country": "US"},
        {"id": 3, "name": "Partner C", "status": "active",   "country": "UK"},
    ]
    print(f"Extracted {len(data)} records")
    return data