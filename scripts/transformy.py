def transform(data):
    print(f"Transforming {len(data)} records...")
    transformed = []
    for record in data:
        if record.get("status") == "active":
            transformed.append({
                "id": record["id"],
                "name": record["name"].upper(),
                "country": record.get("country"),
                "is_active": True,
            })
    print(f"Transformed: {len(transformed)} active records kept from {len(data)} total")
    return transformed
