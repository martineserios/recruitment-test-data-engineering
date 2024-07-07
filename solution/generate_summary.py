#!/usr/bin/env python3
import json

from sqlalchemy import create_engine, text

# Database connection
db_url = 'mysql+mysqlconnector://root:password@db:3306/mydb'
engine = create_engine(db_url)

# Query to get country-wise birth count
query = text("""
    SELECT p.country, COUNT(*) as birth_count
    FROM people pe
    JOIN places p ON pe.place_of_birth = p.city
    GROUP BY p.country
    ORDER BY birth_count DESC
""")

# Execute query and fetch results
with engine.connect() as connection:
    result = connection.execute(query)
    data = [{"country": row[0], "birth_count": row[1]} for row in result]

# Write to JSON file
with open('/app/output/summary_output.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("Summary data written to summary_output.json")