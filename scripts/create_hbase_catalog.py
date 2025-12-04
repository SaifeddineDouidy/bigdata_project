"""
Génère le fichier catalog JSON pour le connecteur Spark-HBase
"""
import json
import os

# Configuration
HBASE_TABLE = "sales"
HBASE_CF = "cf"
HBASE_ROWKEY = "id"

# Catalog JSON pour Spark-HBase
catalog = {
    "table": {
        "namespace": "default",
        "name": HBASE_TABLE
    },
    "rowkey": HBASE_ROWKEY,
    "columns": {
        "id": {
            "cf": "rowkey",
            "col": HBASE_ROWKEY,
            "type": "string"
        },
        "product": {
            "cf": HBASE_CF,
            "col": "product",
            "type": "string"
        },
        "amount": {
            "cf": HBASE_CF,
            "col": "amount",
            "type": "string"
        },
        "date": {
            "cf": HBASE_CF,
            "col": "date",
            "type": "string"
        }
    }
}

# Sauvegarder le catalog
output_file = "hbase_catalog.json"
with open(output_file, 'w') as f:
    json.dump(catalog, f, indent=2)

print(f"Catalog JSON créé: {output_file}")
print("\nContenu:")
print(json.dumps(catalog, indent=2))

