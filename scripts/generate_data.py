import csv
import random
import sys
import datetime

def generate_data(filename, num_rows):
    """Génère un fichier CSV avec des données de vente aléatoires."""
    products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones', 'Phone', 'Tablet', 'Charger', 'Cable', 'Stand']
    
    print(f"Génération de {num_rows} lignes dans {filename}...")
    
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['id', 'product', 'amount', 'date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        for i in range(1, num_rows + 1):
            writer.writerow({
                'id': i,
                'product': random.choice(products),
                'amount': round(random.uniform(10.0, 2000.0), 2),
                'date': (datetime.date(2023, 1, 1) + datetime.timedelta(days=random.randint(0, 364))).isoformat()
            })
            
            if i % 100000 == 0:
                print(f"  {i} lignes générées...")
                
    print(f"✓ Fichier {filename} généré avec succès.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python generate_data.py <output_file> <num_rows>")
        sys.exit(1)
        
    output_file = sys.argv[1]
    try:
        num_rows = int(sys.argv[2])
    except ValueError:
        print("Erreur: num_rows doit être un entier.")
        sys.exit(1)
        
    generate_data(output_file, num_rows)
