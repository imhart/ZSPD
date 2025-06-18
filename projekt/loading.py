import os
import shutil
import csv
import oracledb  


dsn = oracledb.makedsn("localhost", 1521, service_name="XEPDB1")
connection = oracledb.connect(user="hr", password="hr", dsn=dsn)
cursor = connection.cursor()

input_folder = "data/incoming"
archive_folder = "data/archive"

def validate_row(row):
    try:
        # int(row['employee_id'])  
        # float(row['salary'])    
        return True
    except:
        return False

def load_csv(filename):
    full_path = os.path.join(input_folder, filename)
    with open(full_path, encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if validate_row(row):
                try:
                    cursor.execute("""
                        INSERT INTO EMPLOYEES (employee_id, first_name, last_name, email, hire_date, job_title, salary)
                        VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'), :6, :7)
                    """, (
                        row['employee_id'],
                        row['first_name'],
                        row['last_name'],
                        row['email'],
                        row['hire_date'],
                        row['job_title'],
                        row['salary']
                    ))
                except Exception as e:
                    print(f"Błąd przy wstawianiu rekordu: {e}")
            else:
                print(f"Nieprawidłowe dane: {row}")
    connection.commit()
    # Archiwizacja
    shutil.move(full_path, os.path.join(archive_folder, filename))

# Załaduj wszystkie pliki CSV
for file in os.listdir(input_folder):
    if file.endswith(".csv"):
        load_csv(file)

cursor.close()
connection.close()
