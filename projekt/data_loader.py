import os
import json
import csv
import shutil
import logging
import oracledb
from datetime import datetime
from typing import Dict, List, Any
import requests
from pathlib import Path

# Konfiguracja logowania
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_loader.log'),
        logging.StreamHandler()
    ]
)

class DataLoader:
    def __init__(self):
        # Konfiguracja połączenia z bazą Oracle
        self.dsn = oracledb.makedsn(
            host="213.184.8.44",
            port=1521,
            service_name="orcl"
        )
        self.connection = oracledb.connect(
            user="inf2ns_nieznanskid",
            password="Inrwe124",
            dsn=self.dsn
        )
        self.cursor = self.connection.cursor()
        
        # Konfiguracja ścieżek
        self.base_dir = Path("data")
        self.input_dir = self.base_dir / "incoming"
        self.archive_dir = self.base_dir / "archive"
        self.error_dir = self.base_dir / "error"
        
        # Tworzenie katalogów jeśli nie istnieją
        for directory in [self.input_dir, self.archive_dir, self.error_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    def validate_product(self, data: Dict[str, Any]) -> bool:
        """Walidacja danych produktu"""
        try:
            required_fields = ['product_name', 'unit_price']
            if not all(field in data for field in required_fields):
                return False
            
            if not isinstance(data['product_name'], str) or len(data['product_name']) > 100:
                return False
                
            if not isinstance(float(data['unit_price']), (int, float)) or float(data['unit_price']) < 0:
                return False
                
            return True
        except Exception as e:
            logging.error(f"Błąd walidacji produktu: {e}")
            return False

    def validate_customer(self, data: Dict[str, Any]) -> bool:
        """Walidacja danych klienta"""
        try:
            required_fields = ['company_name']
            if not all(field in data for field in required_fields):
                return False
            
            if not isinstance(data['company_name'], str) or len(data['company_name']) > 100:
                return False
                
            if 'email' in data and not self._is_valid_email(data['email']):
                return False
                
            return True
        except Exception as e:
            logging.error(f"Błąd walidacji klienta: {e}")
            return False

    def _is_valid_email(self, email: str) -> bool:
        """Prosta walidacja adresu email"""
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))

    def load_csv(self, file_path: Path) -> None:
        """Ładowanie danych z pliku CSV"""
        try:
            with open(file_path, encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if self.validate_product(row):
                        self._insert_product(row)
                    elif self.validate_customer(row):
                        self._insert_customer(row)
                    elif True: #TODO self.validate_employee(row):
                        self._insert_employee(row)
                    else:
                        logging.warning(f"Nieprawidłowe dane w pliku {file_path}: {row}")
            
            self._archive_file(file_path)
        except Exception as e:
            logging.error(f"Błąd podczas ładowania pliku CSV {file_path}: {e}")
            self._move_to_error(file_path)

    def load_json(self, file_path: Path) -> None:
        """Ładowanie danych z pliku JSON"""
        try:
            with open(file_path, encoding='utf-8') as jsonfile:
                data = json.load(jsonfile)
                if isinstance(data, list):
                    for item in data:
                        if self.validate_product(item):
                            self._insert_product(item)
                        elif self.validate_customer(item):
                            self._insert_customer(item)
                        else:
                            logging.warning(f"Nieprawidłowe dane w pliku {file_path}: {item}")
                else:
                    logging.error(f"Nieprawidłowy format JSON w pliku {file_path}")
            
            self._archive_file(file_path)
        except Exception as e:
            logging.error(f"Błąd podczas ładowania pliku JSON {file_path}: {e}")
            self._move_to_error(file_path)

    def fetch_from_api(self, url: str, data_type: str) -> None:
        """Pobieranie danych z API"""
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            if data_type == 'products':
                for item in data:
                    if self.validate_product(item):
                        self._insert_product(item)
            elif data_type == 'customers':
                for item in data:
                    if self.validate_customer(item):
                        self._insert_customer(item)
                        
        except Exception as e:
            logging.error(f"Błąd podczas pobierania danych z API: {e}")

    def _insert_product(self, data: Dict[str, Any]) -> None:
        """Wstawianie produktu do bazy danych"""
        try:
            self.cursor.execute("""
                INSERT INTO products (product_name, unit_price, category_id, supplier_id)
                VALUES (:1, :2, :3, :4)
            """, (
                data['product_name'],
                float(data['unit_price']),
                data.get('category_id'),
                data.get('supplier_id')
            ))
            self.connection.commit()
        except Exception as e:
            logging.error(f"Błąd podczas wstawiania produktu: {e}")
            self.connection.rollback()

    def _insert_customer(self, data: Dict[str, Any]) -> None:
        """Wstawianie klienta do bazy danych"""
        try:
            self.cursor.execute("""
                INSERT INTO customers (company_name, contact_name, email, phone, address, city, country)
                VALUES (:1, :2, :3, :4, :5, :6, :7)
            """, (
                data['company_name'],
                data.get('contact_name'),
                data.get('email'),
                data.get('phone'),
                data.get('address'),
                data.get('city'),
                data.get('country')
            ))
            self.connection.commit()
        except Exception as e:
            logging.error(f"Błąd podczas wstawiania klienta: {e}")
            self.connection.rollback()
            
    def _insert_employee(self, data: Dict[str, Any]) -> None:
        """Wstawianie pracownika do bazy danych"""
        try:
            self.cursor.execute("""
                INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_title, salary)
                VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'), :6, :7)
            """, (
                int(data['employee_id']),
                data['first_name'],
                data['last_name'],
                data.get('email'),
                data['hire_date'],
                data['job_title'],
                float(data['salary'])
            ))
            self.connection.commit()
        except Exception as e:
            logging.error(f"Błąd podczas wstawiania pracownika: {e}")
            self.connection.rollback()


    def _archive_file(self, file_path: Path) -> None:
        """Archiwizacja przetworzonego pliku"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
            shutil.move(str(file_path), str(self.archive_dir / archive_name))
            logging.info(f"Zarchiwizowano plik: {file_path}")
        except Exception as e:
            logging.error(f"Błąd podczas archiwizacji pliku {file_path}: {e}")

    def _move_to_error(self, file_path: Path) -> None:
        """Przenoszenie pliku z błędami do katalogu error"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
            shutil.move(str(file_path), str(self.error_dir / error_name))
            logging.info(f"Przeniesiono plik z błędami: {file_path}")
        except Exception as e:
            logging.error(f"Błąd podczas przenoszenia pliku {file_path}: {e}")

    def process_all_files(self) -> None:
        """Przetwarzanie wszystkich plików w katalogu wejściowym"""
        for file_path in self.input_dir.glob('*'):
            if file_path.suffix.lower() == '.csv':
                self.load_csv(file_path)
            elif file_path.suffix.lower() == '.json':
                self.load_json(file_path)

    def close(self) -> None:
        """Zamykanie połączenia z bazą danych"""
        self.cursor.close()
        self.connection.close()

    def call_add_product(self, product_name, category_id, supplier_id, unit_price, units_in_stock=0, reorder_level=0):
        """Wywołanie procedury add_product za pomocą execute"""
        result = self.cursor.var(str)
        self.cursor.execute("""
            BEGIN
                add_product(:1, :2, :3, :4, :5, :6, :7);
            END;
        """, [
            product_name,
            category_id,
            supplier_id,
            unit_price,
            units_in_stock,
            reorder_level,
            result
        ])
        self.connection.commit()
        return result.getvalue()

    def call_delete_product(self, product_id):

        result = self.cursor.var(str)
        self.cursor.execute("""
            BEGIN
                delete_product(:1, :2);
            END;
        """, [
            product_id,
            result
        ])
        self.connection.commit()
        return result.getvalue()

if __name__ == "__main__":
    loader = DataLoader()
    try:
        msg = loader.call_add_product('Nowy produkt', 1, 1, 10.99, 100, 1)
        print(f"Wynik dodania produktu: {msg}")

        msg = loader.call_delete_product(123)
        print(f"Wynik usuwania produktu: {msg}")

        loader.process_all_files()
    finally:
        loader.close() 