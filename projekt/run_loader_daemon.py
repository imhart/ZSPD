import time
import schedule
import logging
from data_loader import DataLoader
import sys
import os
from datetime import datetime

# Konfiguracja logowania
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daemon.log'),
        logging.StreamHandler()
    ]
)

def run_loader():
    """Funkcja uruchamiająca ładowanie danych"""
    try:
        logging.info("Rozpoczynam ładowanie danych...")
        loader = DataLoader()
        loader.process_all_files()
        loader.close()
        logging.info("Ładowanie danych zakończone pomyślnie")
    except Exception as e:
        logging.error(f"Wystąpił błąd podczas ładowania danych: {e}")

def main():
    """Główna funkcja daemona"""
    # Ustawienie harmonogramu
    schedule.every(1).hours.do(run_loader)  # Uruchamianie co godzinę
    
    logging.info("Daemon uruchomiony")
    
    # Pierwsze uruchomienie
    run_loader()
    
    # Pętla główna
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Sprawdzanie co minutę
        except KeyboardInterrupt:
            logging.info("Daemon zatrzymany przez użytkownika")
            sys.exit(0)
        except Exception as e:
            logging.error(f"Wystąpił nieoczekiwany błąd: {e}")
            time.sleep(300)  # Czekaj 5 minut przed ponowną próbą

if __name__ == "__main__":
    main() 