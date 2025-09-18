"""
Configuracion para Scraper Automation
"""

import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Google Sheets Configuration
GOOGLE_SHEET_ID_DATA = os.getenv('GOOGLE_SHEET_ID_DATA', '')

# Para testing local - RUTA CORREGIDA
LOCAL_CREDENTIALS_FILE = "../credentials/service-account.json"

# Para testing rapido - Solo 2 cuentas
CUIMO_ACCOUNTS_TEST = {
    "CUIMO.1": "https://es.wallapop.com/user/cuimo-418807885",
    'CUIMO.2': 'https://es.wallapop.com/user/cuimom-469497220'
}

# CUENTAS CUIMO COMPLETAS
CUIMO_ACCOUNTS_FULL = {
    'CUIMO.1': 'https://es.wallapop.com/user/cuimo-418807885',
    'CUIMO.2': 'https://es.wallapop.com/user/cuimom-469497220',
    'CUIMO.3': 'https://es.wallapop.com/user/cuimom-457423496',
    'MOTOSPLUS': 'https://es.wallapop.com/user/motosp-436538733'
}

def get_moto_accounts(test_mode=False):
    """
    Devuelve lista de cuentas segun el modo
    
    Args:
        test_mode: Si esta en modo test
    """
    test_mode = test_mode or os.getenv('TEST_MODE', 'false').lower() == 'true'
    
    if test_mode:
        print("MODO TEST: Solo procesando 2 cuentas ")
        return CUIMO_ACCOUNTS_TEST
    else:
        print(f"MODO COMPLETO: Procesando {len(CUIMO_ACCOUNTS_FULL)} cuentas ")
        return CUIMO_ACCOUNTS_FULL