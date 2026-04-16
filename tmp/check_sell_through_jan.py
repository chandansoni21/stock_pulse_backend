import os
import sys
from dotenv import load_dotenv

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(ROOT_DIR)
load_dotenv(os.path.join(ROOT_DIR, '.env'))

from api.db_powerbi import execute_dax

dax = """
EVALUATE
SUMMARIZECOLUMNS(
    'Date_Table'[Month-Year],
    KEEPFILTERS(FILTER(ALL('Date_Table'[Month-Year]), 'Date_Table'[Month-Year] = "Jan-2026")),
    "st_raw", [Sell_Through],
    "st_pct", [Sell Through %]
)
"""
try:
    results = execute_dax(dax)
    if not results:
        print("NO_RESULTS")
    else:
        for r in results:
            for k, v in r.items():
                print(f"{k} ===> {v}")
except Exception as e:
    print(f"ERROR: {e}")
