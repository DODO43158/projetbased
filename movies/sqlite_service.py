import sqlite3
from django.conf import settings

def get_sqlite_conn():
    return sqlite3.connect(settings.DATABASES['default']['NAME'])

def get_sqlite_tables_count():
    conn = get_sqlite_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table'")
    count = cursor.fetchone()[0]
    conn.close()
    return count