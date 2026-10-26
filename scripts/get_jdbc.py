"""Download the PostgreSQL JDBC driver into ./jars (idempotent)."""
import urllib.request
from pathlib import Path
import  settings

NAME = settings.JDBC_JAR_NAME
URL = settings.JDBC_JAR_URL
dest = Path(__file__).resolve().parents[1] / "jars" / NAME
dest.parent.mkdir(exist_ok=True)
if dest.exists():
    print("Already downloaded:", dest)
else:
    urllib.request.urlretrieve(URL, dest)
    print("Downloaded:", dest)





