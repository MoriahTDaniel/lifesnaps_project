import numpy as np
from pathlib import Path
from zipfile import ZipFile
from bson import decode_file_iter

LIFESNAPS_ZIP = Path("rais_anonymized.zip")

def find_collection_file(archive: ZipFile, collection: str) -> str:
    from pathlib import PurePosixPath
    expected_name = f"{collection}.bson"
    return [n for n in archive.namelist() if PurePosixPath(n).name == expected_name][0]

def inspect_missing_rows():
    print("[*] מחפש את השורות ללא חותמת הזמן (דוגמה של 5 ראשונות)... \n")
    count = 0
    
    with ZipFile(LIFESNAPS_ZIP) as archive:
        member = find_collection_file(archive, "fitbit")
        with archive.open(member, "r") as bson_file:
            for doc in decode_file_iter(bson_file):
                data = doc.get('data', {})
                ts_str = data.get('dateTime') or data.get('timestamp') or data.get('recorded_time')
                
                # מצאנו שורה שנזרקה!
                if not ts_str:
                    count += 1
                    if count <= 5: # נדפיס רק את ה-5 הראשונות כדי לא להציף
                        print(f"--- שורה חסרת זמן מס' {count} ---")
                        print(f"סוג מדד (type): {doc.get('type')}")
                        print(f"תוכן ה-data הגולמי: {data}")
                        print("-" * 40)
                        
    print(f"\n[V] סך הכל נמצאו {count:,} שורות ללא חותמת זמן שסוננו.")

if __name__ == "__main__":
    inspect_missing_rows()