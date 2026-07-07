import os
import shutil
import pandas as pd
import numpy as np
from pathlib import Path, PurePosixPath
from zipfile import ZipFile
from bson import decode_file_iter
import json # חובה להוסיף בשביל לפענח נתונים מורכבים כמו שינה

# =========================================================================
# 1. הגדרות ונתיבים (Configuration)
# =========================================================================
# משתנה בסיס המאפשר מעבר קל לדיסק חיצוני בעתיד (פשוט משנים את ה-Path)
DATA_BASE_PATH = Path(".") 

LIFESNAPS_ZIP = DATA_BASE_PATH / "rais_anonymized.zip" 
OUTPUT_DIR = DATA_BASE_PATH / "processed_data_parquet"

# הקמת תיקיית היעד במידה ולא קיימת
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =========================================================================
# 2. פונקציות עזר לקריאת BSON מתוך ה-ZIP (מתוך מחברת הקולאב)
# =========================================================================
def find_collection_file(archive: ZipFile, collection: str) -> str:
    """ מוצאת את מיקום קובץ ה-BSON בתוך ארכיון ה-ZIP """
    expected_name = f"{collection}.bson"
    matches = [
        name for name in archive.namelist()
        if PurePosixPath(name).name == expected_name
        and "mongo_rais_anonymized" in PurePosixPath(name).parts
    ]
    if len(matches) != 1:
        raise FileNotFoundError(f"צפוי קובץ אחד בשם {expected_name}; נמצאו: {matches}")
    return matches[0]

def iter_lifesnaps_documents(collection: str, zip_path: Path = LIFESNAPS_ZIP):
    """ טוענת ומפענחת שורה אחת בלבד בכל רגע נתון לחיסכון בזיכרון RAM """
    with ZipFile(zip_path) as archive:
        member = find_collection_file(archive, collection)
        with archive.open(member, "r") as bson_file:
            yield from decode_file_iter(bson_file)

# =========================================================================
# 3. פונקציות העיבוד והחיתוך (The Data Pipeline)
# =========================================================================
def flush_buffer_to_parquet(buffer: list):
    """ פונקציית עזר שלוקחת את הצבר הזמני בזיכרון ושופכת אותו לקבצים בדיסק """
    if not buffer:
        return
        
    df_temp = pd.DataFrame(buffer)
    
    # מקבצים לפי משתמש וסוג מדד, ושומרים כל שילוב לקובץ Parquet משלו
    for (user_id, metric_type), group in df_temp.groupby(['user_id', 'metric_type']):
        # יצירת נתיב תיקייה: processed_data_parquet/user_XXXX
        user_dir = OUTPUT_DIR / f"user_{user_id}"
        user_dir.mkdir(exist_ok=True)
        
        # שם הקובץ יהיה פשוט שם המדד (למשל heart_rate.parquet)
        clean_metric_name = metric_type.replace(' ', '_').lower()
        file_path = user_dir / f"{clean_metric_name}.parquet"
        
        # אם הקובץ כבר קיים מסבבים קודמים, נפתח אותו, נשרשר את המידע החדש ונשמור מחדש
        if file_path.exists():
            existing_df = pd.read_parquet(file_path)
            combined_df = pd.concat([existing_df, group], ignore_index=True)
            combined_df.to_parquet(file_path, index=False)
        else:
            group.to_parquet(file_path, index=False)

import json
import numpy as np
import pandas as pd
# נוודא שכל היבואים למעלה קיימים...

def slice_and_save_fitbit_raw(max_documents: int = None):
    """
    שואבת את כל מדדי הפיטביט ללא סינון, ושומרת שורות פגומות לקובץ לוג לביקורת.
    """
    buffer = []
    BUFFER_SIZE = 250000  
    docs_scanned = 0
    total_saved = 0
    skipped_count = 0
    
    print(f"[*] מתחיל סריקה ופיצול של קובץ הפיטביט (שואב את הכל + כותב לוג)...")
    
    # פותחים קובץ טקסט לשמירת השורות שדולגו
    with open("skipped_records_log.jsonl", "w", encoding="utf-8") as log_file:
        
        for doc in iter_lifesnaps_documents("fitbit"):
            docs_scanned += 1
            
            # הדפסת התקדמות בלייב
            if docs_scanned % 500000 == 0:
                print(f"   נסרקו {docs_scanned:,} שורות... נשמרו בינתיים {total_saved:,} דגימות.")
                
            doc_type = doc.get("type")
            
            # --- התיקון שלך: שמירת שורות ללא type ללוג ---
            if not doc_type:
                skipped_count += 1
                # נשמור עד 50,000 שורות פגומות כדי לא לסתום את המחשב במקרה של הצפה
                if skipped_count <= 50000:
                    log_file.write(json.dumps(doc, default=str) + "\n")
                continue 
                
            user_id = str(doc.get('id', doc.get('user_id')))
            data = doc.get('data', {})
            
            # --- התיקון הקודם שלנו: הוספת startTime והצלת הנתונים הסטטיים ---
            ts_str = data.get('dateTime') or data.get('timestamp') or data.get('recorded_time') or data.get('startTime')
            if not ts_str:
                ts_str = "UNDEFINED"
                
            # חילוץ גנרי שמתאים לכל המדדים
            raw_value = data.get('value')
            clean_val = np.nan
            confidence_val = np.nan
            
            if doc_type == 'heart_rate' and isinstance(raw_value, dict):
                clean_val = raw_value.get('bpm')
                confidence_val = raw_value.get('confidence', 3)
            elif doc_type == 'Heart Rate Variability Details':
                clean_val = data.get('rmssd')
            else:
                if isinstance(raw_value, (dict, list)):
                    clean_val = json.dumps(raw_value)
                else:
                    clean_val = raw_value

            # פיצול חכם למספרים או טקסט
            if isinstance(clean_val, str):
                val_num = np.nan
                val_text = clean_val
            else:
                try:
                    val_num = float(clean_val) if clean_val is not None else np.nan
                    val_text = None
                except (ValueError, TypeError):
                    val_num = np.nan
                    val_text = str(clean_val)

            buffer.append({
                'user_id': user_id,
                'metric_type': doc_type,
                'timestamp': str(ts_str), 
                'value_numeric': val_num,
                'value_text': val_text,
                'confidence': float(confidence_val) if pd.notna(confidence_val) else np.nan
            })
            total_saved += 1
            
            if len(buffer) >= BUFFER_SIZE:
                flush_buffer_to_parquet(buffer)
                buffer = []
                
            if max_documents and docs_scanned >= max_documents:
                print(f"[!] הגענו למגבלת הטסט שהוגדרה ({max_documents:,} שורות). עוצר.")
                break
                
        if buffer:
            flush_buffer_to_parquet(buffer)
            
    print(f"\n[V] הפיצול הסתיים בהצלחה!")
    print(f"    סה\"כ נסרקו מהמקור: {docs_scanned:,} שורות.")
    print(f"    סה\"כ נשמרו ב-Parquet: {total_saved:,} שורות.")
    if skipped_count > 0:
        print(f"    [!] אזהרה: נמצאו {skipped_count:,} שורות ללא type. הן נשמרו לבדיקה בקובץ skipped_records_log.jsonl")

def extract_psychology_data():
    """
    מחלצת את כל דיווחי האפליקציה (SEMA - רגשות) ואת השאלונים החד-פעמיים,
    ושומרת אותם כטבלאות מרוכזות.
    """
    print("\n[*] מתחיל חילוץ דיווחי רגש (SEMA) ושאלונים...")
    
    # פונקציית עזר פנימית שמתקנת את כל סוגי ה-ID של MongoDB בבת אחת
    def fix_mongo_ids(df):
        for col in df.columns:
            # אם שם העמודה מכיל 'id' (כמו user_id, _id, survey_id) והיא מסוג אובייקט
            if 'id' in col.lower() and df[col].dtype == 'object':
                df[col] = df[col].astype(str)
        
        # המרה גורפת של כל עמודת אובייקט אחרת שאולי מכילה רשימות/מילונים פנימיים לטקסט
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: str(x) if x is not None else None)
                
        return df

    # חילוץ דיווחי רגשות - SEMA
    sema_docs = list(iter_lifesnaps_documents("sema"))
    if sema_docs:
        df_sema = pd.json_normalize(sema_docs)
        df_sema = fix_mongo_ids(df_sema)
        df_sema.to_parquet(OUTPUT_DIR / "all_sema_emotions.parquet", index=False)
        print(f"   [V] נשמרו {len(df_sema):,} דיווחי רגשות (EMA) לקובץ all_sema_emotions.parquet")
    
    # חילוץ שאלוני רקע ואישיות - Surveys
    surveys_docs = list(iter_lifesnaps_documents("surveys"))
    if surveys_docs:
        df_surveys = pd.json_normalize(surveys_docs)
        df_surveys = fix_mongo_ids(df_surveys)
        df_surveys.to_parquet(OUTPUT_DIR / "all_surveys.parquet", index=False)
        print(f"   [V] נשמרו {len(df_surveys):,} שאלוני רקע לקובץ all_surveys.parquet")
# =========================================================================
# 4. נקודת הריצה (Execution)
# =========================================================================
if __name__ == "__main__":
    # בדיקת מקום פנוי בכונן לפני שמתחילים
    total, used, free = shutil.disk_usage(".")
    free_gb = free / (1024 ** 3)
    print(f"--- בדיקת מערכת ---")
    print(f"מקום פנוי בכונן: {free_gb:.2f} GB")
    print(f"-------------------\n")
    
    if not LIFESNAPS_ZIP.exists():
        print(f"[X] שגיאה: הקובץ {LIFESNAPS_ZIP.name} לא נמצא בתיקייה!")
        print("    אנא הריצי קודם את setup_environment.py כדי להוריד אותו.")
    else:
        # לטסט ראשוני: נריץ רק על מיליון השורות הראשונות כדי לראות שהכל עובד חלק.
        # בשלב הבא, כשנרצה לפצל את כל ה-9GB, פשוט נמחק את ה-(max_documents=1000000) והוא ירוץ על הכל.
        #slice_and_save_fitbit_raw(max_documents=1000000)
                
        slice_and_save_fitbit_raw()
        # חילוץ הפסיכולוגיה והרגש (ירוץ על הכל כי זה קובץ קטן)
        extract_psychology_data()