import os
import shutil
import pandas as pd
import numpy as np
from pathlib import Path, PurePosixPath
from zipfile import ZipFile
from bson import decode_file_iter

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

def slice_and_save_fitbit_raw(max_documents: int = None):
    """
    הפונקציה המרכזית: רצה על קובץ הפיטביט, מחלצת מדדים, ומפצלת אותם.
    """
    # המדדים הפיזיולוגיים החשובים ביותר לחקר תנודות במצב הרוח
    TARGET_METRICS = {'heart_rate', 'steps', 'Heart Rate Variability Details'}
    
    buffer = []
    BUFFER_SIZE = 250000  # כתיבה לדיסק בכל צבירה של 250 אלף שורות (אופטימלי ל-RAM)
    docs_scanned = 0
    total_saved = 0
    
    print(f"[*] מתחיל סריקה ופיצול של קובץ הפיטביט...")
    
    for doc in iter_lifesnaps_documents("fitbit"):
        docs_scanned += 1
        
        # הדפסת התקדמות בלייב בטרמינל
        if docs_scanned % 500000 == 0:
            print(f"   נסרקו {docs_scanned:,} שורות מהקובץ הגולמי... נשמרו בינתיים {total_saved:,} דגימות.")
            
        doc_type = doc.get("type")
        if doc_type not in TARGET_METRICS:
            continue
            
        user_id = str(doc.get('id', doc.get('user_id')))
        data = doc.get('data', {})
        
        # שליפת חותמת הזמן בהתאם לפורמט המשתנה של המדדים
        ts_str = data.get('dateTime') or data.get('timestamp') or data.get('recorded_time')
        if not ts_str:
            continue
            
        # חילוץ חכם של הערך המספרי הנקי ומדד הביטחון (Confidence)
        clean_val = np.nan
        confidence_val = 3 # ברירת מחדל לצעדים ו-HRV שאין להם קונפידנס מובנה
        
        if doc_type == 'heart_rate':
            val_dict = data.get('value', {})
            if isinstance(val_dict, dict):
                clean_val = val_dict.get('bpm')
                confidence_val = val_dict.get('confidence', 3)
            else:
                clean_val = data.get('value')
        elif doc_type == 'steps':
            clean_val = data.get('value')
        elif doc_type == 'Heart Rate Variability Details':
            clean_val = data.get('rmssd') # לוקחים את מדד ה-RMSSD הקלאסי של HRV

        # הוספה לבאפר הזמני
        buffer.append({
            'user_id': user_id,
            'metric_type': doc_type,
            'timestamp': ts_str, 
            'value': float(clean_val) if clean_val is not None else np.nan,
            'confidence': int(confidence_val)
        })
        total_saved += 1
        
        # אם הבאפר מלא, נשפוך אותו לדיסק וננקה את הזיכרון
        if len(buffer) >= BUFFER_SIZE:
            flush_buffer_to_parquet(buffer)
            buffer = []
            
        # מנגנון הגנה לטסטים - עוצר אם הגדרנו מגבלה
        if max_documents and docs_scanned >= max_documents:
            print(f"[!] הגענו למגבלת הטסט שהוגדרה ({max_documents:,} שורות). עוצר.")
            break
            
    # פריקה אחרונה של השאריות שנשארו בבאפר בסיום הלולאה
    if buffer:
        flush_buffer_to_parquet(buffer)
        
    print(f"\n[V] הפיצול הסתיים בהצלחה!")
    print(f"    סה\"כ נסרקו מהמקור: {docs_scanned:,} שורות.")
    print(f"    סה\"כ נשמרו ב-Parquet: {total_saved:,} שורות מדדים ממוקדים.")


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
        slice_and_save_fitbit_raw(max_documents=1000000)
                
        # חילוץ הפסיכולוגיה והרגש (ירוץ על הכל כי זה קובץ קטן)
        extract_psychology_data()