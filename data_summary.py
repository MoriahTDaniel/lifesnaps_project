import pandas as pd
from pathlib import Path
import hashlib

OUTPUT_DIR = Path("processed_data_parquet")

def calculate_file_hash(file_path):
    """מחשב טביעת אצבע דיגיטלית ייחודית לקובץ כדי לוודא זהות בין מחשבים"""
    if not file_path.exists():
        return "קובץ חסר"
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        buf = f.read(1024 * 1024) # קריאה בבלוקים של 1MB ליציבות
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(1024 * 1024)
    return hasher.hexdigest()[:8] # לוקחים רק את 8 התווים הראשונים לנוחות

def summarize_all_data():
    print("==================================================")
    print("        דו\"ח אימות וסיכום נתונים לצוות           ")
    print("==================================================\n")
    
    if not OUTPUT_DIR.exists():
        print("[X] תיקיית הנתונים המעובדים לא קיימת!")
        return

    print("--- 1. קבצים פסיכולוגיים מרכזיים ---")
    psych_files = ["all_sema_emotions.parquet", "all_surveys.parquet"]
    for file_name in psych_files:
        file_path = OUTPUT_DIR / file_name
        if file_path.exists():
            df = pd.read_parquet(file_path)
            file_hash = calculate_file_hash(file_path)
            print(f"[V] קובץ: {file_name}")
            print(f"    - כמות שורות: {len(df):,}")
            print(f"    - טביעת אצבע דיגיטלית (Hash): {file_hash}")
        else:
            print(f"[X] קובץ {file_name} חסר!")
            
    print("\n--- 2. נתונים פיזיולוגיים (Fitbit) ---")
    user_dirs = [d for d in OUTPUT_DIR.iterdir() if d.is_dir() and d.name.startswith("user_")]
    print(f"סה\"כ תיקיות משתמשים (נבדקים): {len(user_dirs)}")
    
    if user_dirs:
        metrics_summary = {}
        total_physio_rows = 0
        
        for user_dir in user_dirs:
            for parquet_file in user_dir.glob("*.parquet"):
                metric_name = parquet_file.stem
                df = pd.read_parquet(parquet_file)
                rows = len(df)
                total_physio_rows += rows
                metrics_summary[metric_name] = metrics_summary.get(metric_name, 0) + rows
                
        print(f"סה\"כ שורות פיזיולוגיות שנשמרו: {total_physio_rows:,}")
        print("\nפירוט שורות לפי סוג מדד:")
        for metric, count in sorted(metrics_summary.items()):
            print(f"    - {metric.upper()}: {count:,} דגימות")
            
    print("\n==================================================")
    print("הריצו דו\"ח זה במחשבים שלכן. אם כמות השורות וטביעות האצבע (Hash)")
    print("זהות בין כל חברי הצוות - סימן שלכולם יש בדיוק אותו מידע!")

if __name__ == "__main__":
    summarize_all_data()