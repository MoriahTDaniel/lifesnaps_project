import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path("processed_data_parquet")

def summarize_all_data():
    print("==================================================")
    print("        דו\"ח סיכום מלא של הנתונים המעובדים        ")
    print("==================================================\n")
    
    if not OUTPUT_DIR.exists():
        print("[X] התיקייה המעובדת לא נמצאת. האם הרצת את תהליך העיבוד?")
        return

    # 1. סיכום נתונים פסיכולוגיים
    print("--- 1. נתונים פסיכולוגיים ושאלוני EMA ---")
    psych_files = ["all_sema_emotions.parquet", "all_surveys.parquet"]
    for file_name in psych_files:
        file_path = OUTPUT_DIR / file_name
        if file_path.exists():
            df = pd.read_parquet(file_path)
            # חיפוש עמודת מזהה המשתמש
            id_col = 'new_id' if 'new_id' in df.columns else ('user_id' if 'user_id' in df.columns else None)
            unique_users = df[id_col].nunique() if id_col else "לא ידוע"
            
            print(f"[V] קובץ: {file_name}")
            print(f"    - סה\"כ שורות (תצפיות): {len(df):,}")
            print(f"    - סה\"כ משתתפים שונים: {unique_users}")
        else:
            print(f"[X] קובץ {file_name} חסר!")
            
    # 2. סיכום נתונים פיזיולוגיים
    print("\n--- 2. נתונים פיזיולוגיים (Fitbit) ---")
    # חיפוש כל התיקיות שמתחילות ב-"user_"
    user_dirs = [d for d in OUTPUT_DIR.iterdir() if d.is_dir() and d.name.startswith("user_")]
    print(f"סה\"כ משתתפים עם נתונים פיזיולוגיים: {len(user_dirs)}")
    
    if user_dirs:
        metrics_summary = {}
        total_physio_rows = 0
        
        print("\nסורק את תיקיות המשתמשים (זה עשוי לקחת כמה שניות)...")
        for user_dir in user_dirs:
            for parquet_file in user_dir.glob("*.parquet"):
                metric_name = parquet_file.stem
                df = pd.read_parquet(parquet_file)
                rows = len(df)
                total_physio_rows += rows
                
                if metric_name not in metrics_summary:
                    metrics_summary[metric_name] = 0
                metrics_summary[metric_name] += rows
                
        print(f"סה\"כ דגימות פיזיולוגיות שנשמרו מכל המשתתפים: {total_physio_rows:,}\n")
        print("פירוט לפי סוג מדד:")
        for metric, count in metrics_summary.items():
            print(f"    - {metric.upper()}: {count:,} דגימות")
            
    print("\n==================================================")
    print("הבדיקה הסתיימה. זהו כל המידע שזמין כרגע במחשב שלך.")

if __name__ == "__main__":
    summarize_all_data()