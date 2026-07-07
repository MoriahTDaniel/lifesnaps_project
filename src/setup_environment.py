import os
import urllib.request
import sys
from pathlib import Path

# הנתיב והקישור להורדה
ZENODO_URL = "https://zenodo.org/records/7229547/files/rais_anonymized.zip?download=1"
FILE_NAME = "rais_anonymized.zip"

def show_progress(block_num, block_size, total_size):
    """
    פונקציה המייצרת מד התקדמות חי בטרמינל כדי לא להשאיר מסך שחור
    """
    downloaded = block_num * block_size
    if total_size > 0:
        percent = downloaded * 100 / total_size
        # מעדכן את אותה שורת טקסט כדי לא להציף את המסך
        sys.stdout.write(f"\rהורדה מתקדמת: {percent:.1f}% ({downloaded / (1024**3):.2f} GB / {total_size / (1024**3):.2f} GB)")
        sys.stdout.flush()

def setup_project():
    print("=== מתחיל התקנת סביבת פרויקט LifeSnaps ===")
    
    # 1. יצירת תיקיית העיבוד
    Path("processed_data_parquet").mkdir(parents=True, exist_ok=True)
    print("[V] תיקיית היעד (processed_data_parquet) מוכנה.")

    # 2. הורדת הנתונים הגולמיים
    if Path(FILE_NAME).exists():
        print(f"[V] הקובץ {FILE_NAME} כבר קיים במחשב. מדלג על ההורדה.")
    else:
        print(f"[*] מתחיל הורדת קובץ הנתונים (9.2GB) מ-Zenodo...")
        print("זה ייקח זמן בהתאם למהירות האינטרנט שלך, אפשר ללכת לשתות קפה :)")
        try:
            # הפקודה שמורידה את הקובץ ומפעילה את מד ההתקדמות שלנו
            urllib.request.urlretrieve(ZENODO_URL, FILE_NAME, show_progress)
            print(f"\n[V] ההורדה הושלמה בהצלחה! הקובץ נשמר כ-{FILE_NAME}")
        except Exception as e:
            print(f"\n[X] שגיאה בהורדה: {e}")
            return
            
    print("=== ההתקנה הסתיימה! אפשר כעת להריץ את process_data.py ===")

if __name__ == "__main__":
    setup_project()