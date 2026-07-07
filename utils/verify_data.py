import pandas as pd
from pathlib import Path

# נגדיר את נתיב התיקייה של הנבדק שמצאת (תשני את השם אם ה-ID המלא שונה)

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "processed_parquet"
USER_DIR = OUTPUT_DIR / "user_621e2e8e67b776a24055b564"  # החליפי את זה ל-ID של הנבדק שלך
# (ב-verify_data.py, תוסיפי ל-OUTPUT_DIR את ה- / "user_621e2e8e67b776a24055b564")
def verify_user_data():
    print(f"=== בודק נתונים עבור נבדק: {USER_DIR.name} ===\n")
    
    if not USER_DIR.exists():
        print("[X] התיקייה לא קיימת. ודאי שהשם נכון.")
        return

    # מעבר על כל קובץ Parquet בתיקייה
    for file_path in USER_DIR.glob("*.parquet"):
        df = pd.read_parquet(file_path)
        
    
        # המרת עמודת הזמן לתאריך כדי למצוא התחלה וסוף
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        valid_times = df['timestamp'].dropna() # מסננים זמנים ריקים (NaT)
        
        if not valid_times.empty:
            start_time = valid_times.min().strftime('%Y-%m-%d %H:%M')
            end_time = valid_times.max().strftime('%Y-%m-%d %H:%M')
        else:
            start_time = "נתונים סטטיים (ללא זמן)"
            end_time = "נתונים סטטיים (ללא זמן)"
        print(f"מדד: {file_path.stem.upper()}")
        print(f"  - כמות דגימות: {len(df):,}")
        print(f"  - תאריך התחלה: {start_time}")
        print(f"  - תאריך סיום:  {end_time}")
        
        # הצגת 2 השורות הראשונות כדוגמה
        print("  - דוגמה לנתונים:")
        display_df = df.head(2).copy()
        display_df['timestamp'] = display_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        print(display_df.to_string(index=False))
        print("-" * 40)

if __name__ == "__main__":
    verify_user_data()