import pandas as pd
from pathlib import Path

# מוצא את תיקיית השורש של הפרויקט
BASE_DIR = Path(__file__).resolve().parent.parent 
DATA_DIR = BASE_DIR / "data" / "processed_parquet"

def create_emotion_physio_matches():
    # 0. וידוא שהתיקייה קיימת
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. נטען את כל הדיווחים הפסיכולוגיים
    sema_path = DATA_DIR / "all_sema_emotions.parquet"
    df_sema = pd.read_parquet(sema_path)
    df_sema['timestamp'] = pd.to_datetime(df_sema['data.COMPLETED_TS'])
    
    # 2. נכין רשימה לתוצאות
    all_matches = []
    
    # 3. נסרוק כל נבדק בנפרד
    for user_folder in (DATA_DIR / "user_data").glob("user_*"):
        user_id = user_folder.name.replace("user_", "")
        hr_file = user_folder / "heart_rate.parquet"
        
        # סינון הרגשות של הנבדק הספציפי
        user_sema = df_sema[df_sema['participant_id'] == user_id].sort_values('timestamp')
        
        # בדיקת קיום נתונים
        if user_sema.empty:
            print(f"נבדק {user_id}: לא נמצאו דיווחי רגש.")
            continue
            
        if not hr_file.exists():
            print(f"נבדק {user_id}: לא נמצא קובץ דופק.")
            continue
            
        # טעינת ועיבוד דופק
        df_hr = pd.read_parquet(hr_file)
        df_hr['timestamp'] = pd.to_datetime(df_hr['timestamp'])
        df_hr = df_hr.sort_values('timestamp')
        
        # הדפסות לאבחון (רק אם את רוצה לוודא טווחים)
        print(f"נבדק {user_id} | SEMA: {user_sema['timestamp'].min()} עד {user_sema['timestamp'].max()} | HR: {df_hr['timestamp'].min()} עד {df_hr['timestamp'].max()}")
        user_sema['timestamp'] = user_sema['timestamp'].dt.tz_localize(None)
        df_hr['timestamp'] = df_hr['timestamp'].dt.tz_localize(None)
        # התאמה ב-30 דקות לפני
        matches = pd.merge_asof(
            user_sema, df_hr, 
            on='timestamp', 
            direction='backward', 
            tolerance=pd.Timedelta('30m')
        )
        all_matches.append(matches)
        
    return pd.concat(all_matches) if all_matches else pd.DataFrame()

# הרצה ושמירה
if __name__ == "__main__":
    final_df = create_emotion_physio_matches()
    if not final_df.empty:
        final_df.to_parquet(DATA_DIR / "matched_emotion_physio.parquet")
        print(f"הסתיימה בניית טבלת ההתאמות! נמצאו {len(final_df)} שורות.")
    else:
        print("לא נמצאו התאמות. בדקי את הטווחים (Dates) שהודפסו בטרמינל כדי לראות אם יש חפיפה.")