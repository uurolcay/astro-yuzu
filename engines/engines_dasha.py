from datetime import datetime, timedelta
import pytz
from core.calculation_context import CalculationContext

def calculate_vims_dasha(context_or_birth_dt, moon_lon):
    birth_dt = context_or_birth_dt.datetime_utc if isinstance(context_or_birth_dt, CalculationContext) else context_or_birth_dt
    # birth_dt'nin timezone bilgisini UTC olarak sabitleyelim (Hata Çözümü)
    if birth_dt.tzinfo is None:
        birth_dt = birth_dt.replace(tzinfo=pytz.UTC)

    # Vimshottari Dasha Periyotları (Yıl bazında)
    periods = [
        ("Ketu", 7), ("Venus", 20), ("Sun", 6), ("Moon", 10),
        ("Mars", 7), ("Rahu", 18), ("Jupiter", 16), ("Saturn", 19), ("Mercury", 17)
    ]
    
    nakshatra_range = 360 / 27 # 13.3333 derece
    nak_idx = int(moon_lon / nakshatra_range)
    start_lord_idx = nak_idx % 9
    
    elapsed_in_nak = moon_lon % nakshatra_range
    remaining_ratio = 1 - (elapsed_in_nak / nakshatra_range)
    
    dasha_timeline = []
    current_date = birth_dt
    
    for i in range(9):
        idx = (start_lord_idx + i) % 9
        name, years = periods[idx]
        
        duration_days = years * 365.25
        if i == 0:
            duration_days *= remaining_ratio
            
        end_date = current_date + timedelta(days=duration_days)
        
        dasha_timeline.append({
            "planet": name,
            "start": current_date.strftime("%Y-%m-%d"),
            "end": end_date.strftime("%Y-%m-%d")
        })
        current_date = end_date
        
    return dasha_timeline
