import swisseph as swe
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import uvicorn
from openai import OpenAI
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
import pytz
import json
import os

# --- DATABASE ARCHITECTURE (SQLAlchemy) ---
from sqlalchemy import Column, Integer, String, Text, Float, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# Yerel veritabanı dosyası: astro_logic.db
SQLALCHEMY_DATABASE_URL = "sqlite:///./astro_logic.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class UserRecord(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    birth_date = Column(String, index=True) 
    city = Column(String, index=True)
    lat = Column(Float)
    lon = Column(Float)
    natal_data_json = Column(Text) 

# Tabloları oluştur
Base.metadata.create_all(bind=engine)

# Veritabanı oturum yönetimi
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- INITIALIZATION ---
app = FastAPI(title="Professional Astrology AI Engine", version="1.4")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# !!! ÖNEMLİ: Kendi OpenAI API Anahtarınızı buraya yazın !!!
client = OpenAI(api_key="YOUR_OPENAI_API_KEY")
geolocator = Nominatim(user_agent="jyotish_ai_engine_v1_4_fixed")
tf = TimezoneFinder()

# --- MODULE 1: INPUT NORMALIZATION ---
def get_utc_and_coords(birth_date_str, city_name):
    location = geolocator.geocode(city_name)
    if not location:
        raise ValueError(f"City '{city_name}' not found.")
    
    lat, lon = location.latitude, location.longitude
    timezone_str = tf.timezone_at(lng=lon, lat=lat) or "UTC"
    
    local_tz = pytz.timezone(timezone_str)
    naive_dt = datetime.strptime(birth_date_str, "%Y-%m-%dT%H:%M")
    
    local_dt = local_tz.localize(naive_dt)
    utc_dt = local_dt.astimezone(pytz.UTC)
    
    return utc_dt, lat, lon

# --- MODULE 2 & 3: NATAL & TRANSIT COMPUTATION ---
def calculate_astrology_data(utc_dt, lat, lon, is_transit=False):
    decimal_hour = utc_dt.hour + utc_dt.minute / 60.0 + utc_dt.second / 3600.0
    jd = swe.julday(utc_dt.year, utc_dt.month, utc_dt.day, decimal_hour)
    
    swe.set_sid_mode(swe.SIDM_LAHIRI) # Vedik (Lahiri) Ayanamsa
    
    # Gezegen Listesi (Sun'dan True Rahu'ya)
    p_ids = [0, 1, 4, 2, 5, 3, 6, 11]
    p_names = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn", "Rahu"]
    
    planets_data = []
    for pid, name in zip(p_ids, p_names):
        res = swe.calc_ut(jd, pid, swe.FLG_SIDEREAL)[0]
        lon_abs = float(res)
        planets_data.append({
            "name": name,
            "abs_longitude": round(lon_abs, 4),
            "sign_idx": int(lon_abs / 30),
            "degree": round(lon_abs % 30, 4)
        })
    
    # Ketu (Rahu + 180 derece)
    rahu = next(p for p in planets_data if p['name'] == "Rahu")
    ketu_lon = (rahu['abs_longitude'] + 180) % 360
    planets_data.append({
        "name": "Ketu", 
        "abs_longitude": round(ketu_lon, 4),
        "sign_idx": int(ketu_lon / 30), 
        "degree": round(ketu_lon % 30, 4)
    })

    if is_transit: 
        return planets_data

    # Natal Ekstralar (Lagna & Evler)
    houses, ascmc = swe.houses_ex(jd, lat, lon, b'W') # Whole Sign System
    lagna_lon = ascmc[0]
    lagna_sign = int(lagna_lon / 30)

    for p in planets_data:
        p["house"] = ((p["sign_idx"] - lagna_sign + 12) % 12) + 1

    # Karaka Hesaplama (Atmakaraka & Amatyakaraka)
    main_7 = [p for p in planets_data if p['name'] not in ["Rahu", "Ketu"]]
    sorted_p = sorted(main_7, key=lambda x: x['degree'], reverse=True)
    
    return {
        "planets": planets_data,
        "ascendant": {
            "name": "Lagna", 
            "abs_longitude": round(lagna_lon, 4),
            "sign_idx": lagna_sign, 
            "degree": round(lagna_lon % 30, 4), 
            "house": 1
        },
        "karakas": {
            "atmakaraka": sorted_p[0]['name'], 
            "amatyakaraka": sorted_p[1]['name']
        }
    }

# --- MODULE 4: EVENT SCORING ---
def score_events(natal_data, transit_planets):
    scores = []
    lagna_sign = natal_data['ascendant']['sign_idx']
    
    for tp in transit_planets:
        house_num = ((tp['sign_idx'] - lagna_sign + 12) % 12) + 1
        base_score = 30
        if house_num in [1, 10]: base_score = 60
        
        # Kavuşum Kontrolü
        for np in natal_data['planets']:
            if tp['sign_idx'] == np['sign_idx']:
                orb = abs(tp['degree'] - np['degree'])
                if orb < 6.0:
                    impact = base_score + 35
                    if tp['name'] in ["Saturn", "Jupiter"]: impact += 15
                    scores.append({
                        "event": f"Transit {tp['name']} on Natal {np['name']}",
                        "house": house_num, 
                        "score": min(impact, 100)
                    })
        
        # Sadece Ev Geçişi (Eğer kavuşum yoksa)
        if not any(s['event'].startswith(f"Transit {tp['name']}") for s in scores):
            scores.append({
                "event": f"Transit {tp['name']} in House {house_num}", 
                "house": house_num, 
                "score": base_score
            })

    return sorted(scores, key=lambda x: x['score'], reverse=True)

# --- MODULE 6: AI INTERPRETATION ---
def get_ai_insight(scored_events, karakas):
    try:
        top_events = scored_events[:5]
        prompt = (
            f"User Soul Path (Atmakaraka): {karakas['atmakaraka']}. "
            f"Top 5 Astrological Events: {top_events}. "
            "Based on this data, provide a high-level strategic interpretation for today. "
            "Focus on career and inner growth. Be concise."
        )
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a professional Jyotish strategist. Strategic advice only, no fluff."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3
        )
        return response.choices[0].message.content
    except: 
        return "Insight engine temporarily unavailable."

# --- API ENDPOINTS ---
@app.get("/api/v1/natal")
async def main_engine(date: str, city: str, name: str = "User", db: Session = Depends(get_db)):
    try:
        # 1. VERİTABANI KONTROLÜ (Cache)
        record = db.query(UserRecord).filter(UserRecord.birth_date == date, UserRecord.city == city).first()
        
        if record:
            natal_data = json.loads(record.natal_data_json)
            lat, lon = record.lat, record.lon
            source = "database_cache"
        else:
            # 2. YENİ HESAPLAMA
            utc_dt, lat, lon = get_utc_and_coords(date, city)
            natal_data = calculate_astrology_data(utc_dt, lat, lon)
            
            # 3. VERİTABANINA KAYDET
            new_user = UserRecord(
                name=name, 
                birth_date=date, 
                city=city, 
                lat=lat, 
                lon=lon, 
                natal_data_json=json.dumps(natal_data)
            )
            db.add(new_user)
            db.commit()
            source = "fresh_calculation"

        # 4. TRANSİT HESAPLAMA (Dinamik - Her seferinde güncel)
        now_utc = datetime.now(pytz.UTC)
        transit_planets = calculate_astrology_data(now_utc, lat, lon, is_transit=True)
        scored_events = score_events(natal_data, transit_planets)
        ai_insight = get_ai_insight(scored_events, natal_data['karakas'])
        
        return {
            "status": "success", 
            "source": source,
            "data": {
                "natal": natal_data,
                "transit_highlights": scored_events[:5],
                "ai_insight": ai_insight
            }
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)