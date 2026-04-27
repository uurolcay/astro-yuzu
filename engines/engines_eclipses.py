import swisseph as swe
from datetime import datetime, timedelta
import pytz

from config.astro_config import ASTRO_CONFIG
from core.ayanamsa import configure_sidereal_mode
from core.calculation_context import CalculationContext

def _build_context(birth_dt, lat, lon):
    return CalculationContext(
        datetime_local=birth_dt,
        datetime_utc=birth_dt,
        latitude=lat,
        longitude=lon,
        timezone="UTC",
        ayanamsa=ASTRO_CONFIG["ayanamsa"],
        node_mode=ASTRO_CONFIG["node_mode"],
        house_system=ASTRO_CONFIG["house_system"],
    )


def calculate_upcoming_eclipses(context_or_birth_dt, lat=None, lon=None, natal_data=None):
    """
    Önümüzdeki 18 ay içindeki Güneş ve Ay tutulmalarını hesaplar 
    ve natal gezegenlerle olan açısal etkisini (orb) analiz eder.
    """
    # Lahiri Ayanamsa Ayarı
    context = context_or_birth_dt if isinstance(context_or_birth_dt, CalculationContext) else _build_context(context_or_birth_dt, lat, lon)
    configure_sidereal_mode(context)
    
    # Başlangıç zamanı (Bugün UTC)
    start_dt = datetime.now(pytz.UTC)
    end_dt = start_dt + timedelta(days=540)
    
    # İsviçre Efemeris zaman formatına çevrim (Julian Day)
    tjd_start = swe.julday(start_dt.year, start_dt.month, start_dt.day, start_dt.hour)
    
    eclipses = []
    
    # 1. GÜNEŞ TUTULMALARI TARAMASI
    t_search = tjd_start
    while t_search < swe.julday(end_dt.year, end_dt.month, end_dt.day):
        # Güneş tutulması ara (iflag: her türlü tutulma)
        res = swe.sol_eclipse_when_glob(t_search, swe.FLG_SIDEREAL)
        t_eclipse = res[1][0] # Tutulma maksimum zamanı (JD)
        
        if t_eclipse > swe.julday(end_dt.year, end_dt.month, end_dt.day):
            break
            
        # Tutulma detaylarını al (Burç ve Derece)
        # res[0] tutulma tipini verir
        e_type = "Solar (Güneş)"
        flags = res[0]
        type_str = "Tam" if flags & swe.ECL_TOTAL else "Halkalı" if flags & swe.ECL_ANNULAR else "Parçalı"
        
        eclipse_info = _get_eclipse_details(t_eclipse, e_type, type_str, natal_data, context)
        eclipses.append(eclipse_info)
        
        t_search = t_eclipse + 30 # Bir sonraki ay için kaydır
        
    # 2. AY TUTULMALARI TARAMASI
    t_search = tjd_start
    while t_search < swe.julday(end_dt.year, end_dt.month, end_dt.day):
        res = swe.lun_eclipse_when(t_search, swe.FLG_SIDEREAL)
        t_eclipse = res[1][0]
        
        if t_eclipse > swe.julday(end_dt.year, end_dt.month, end_dt.day):
            break
            
        e_type = "Lunar (Ay)"
        flags = res[0]
        type_str = "Tam" if flags & swe.ECL_TOTAL else "Parçalı" if flags & swe.ECL_PARTIAL else "Penumbral (Gölgeli)"
        
        eclipse_info = _get_eclipse_details(t_eclipse, e_type, type_str, natal_data, context)
        eclipses.append(eclipse_info)
        
        t_search = t_eclipse + 30

    # Tarihe göre sırala
    eclipses.sort(key=lambda x: x['date'])
    return eclipses

def _get_eclipse_details(jd, e_type, type_str, natal_data, context):
    """Tutulmanın burç/derece bilgisini ve natal etkisini hesaplayan yardımcı fonksiyon."""
    # Tutulma anındaki Güneş (Solar) veya Ay (Lunar) pozisyonu
    obj = swe.SUN if "Solar" in e_type else swe.MOON
    configure_sidereal_mode(context)
    res = swe.calc_ut(jd, obj, swe.FLG_SIDEREAL)[0]
    
    abs_lon = res[0]
    sign_idx = int(abs_lon / 30)
    degree = abs_lon % 30
    
    # Julian Day'den okunabilir tarihe
    y, m, d, h = swe.revjul(jd)[:4]
    date_str = f"{int(y)}-{int(m):02d}-{int(d):02d}"
    
    # Natal Etki Analizi (Orb kontrolü)
    impacts = []
    # Natal gezegenleri ve Lagna'yı tara
    check_points = natal_data['planets'] + [{"name": "Lagna", "abs_longitude": natal_data['ascendant'].get('abs_longitude', (natal_data['ascendant']['sign_idx']*30 + natal_data['ascendant']['degree']))}]
    
    for point in check_points:
        p_lon = point.get('abs_longitude', (point.get('sign_idx', 0)*30 + point.get('degree', 0)))
        diff = abs(abs_lon - p_lon)
        if diff > 180: diff = 360 - diff # Karşıt açı/kavuşum mesafe düzeltmesi
        
        strength = None
        if diff <= 5: strength = "Güçlü (Kritik)"
        elif diff <= 10: strength = "Orta (Belirgin)"
        elif diff <= 15: strength = "Zayıf (Hafif)"
        
        if strength:
            impacts.append({
                "point": point['name'],
                "orb": round(diff, 2),
                "strength": strength
            })

    return {
        "date": date_str,
        "type": e_type,
        "subtype": type_str,
        "sign_idx": sign_idx,
        "degree": round(degree, 4),
        "natal_impacts": impacts
    }
