import swisseph as swe

def calculate_navamsa(natal_data):
    """
    D1 haritasındaki boylamları alıp D9 (Navamsa) pozisyonlarını hesaplar.
    Her burç 30 derecedir, her Navamsa bölgesi (pada) 3° 20' (3.333...) derecedir.
    """
    navamsa_results = {"planets": [], "ascendant": None}
    
    # 1. Gezegenlerin Navamsa Pozisyonları
    for planet in natal_data['planets']:
        lon = planet['abs_longitude']
        
        # Toplam boylamı 3.3333...'e bölerek kaçıncı pada olduğunu buluyoruz
        pada_index = int(lon / (30 / 9)) 
        # Navamsa burç indeksi (0-11 arası)
        nav_sign_idx = pada_index % 12
        # Burç içi derece
        nav_degree = (lon % (30 / 9)) * 9 
        
        navamsa_results['planets'].append({
            "name": planet['name'],
            "sign_idx": nav_sign_idx,
            "degree": round(nav_degree, 4),
            "house": (nav_sign_idx - natal_data['ascendant']['sign_idx']) % 12 + 1
        })

    # 2. Navamsa Lagna Hesaplama
    lagna_lon = natal_data['ascendant']['abs_longitude'] if 'abs_longitude' in natal_data['ascendant'] else (natal_data['ascendant']['sign_idx'] * 30 + natal_data['ascendant']['degree'])
    
    lagna_pada_idx = int(lagna_lon / (30 / 9))
    nav_lagna_sign_idx = lagna_pada_idx % 12
    nav_lagna_degree = (lagna_lon % (30 / 9)) * 9

    navamsa_results['ascendant'] = {
        "name": "Navamsa Lagna (D9)",
        "sign_idx": nav_lagna_sign_idx,
        "degree": round(nav_lagna_degree, 4)
    }

    return navamsa_results