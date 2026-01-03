from flask import Flask, render_template, jsonify, request
from sqlalchemy import create_engine, text
import pandas as pd
import json
from datetime import datetime
import requests
import concurrent.futures
import urllib3
import os

# Matikan warning SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

# --- KONFIGURASI DATABASE ---
# Pastikan password/username sesuai dengan settingan PC Anda
DB_URI = os.environ.get('DATABASE_URL', 'postgresql://neondb_owner:npg_UMB35qZRoWzf@ep-steep-grass-a1183odj-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')
engine = create_engine(DB_URI, client_encoding='utf8')

@app.route('/')
def index():
    return render_template('index.html')

# ==============================================================================
# API 1: DATA CUACA BMKG
# ==============================================================================
@app.route('/api/bmkg-weather')
def get_bmkg_weather():
    # Daftar Kode Wilayah (ADM4) Pusat Kota di Kaltim
    locations = [
        {"kota": "Samarinda", "id": "64.72.04.1001"},   
        {"kota": "Balikpapan", "id": "64.71.01.1004"},  
        {"kota": "Bontang", "id": "64.74.02.1001"},     
        {"kota": "Tenggarong", "id": "64.02.06.1001"},  
        {"kota": "Sangatta", "id": "64.04.01.1001"},    
        {"kota": "Tanjung Redeb", "id": "64.03.04.1001"}, 
        {"kota": "Sendawar", "id": "64.07.07.1002"},    
        {"kota": "Penajam", "id": "64.09.01.1001"},     
        {"kota": "Tanah Grogot", "id": "64.01.04.1001"}, 
        {"kota": "Mahakam Ulu", "id": "64.11.03.2001"}
    ]

    weather_result = []

    def fetch_city_weather(loc):
        try:
            url = f"https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4={loc['id']}"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            response = requests.get(url, headers=headers, timeout=10, verify=False)
            
            if response.status_code == 200:
                api_data = response.json()
                all_forecasts = []
                
                try:
                    if 'data' in api_data and len(api_data['data']) > 0:
                        cuaca_per_hari = api_data['data'][0].get('cuaca', [])
                        for daily_data in cuaca_per_hari:
                            for hourly_data in daily_data:
                                all_forecasts.append(hourly_data)
                except Exception as e:
                    return None

                if not all_forecasts:
                    return None

                now = datetime.now()
                current_weather = None
                min_diff = float('inf')
                
                for item in all_forecasts:
                    if 'local_datetime' in item:
                        try:
                            time_str = item['local_datetime']
                            item_time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                            diff = abs((item_time - now).total_seconds())
                            if diff < min_diff:
                                min_diff = diff
                                current_weather = item
                        except ValueError:
                            continue
                
                if current_weather:
                    icon_url = current_weather.get('image', '')
                    if icon_url:
                        icon_url = icon_url.replace(" ", "%20")
                    return {
                        "kota": loc['kota'],
                        "suhu": str(current_weather.get('t', '-')),
                        "cuaca": current_weather.get('weather_desc', 'Berawan'),
                        "icon": icon_url
                    }
            return None
        except Exception as e:
            return None

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(fetch_city_weather, locations)
    
    for res in results:
        if res: weather_result.append(res)

    return jsonify(sorted(weather_result, key=lambda x: x['kota']))

# ==============================================================================
# API 2: DASHBOARD DATA (HOME)
# ==============================================================================
@app.route('/api/dashboard-data')
def get_dashboard_data():
    try:
        conn = engine.connect()
        # Query Data Harian Hari Ini
        query_daily = text("""
            SELECT nama_pos, kecamatan, kabupaten, lintang, bujur, 
                   curah_hujan, TO_CHAR(tanggal, 'DD Mon YYYY') as tanggal_str
            FROM curah_hujan_harian
            WHERE tanggal = CURRENT_DATE
        """)
        df_daily = pd.read_sql(query_daily, conn)
        df_daily['curah_hujan'] = df_daily['curah_hujan'].fillna(0).astype(float)
        
        # Top 10 (Harian)
        df_top10 = df_daily[df_daily['curah_hujan'] > 0].sort_values(by='curah_hujan', ascending=False).head(10)
        
        # Monthly Summary (Untuk Popup Home jika diperlukan)
        query_monthly = text("""
            SELECT nama_pos,
                SUM(curah_hujan) as total_bulanan
            FROM curah_hujan_harian
            WHERE EXTRACT(MONTH FROM tanggal) = EXTRACT(MONTH FROM CURRENT_DATE)
              AND EXTRACT(YEAR FROM tanggal) = EXTRACT(YEAR FROM CURRENT_DATE)
            GROUP BY nama_pos
        """)
        df_monthly = pd.read_sql(query_monthly, conn)
        df_monthly = df_monthly.fillna(0)

        conn.close()
        return jsonify({
            "daily": df_daily.to_dict(orient='records'),
            "top10": df_top10.to_dict(orient='records'),
            "monthly": df_monthly.to_dict(orient='records')
        })
    except Exception as e:
        return jsonify({'error': str(e)})

# ==============================================================================
# API 3: DATA INTERAKTIF (PERBAIKAN PENGELOMPOKAN DATA)
# ==============================================================================
@app.route('/api/interactive-data')
def get_interactive_data():
    date_param = request.args.get('date')
    if not date_param: return jsonify({'error': 'Tanggal diperlukan'})
    
    try:
        conn = engine.connect()
        selected_date = datetime.strptime(date_param, '%Y-%m-%d')
        year = selected_date.year
        month = selected_date.month
        day = selected_date.day

        # 1. Query Harian (Titik Peta)
        query_daily = text(f"SELECT nama_pos, lintang, bujur, curah_hujan FROM curah_hujan_harian WHERE tanggal = '{date_param}'")
        df_daily = pd.read_sql(query_daily, conn)
        df_daily['curah_hujan'] = df_daily['curah_hujan'].fillna(0).astype(float)

        # 2. Query Akumulasi Bulanan s.d Tanggal Terpilih (Bubble Peta)
        query_accum = text(f"""
            SELECT nama_pos, lintang, bujur, SUM(curah_hujan) as total_akumulasi
            FROM curah_hujan_harian
            WHERE EXTRACT(YEAR FROM tanggal) = {year} 
              AND EXTRACT(MONTH FROM tanggal) = {month} 
              AND EXTRACT(DAY FROM tanggal) <= {day}
            GROUP BY nama_pos, lintang, bujur
        """)
        df_accum = pd.read_sql(query_accum, conn)
        df_accum['total_akumulasi'] = df_accum['total_akumulasi'].fillna(0).astype(float)

        # 3. Query Tabel Rekapitulasi (PERBAIKAN UTAMA DISINI)
        # Mengelompokkan berdasarkan nama_pos, bukan hanya kecamatan
        query_table = text(f"""
            SELECT 
                nama_pos,
                kecamatan, 
                SUM(CASE WHEN EXTRACT(DAY FROM tanggal) <= 10 THEN curah_hujan ELSE 0 END) as das1,
                SUM(CASE WHEN EXTRACT(DAY FROM tanggal) BETWEEN 11 AND 20 THEN curah_hujan ELSE 0 END) as das2,
                SUM(CASE WHEN EXTRACT(DAY FROM tanggal) > 20 THEN curah_hujan ELSE 0 END) as das3,
                SUM(curah_hujan) as total,
                COUNT(CASE WHEN curah_hujan >= 1 THEN 1 END) as hh
            FROM curah_hujan_harian
            WHERE EXTRACT(YEAR FROM tanggal) = {year} 
              AND EXTRACT(MONTH FROM tanggal) = {month}
            GROUP BY nama_pos, kecamatan  -- Dikelompokkan per POS, bukan gabungan kecamatan
            ORDER BY total DESC
        """)
        df_table = pd.read_sql(query_table, conn)
        df_table = df_table.fillna(0)

        # 4. Query Peta DAS (Agar Popup Muncul di Layer DAS)
        query_das_map = text(f"""
            SELECT 
                nama_pos, lintang, bujur,
                SUM(CASE WHEN EXTRACT(DAY FROM tanggal) <= 10 THEN curah_hujan ELSE 0 END) as das1_val,
                SUM(CASE WHEN EXTRACT(DAY FROM tanggal) BETWEEN 11 AND 20 THEN curah_hujan ELSE 0 END) as das2_val,
                SUM(CASE WHEN EXTRACT(DAY FROM tanggal) > 20 THEN curah_hujan ELSE 0 END) as das3_val
            FROM curah_hujan_harian
            WHERE EXTRACT(YEAR FROM tanggal) = {year} 
              AND EXTRACT(MONTH FROM tanggal) = {month}
            GROUP BY nama_pos, lintang, bujur
        """)
        df_das_map = pd.read_sql(query_das_map, conn)
        df_das_map = df_das_map.fillna(0)

        conn.close()
        
        return jsonify({
            "daily": df_daily.to_dict(orient='records'),
            "accumulation": df_accum.to_dict(orient='records'),
            "table_data": df_table.to_dict(orient='records'), # Data Tabel sekarang per POS
            "das_map": df_das_map.to_dict(orient='records')   # Untuk Peta DAS
        })
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, port=5000)