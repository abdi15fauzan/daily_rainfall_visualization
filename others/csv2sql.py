import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, date
import sys

# ================== KONFIGURASI ==================
SOURCE_URLS = [
    # URL 1: Data Curah Hujan (Sheet Utama)
    'https://docs.google.com/spreadsheets/d/e/2PACX-1vTfOhV4FOzPfnrQYG7mIpR0ejzzgUvjc3c0MyIkzHU-aFmewiQuv-zczJB0rwO-T0oUF7aY5CXJVLOY/pub?output=csv',
    
    # URL 2: Data Link Peta (Sheet Peta - dengan GID dan output CSV yang benar)
    #'https://docs.google.com/spreadsheets/d/e/2PACX-1vSOEy4BaqgfHxvNDRGbaaAvBEyMQqKlY8xmyOOTzGF-iywkQFDM-bFIadLZmx-6RQ-pN2vwLmoRI2qg/pub?gid=770549085&single=true&output=csv'
]

DB_URI = 'postgresql://neondb_owner:npg_UMB35qZRoWzf@ep-steep-grass-a1183odj-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
# =================================================

def clean_numeric(val):
    if pd.isna(val) or val == '' or str(val).strip() == '-': return 0
    try: return float(str(val).replace(',', '.'))
    except: return 0

def sanitize_value(val):
    str_val = str(val).strip()
    if str_val in ['9999', '', 'nan', 'None'] or pd.isna(val): return None, 'Kosong (9999)'
    elif str_val == '8888': return 0, 'Trace (8888)'
    try: return float(val), 'Terukur'
    except: return None, 'Error'

def parse_latlong(latlong_str):
    try:
        if pd.isna(latlong_str) or str(latlong_str).strip() == '': return None, None
        parts = str(latlong_str).split(',')
        if len(parts) >= 2: return float(parts[0].strip()), float(parts[1].strip())
        return None, None
    except: return None, None

def get_file_type(df):
    """Menentukan jenis file berdasarkan kolom yang tersedia"""
    cols = [c.lower() for c in df.columns]
    if 'url_peta' in cols or 'link_peta' in cols:
        return 'MAP_FILE'
    elif 'kabupaten' in cols:
        return 'RAINFALL_FILE'
    return 'UNKNOWN'

def run_migration():
    print(f"[{datetime.now()}] --- MULAI MIGRASI CERDAS ---")
    
    # Variabel penampung
    map_reference = {} # Dictionary untuk menyimpan {tanggal: url_peta}
    rainfall_data_container = []
    
    tahun = datetime.now().year
    bulan = datetime.now().month

    # ---------------------------------------------------------
    # TAHAP 1: BACA DAN KLASIFIKASI FILE
    # ---------------------------------------------------------
    files_df = [] # Simpan DF sementara
    
    for i, url in enumerate(SOURCE_URLS):
        print(f"\n--- Membaca Sumber ke-{i+1} ---")
        try:
            # Perbaikan format URL jika user salah input (HTML -> CSV)
            if 'pubhtml' in url:
                url = url.replace('pubhtml', 'pub').replace('?', '?output=csv&')
                if 'output=csv' not in url: url += '?output=csv'
            
            # Cari header yang valid (scan 5 baris pertama)
            header_idx = 0
            try:
                temp = pd.read_csv(url, header=None, nrows=5)
                for idx, row in temp.iterrows():
                    row_str = [str(x).lower() for x in row.values]
                    if 'kabupaten' in row_str or 'url_peta' in row_str or 'date' in row_str:
                        header_idx = idx
                        break
            except: pass

            df = pd.read_csv(url, header=header_idx, dtype=str)
            df.columns = df.columns.str.strip()
            file_type = get_file_type(df)
            
            print(f"   Jenis File Terdeteksi: {file_type}")
            files_df.append({'type': file_type, 'data': df})
            
        except Exception as e:
            print(f"❌ Gagal membaca URL {i+1}: {e}")

    # ---------------------------------------------------------
    # TAHAP 2: PROSES FILE PETA (FILE 2) TERLEBIH DAHULU
    # ---------------------------------------------------------
    print("\n--- Memproses Referensi Peta ---")
    for item in files_df:
        if item['type'] == 'MAP_FILE':
            df_map = item['data']
            # Pastikan nama kolom sesuai (case insensitive)
            col_map = {c.lower(): c for c in df_map.columns}
            
            col_date = col_map.get('date')
            # Prioritaskan 'url_peta' (link AppSheet), kalau tidak ada pakai 'link_peta'
            col_url = col_map.get('url_peta') or col_map.get('link_peta')
            
            if col_date and col_url:
                for _, row in df_map.iterrows():
                    try:
                        # Parsing tanggal dari format mm/dd/yyyy atau dd/mm/yyyy
                        d_str = str(row[col_date])
                        d_obj = pd.to_datetime(d_str).date()
                        
                        url_val = row[col_url]
                        if pd.notna(url_val) and str(url_val).startswith('http'):
                            map_reference[d_obj] = str(url_val)
                    except:
                        continue
                print(f"✅ Berhasil memuat {len(map_reference)} referensi link peta.")
            else:
                print("⚠️ Kolom 'date' atau 'url_peta' tidak ditemukan di File Peta.")

    # ---------------------------------------------------------
    # TAHAP 3: PROSES FILE CURAH HUJAN (FILE 1) & GABUNGKAN
    # ---------------------------------------------------------
    print("\n--- Memproses Data Curah Hujan & Menggabungkan Peta ---")
    for item in files_df:
        if item['type'] == 'RAINFALL_FILE':
            df = item['data']
            
            # Deteksi Kolom Tanggal (1-31)
            target_dates = [f"{x:02d}" for x in range(1, 32)] 
            date_vars = [col for col in target_dates if col in df.columns]
            if not date_vars:
                date_vars = [str(x) for x in range(1, 32) if str(x) in df.columns]

            if not date_vars:
                print("❌ Skip: Tidak ada kolom tanggal di file curah hujan.")
                continue

            # Unpivot
            wanted_cols = ['Kabupaten', 'Kecamatan', 'Nama Pos', 'Nama_Pos', 'Latlong', 'Elevasi', 'DAS1', 'DAS2', 'DAS3', 'Total', 'HH', 'Data']
            actual_id_vars = [c for c in wanted_cols if c in df.columns]
            
            df_melted = pd.melt(df, id_vars=actual_id_vars, value_vars=date_vars, var_name='hari_str', value_name='nilai_mentah')

            for _, row in df_melted.iterrows():
                try:
                    nama_pos = row.get('Nama Pos') or row.get('Nama_Pos')
                    hari = int(row['hari_str'])
                    # Tanggal Data
                    tgl_fix = date(tahun, bulan, hari)
                    
                    curah, status = sanitize_value(row['nilai_mentah'])
                    lat_val, lon_val = parse_latlong(row.get('Latlong'))

                    # LOGIKA PENENTUAN LINK PETA
                    # 1. Cek apakah ada Peta Analisis (dari File 2) untuk tanggal ini?
                    final_link_peta = map_reference.get(tgl_fix)
                    
                    # 2. Jika tidak ada di File 2, buat Link Google Maps (Lokasi Pos)
                    if not final_link_peta and lat_val is not None and lon_val is not None:
                         final_link_peta = f"https://www.google.com/maps?q={lat_val},{lon_val}"

                    data_item = {
                        'kabupaten': row.get('Kabupaten'),
                        'kecamatan': row.get('Kecamatan'),
                        'nama_pos': nama_pos,
                        'lat_long_raw': str(row.get('Latlong')) if pd.notna(row.get('Latlong')) else None,
                        'lintang': lat_val,
                        'bujur': lon_val,
                        'elevasi': row.get('Elevasi'),
                        
                        # --- KOLOM LINK PETA TERINTEGRASI ---
                        'link_peta': final_link_peta, 
                        # ------------------------------------

                        'das1': clean_numeric(row.get('DAS1')),
                        'das2': clean_numeric(row.get('DAS2')),
                        'das3': clean_numeric(row.get('DAS3')),
                        'total_ch': clean_numeric(row.get('Total')),
                        'hh': clean_numeric(row.get('HH')),
                        'status_ketersediaan': row.get('Data'),
                        'tanggal': tgl_fix,
                        'curah_hujan': curah,
                        'status_data': status,
                        'raw_value': str(row['nilai_mentah'])
                    }
                    rainfall_data_container.append(data_item)
                except ValueError: continue
            print(f"✅ Data curah hujan diproses: {len(rainfall_data_container)} baris.")

    # ---------------------------------------------------------
    # TAHAP 4: SIMPAN KE DATABASE
    # ---------------------------------------------------------
    if rainfall_data_container:
        final_df = pd.DataFrame(rainfall_data_container)
        engine = create_engine(DB_URI)
        try:
            with engine.connect() as conn:
                print(f"\n[{datetime.now()}] Membersihkan data lama bulan {bulan}-{tahun}...")
                conn.execute(text(f"DELETE FROM curah_hujan_harian WHERE EXTRACT(MONTH FROM tanggal)={bulan} AND EXTRACT(YEAR FROM tanggal)={tahun}"))
                conn.commit()
            
            print(f"Menyimpan {len(final_df)} data gabungan ke PostgreSQL...")
            final_df.to_sql('curah_hujan_harian', engine, if_exists='append', index=False, chunksize=1000)
            print("✅ SUKSES! Data Curah Hujan & Link Peta (dari File 2) berhasil disimpan.")
        except Exception as e:
            print(f"❌ Error Database: {e}")
    else:
        print("⚠️ Tidak ada data yang diproses.")

if __name__ == "__main__":
    run_migration()