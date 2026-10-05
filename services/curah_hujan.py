# services/curah_hujan.py — VERSI FINAL KONSOLIDASI (Tahap 2)
import calendar
import math
import time
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text


def _clean_val(v):
    """Aturan baku: 9999 = tidak valid (dikecualikan), 8888 = valid dianggap 0.0,
    kode lain di luar itu (>887, bukan 8888) tetap dianggap tidak valid."""
    if v is None:
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(n):
        return None
    if n == 9999:
        return None
    if n == 8888:
        return 0.0
    if n > 887:
        return None
    return n


def _hitung_metrik(grp_valid, metrik):
    """Hitung satu metrik dari baris-baris valid (nilai_bersih notna) milik SATU entitas
    (satu pos, dalam rentang tanggal apa pun). Dipakai berulang untuk pos/kabupaten/provinsi."""
    if len(grp_valid) == 0:
        return None
    if metrik == "total":
        return round(float(grp_valid['nilai_bersih'].sum()), 1)
    if metrik == "rata_rata":
        # Aturan baku: total nilai valid / jumlah hari data valid (9999 sudah dikecualikan lewat dropna)
        return round(float(grp_valid['nilai_bersih'].sum() / len(grp_valid)), 2)
    if metrik == "maksimum":
        return round(float(grp_valid['nilai_bersih'].max()), 1)
    if metrik == "minimum":
        return round(float(grp_valid['nilai_bersih'].min()), 1)
    if metrik == "std_dev":
        return round(float(grp_valid['nilai_bersih'].std()), 2) if len(grp_valid) >= 2 else None
    if metrik == "kurtosis":
        return round(float(grp_valid['nilai_bersih'].kurt()), 3) if len(grp_valid) >= 4 else None
    if metrik == "modus":
        m = grp_valid['nilai_bersih'].mode()
        return round(float(m.iloc[0]), 1) if len(m) else None
    return None


# ---------- Referensi wilayah & pos ----------
def get_daftar_kabupaten(engine2):
    with engine2.connect() as conn:
        df = pd.read_sql(
            "SELECT id_wilayah, nama_wilayah FROM wilayah WHERE tipe='KABUPATEN' AND aktif=true ORDER BY nama_wilayah",
            conn)
    return df.to_dict(orient='records')


def get_daftar_pos(engine2, wilayah_id=None):
    with engine2.connect() as conn:
        if wilayah_id:
            q = text("SELECT id_kecamatan, nama_kecamatan FROM kecamatan WHERE aktif=true AND id_wilayah=:wid ORDER BY nama_kecamatan")
            df = pd.read_sql(q, conn, params={'wid': int(wilayah_id)})
        else:
            df = pd.read_sql("SELECT id_kecamatan, nama_kecamatan FROM kecamatan WHERE aktif=true ORDER BY nama_kecamatan", conn)
    return df.to_dict(orient='records')


# ---------- Ringkasan bulanan ----------
def get_data_bulanan(engine2, kecamatan_id, tahun, bulan_awal=1, bulan_akhir=12):
    with engine2.connect() as conn:
        q = text("""
            SELECT c.bulan, c.hari, c.curah_hujan
            FROM curah_hujan_harian c
            WHERE c.id_kecamatan = :kid AND c.tahun = :thn
              AND c.bulan BETWEEN :ba AND :bb
            ORDER BY c.bulan, c.hari
        """)
        df = pd.read_sql(q, conn, params={'kid': int(kecamatan_id), 'thn': tahun, 'ba': bulan_awal, 'bb': bulan_akhir})

    months_result = []
    for bulan in range(bulan_awal, bulan_akhir + 1):
        df_b = df[df['bulan'] == bulan]
        total, maks, hh = 0.0, 0.0, 0
        for _, row in df_b.iterrows():
            cv = _clean_val(row['curah_hujan'])
            if cv is not None:
                total += cv
                if cv > maks: maks = cv
                if cv >= 1: hh += 1
        months_result.append({'tahun': tahun, 'bulan': bulan, 'total': round(total, 1), 'maks': round(maks, 1), 'hh': hh})
    return months_result


def batasi_rentang_bulan(is_logged_in, tahun, bulan_awal, bulan_akhir):
    """Enforcement get_data_bulanan: publik hanya bulan berjalan."""
    if is_logged_in:
        return tahun, bulan_awal, bulan_akhir
    today = date.today()
    return today.year, today.month, today.month


# ---------- Level harian (satu tanggal spesifik) ----------
def rentang_publik_harian():
    today = date.today()
    return today - timedelta(days=30), today


def dalam_batas_publik(is_logged_in, tgl):
    if is_logged_in:
        return True
    awal, akhir = rentang_publik_harian()
    return awal <= tgl <= akhir


def potong_rentang_publik(is_logged_in, tgl_awal, tgl_akhir):
    if is_logged_in:
        return tgl_awal, tgl_akhir, False
    batas_awal, batas_akhir = rentang_publik_harian()
    baru_awal = max(tgl_awal, batas_awal)
    baru_akhir = min(tgl_akhir, batas_akhir)
    if baru_awal > baru_akhir:
        return None
    dipotong = (baru_awal != tgl_awal) or (baru_akhir != tgl_akhir)
    return baru_awal, baru_akhir, dipotong


def get_curah_hujan_harian(engine2, kecamatan_id, tahun, bulan, hari):
    tgl = date(tahun, bulan, hari)
    today = date.today()

    with engine2.connect() as conn:
        q = text("""
            SELECT curah_hujan FROM curah_hujan_harian
            WHERE id_kecamatan=:kid AND tahun=:thn AND bulan=:bln AND hari=:hr
            LIMIT 1
        """)
        df = pd.read_sql(q, conn, params={'kid': int(kecamatan_id), 'thn': tahun, 'bln': bulan, 'hr': hari})

    if len(df) == 0:
        if tgl >= today:
            return {"tersedia": False, "pesan": "Data hari ini akan muncul esok hari (sistem mencatat curah hujan hari H pada H+1)."}
        return {"tersedia": False, "pesan": "Data untuk tanggal ini tidak tersedia (kemungkinan sensor tidak mencatat atau data belum diinput)."}

    nilai = _clean_val(df.iloc[0]['curah_hujan'])
    return {"tersedia": True, "tanggal": tgl.isoformat(), "curah_hujan_mm": nilai}


# ---------- Statistik periode bebas ----------
def _query_harian_rentang(engine2, kecamatan_id, tgl_awal, tgl_akhir):
    with engine2.connect() as conn:
        q = text("""
            SELECT tahun, bulan, hari, curah_hujan
            FROM curah_hujan_harian
            WHERE id_kecamatan = :kid
              AND make_date(tahun, bulan, hari) BETWEEN :ta AND :tb
            ORDER BY tahun, bulan, hari
        """)
        df = pd.read_sql(q, conn, params={'kid': int(kecamatan_id), 'ta': tgl_awal, 'tb': tgl_akhir})
    df['nilai_bersih'] = df['curah_hujan'].apply(_clean_val)
    return df


def get_statistik_curah_hujan(engine2, kecamatan_id, tgl_awal, tgl_akhir, metrik):
    """Statistik SATU pos untuk periode apa pun (hari/dasarian/bulan/tahun/rentang bebas).
    Rata-rata = total nilai valid / jumlah hari data valid (9999 dikecualikan dari pembagi)."""
    df = _query_harian_rentang(engine2, kecamatan_id, tgl_awal, tgl_akhir)
    df_valid = df.dropna(subset=['nilai_bersih'])

    if len(df_valid) == 0:
        return {"tersedia": False, "pesan": "Tidak ada data valid pada rentang yang diminta."}

    if metrik == "maksimum":
        row = df_valid.loc[df_valid['nilai_bersih'].idxmax()]
        return {"tersedia": True, "nilai_mm": round(float(row['nilai_bersih']), 1),
                "tanggal": f"{int(row['tahun'])}-{int(row['bulan']):02d}-{int(row['hari']):02d}"}

    if metrik == "minimum":
        row = df_valid.loc[df_valid['nilai_bersih'].idxmin()]
        return {"tersedia": True, "nilai_mm": round(float(row['nilai_bersih']), 1),
                "tanggal": f"{int(row['tahun'])}-{int(row['bulan']):02d}-{int(row['hari']):02d}"}

    if metrik == "modus":
        modus_val = df_valid['nilai_bersih'].mode()
        return {"tersedia": True, "nilai_mm": [round(float(v), 1) for v in modus_val.tolist()]}

    if metrik == "total":
        return {"tersedia": True, "nilai_mm": round(float(df_valid['nilai_bersih'].sum()), 1)}

    if metrik == "std_dev":
        if len(df_valid) < 2:
            return {"tersedia": False, "pesan": "Data terlalu sedikit untuk hitung standar deviasi."}
        return {"tersedia": True, "nilai_mm": round(float(df_valid['nilai_bersih'].std()), 2), "catatan": "Standar deviasi sampel"}

    if metrik == "kurtosis":
        if len(df_valid) < 4:
            return {"tersedia": False, "pesan": "Data terlalu sedikit untuk hitung kurtosis."}
        return {"tersedia": True, "nilai": round(float(df_valid['nilai_bersih'].kurt()), 3), "catatan": "Excess kurtosis (Fisher, 0=distribusi normal)"}

    if metrik == "rata_rata":
        nilai = round(float(df_valid['nilai_bersih'].sum() / len(df_valid)), 2)
        return {"tersedia": True, "nilai_mm": nilai,
                "jumlah_hari_valid": len(df_valid),
                "metode": "total nilai valid \u00f7 jumlah hari data valid (kode 9999 dikecualikan)"}

    return {"tersedia": False, "pesan": "Metrik tidak dikenali."}

def get_statistik_wilayah(engine2, wilayah_id, tgl_awal, tgl_akhir, metrik):
    """Rollup spasial memakai metrik yang sama (_hitung_metrik) di setiap tingkat:
    - wilayah_id diisi  -> 1 tingkat: pos -> kabupaten (aturan C)
    - wilayah_id=None   -> 2 tingkat: pos -> kabupaten -> provinsi, tiap kabupaten bobot sama (aturan D)
    HANYA 1 query SQL, berapa pun cakupannya (satu kabupaten atau seluruh Kaltim)."""
    with engine2.connect() as conn:
        if wilayah_id:
            q = text("""
                SELECT k.nama_kecamatan, c.curah_hujan
                FROM kecamatan k
                JOIN curah_hujan_harian c ON c.id_kecamatan = k.id_kecamatan
                WHERE k.id_wilayah = :wid AND k.aktif = true
                  AND make_date(c.tahun, c.bulan, c.hari) BETWEEN :ta AND :tb
            """)
            df = pd.read_sql(q, conn, params={'wid': int(wilayah_id), 'ta': tgl_awal, 'tb': tgl_akhir})
        else:
            q = text("""
                SELECT k.id_wilayah, w.nama_wilayah, k.nama_kecamatan, c.curah_hujan
                FROM kecamatan k
                JOIN curah_hujan_harian c ON c.id_kecamatan = k.id_kecamatan
                JOIN wilayah w ON w.id_wilayah = k.id_wilayah
                WHERE k.aktif = true
                  AND make_date(c.tahun, c.bulan, c.hari) BETWEEN :ta AND :tb
            """)
            df = pd.read_sql(q, conn, params={'ta': tgl_awal, 'tb': tgl_akhir})

    if len(df) == 0:
        return {"tersedia": False, "pesan": "Tidak ada pos hujan/data pada kabupaten atau periode ini."}

    df['nilai_bersih'] = df['curah_hujan'].apply(_clean_val)
    satuan = "" if metrik == "kurtosis" else "mm"

    if wilayah_id:
        # ---- Aturan C: rollup 1 tingkat, pos -> kabupaten ----
        rincian = []
        for nama_pos, grp in df.groupby('nama_kecamatan'):
            nilai = _hitung_metrik(grp.dropna(subset=['nilai_bersih']), metrik)
            if nilai is not None:
                rincian.append({'nama_pos': nama_pos, 'nilai': nilai})

        if not rincian:
            return {"tersedia": False, "pesan": "Tidak ada pos dengan data valid pada periode ini."}

        nilai_list = [p['nilai'] for p in rincian]
        hasil_akhir = round(sum(nilai_list) / len(nilai_list), 2)
        return {
            "tersedia": True, "tingkat": "kabupaten", "satuan": satuan,
            "jumlah_pos_dipakai": len(rincian),
            "nilai": hasil_akhir,
            "rincian_per_pos": rincian
        }

    else:
        # ---- Aturan D: rollup 2 tingkat, pos -> kabupaten -> provinsi (bobot tiap kabupaten sama) ----
        nilai_per_kabupaten = []
        for (wid, nama_wil), grp_wil in df.groupby(['id_wilayah', 'nama_wilayah']):
            rincian_pos = []
            for nama_pos, grp_pos in grp_wil.groupby('nama_kecamatan'):
                nilai_pos = _hitung_metrik(grp_pos.dropna(subset=['nilai_bersih']), metrik)
                if nilai_pos is not None:
                    rincian_pos.append(nilai_pos)
            if rincian_pos:
                nilai_kab = round(sum(rincian_pos) / len(rincian_pos), 2)
                nilai_per_kabupaten.append({
                    'nama_wilayah': nama_wil, 'nilai': nilai_kab, 'jumlah_pos_dipakai': len(rincian_pos)
                })

        if not nilai_per_kabupaten:
            return {"tersedia": False, "pesan": "Tidak ada kabupaten dengan data valid pada periode ini."}

        nilai_list = [k['nilai'] for k in nilai_per_kabupaten]
        hasil_akhir = round(sum(nilai_list) / len(nilai_list), 2)
        return {
            "tersedia": True, "tingkat": "provinsi", "satuan": satuan,
            "jumlah_kabupaten_dipakai": len(nilai_per_kabupaten),
            "nilai": hasil_akhir,
            "rincian_per_kabupaten": nilai_per_kabupaten
        }

def get_akumulasi_hari_hujan(engine2, kecamatan_id, tahun, bulan_awal, bulan_akhir):
    bulanan = get_data_bulanan(engine2, kecamatan_id, tahun, bulan_awal, bulan_akhir)
    total_hh = sum(b['hh'] for b in bulanan)
    total_hari_kalender = sum(calendar.monthrange(tahun, b['bulan'])[1] for b in bulanan)
    return {
        "tersedia": True,
        "tahun": tahun, "bulan_awal": bulan_awal, "bulan_akhir": bulan_akhir,
        "jumlah_hari_hujan": total_hh,
        "jumlah_hari_tidak_hujan": total_hari_kalender - total_hh,
        "jumlah_hari_kalender": total_hari_kalender
    }

def get_curah_hujan_harian_wilayah(engine2, wilayah_id, tahun, bulan, hari):
    """Ambil nilai curah hujan SATU tanggal untuk SEMUA pos dalam satu kabupaten/kota,
    atau SELURUH Kaltim kalau wilayah_id=None. HANYA 1 query SQL (LEFT JOIN), bukan loop per-pos."""
    with engine2.connect() as conn:
        if wilayah_id:
            q = text("""
                SELECT k.nama_kecamatan, c.curah_hujan
                FROM kecamatan k
                LEFT JOIN curah_hujan_harian c
                    ON c.id_kecamatan = k.id_kecamatan
                    AND c.tahun = :thn AND c.bulan = :bln AND c.hari = :hr
                WHERE k.id_wilayah = :wid AND k.aktif = true
                ORDER BY k.nama_kecamatan
            """)
            df = pd.read_sql(q, conn, params={'wid': int(wilayah_id), 'thn': tahun, 'bln': bulan, 'hr': hari})
        else:
            q = text("""
                SELECT k.nama_kecamatan, c.curah_hujan
                FROM kecamatan k
                LEFT JOIN curah_hujan_harian c
                    ON c.id_kecamatan = k.id_kecamatan
                    AND c.tahun = :thn AND c.bulan = :bln AND c.hari = :hr
                WHERE k.aktif = true
                ORDER BY k.nama_kecamatan
            """)
            df = pd.read_sql(q, conn, params={'thn': tahun, 'bln': bulan, 'hr': hari})

    if len(df) == 0:
        return {"tersedia": False, "pesan": "Tidak ada pos hujan ditemukan."}

    rincian = []
    for _, row in df.iterrows():
        nilai = _clean_val(row['curah_hujan']) if row['curah_hujan'] is not None else None
        rincian.append({
            "nama_pos": row['nama_kecamatan'],
            "curah_hujan_mm": nilai,
            "keterangan": None if nilai is not None else "Data tidak tersedia/belum tercatat"
        })

    return {
        "tersedia": True,
        "tanggal": date(tahun, bulan, hari).isoformat(),
        "jumlah_pos": len(rincian),
        "data_per_pos": rincian
    }


def get_perbandingan_pos_wilayah(engine2, kecamatan_id, tgl_awal, tgl_akhir):
    """Bandingkan rata-rata SATU pos dengan rata-rata wilayahnya (aturan C: rata-rata
    dari rata-rata tiap pos di wilayah itu, bobot sama per pos). 1 query SQL."""
    with engine2.connect() as conn:
        q = text("""
            SELECT k.id_kecamatan, k.nama_kecamatan, w.nama_wilayah, c.curah_hujan
            FROM kecamatan k
            JOIN wilayah w ON w.id_wilayah = k.id_wilayah
            JOIN curah_hujan_harian c ON c.id_kecamatan = k.id_kecamatan
            WHERE k.id_wilayah = (SELECT id_wilayah FROM kecamatan WHERE id_kecamatan = :kid)
              AND k.aktif = true
              AND make_date(c.tahun, c.bulan, c.hari) BETWEEN :ta AND :tb
        """)
        df = pd.read_sql(q, conn, params={'kid': int(kecamatan_id), 'ta': tgl_awal, 'tb': tgl_akhir})

    if len(df) == 0:
        return {"tersedia": False, "pesan": "Tidak ada data pada wilayah/periode ini."}

    df['nilai_bersih'] = df['curah_hujan'].apply(_clean_val)
    nama_wilayah = df['nama_wilayah'].iloc[0]

    rata_per_pos = []
    rata_pos_target = None
    nama_pos_target = None
    for kid, grp in df.groupby('id_kecamatan'):
        nilai = _hitung_metrik(grp.dropna(subset=['nilai_bersih']), "rata_rata")
        if nilai is not None:
            rata_per_pos.append(nilai)
            if int(kid) == int(kecamatan_id):
                rata_pos_target = nilai
                nama_pos_target = grp['nama_kecamatan'].iloc[0]

    if rata_pos_target is None:
        return {"tersedia": False, "pesan": "Pos ini tidak punya data valid pada periode yang diminta."}
    if not rata_per_pos:
        return {"tersedia": False, "pesan": "Tidak ada pos dengan data valid di wilayah ini."}

    rata_wilayah = round(sum(rata_per_pos) / len(rata_per_pos), 2)
    selisih = round(rata_pos_target - rata_wilayah, 2)
    if selisih > 0:
        status = "di atas rata-rata wilayah"
    elif selisih < 0:
        status = "di bawah rata-rata wilayah"
    else:
        status = "sama dengan rata-rata wilayah"

    return {
        "tersedia": True,
        "nama_pos": nama_pos_target,
        "nama_wilayah": nama_wilayah,
        "rata_rata_pos": rata_pos_target,
        "rata_rata_wilayah": rata_wilayah,
        "selisih": selisih,
        "status": status
    }


def get_dasarian_wilayah(engine2, wilayah_id, tahun, bulan, dasarian):
    """Dasarian (DAS1/DAS2/DAS3) untuk SEMUA pos dalam satu wilayah sekaligus. 1 query SQL."""
    hari_akhir_bulan = calendar.monthrange(tahun, bulan)[1]
    rentang = {1: (1, 10), 2: (11, 20), 3: (21, hari_akhir_bulan)}
    if dasarian not in rentang:
        return {"tersedia": False, "pesan": "Dasarian harus 1, 2, atau 3."}
    hari_awal, hari_akhir = rentang[dasarian]

    with engine2.connect() as conn:
        q = text("""
            SELECT k.nama_kecamatan, c.curah_hujan
            FROM kecamatan k
            JOIN curah_hujan_harian c ON c.id_kecamatan = k.id_kecamatan
            WHERE k.id_wilayah = :wid AND k.aktif = true
              AND c.tahun = :thn AND c.bulan = :bln
              AND c.hari BETWEEN :ha AND :hb
        """)
        df = pd.read_sql(q, conn, params={'wid': int(wilayah_id), 'thn': tahun, 'bln': bulan, 'ha': hari_awal, 'hb': hari_akhir})

    if len(df) == 0:
        return {"tersedia": False, "pesan": "Tidak ada pos/data pada wilayah ini."}

    df['nilai_bersih'] = df['curah_hujan'].apply(_clean_val)

    rincian = []
    for nama_pos, grp in df.groupby('nama_kecamatan'):
        grp_valid = grp.dropna(subset=['nilai_bersih'])
        if len(grp_valid) == 0:
            rincian.append({'nama_pos': nama_pos, 'total_mm': None, 'keterangan': 'Tidak ada data valid'})
        else:
            rincian.append({'nama_pos': nama_pos, 'total_mm': round(float(grp_valid['nilai_bersih'].sum()), 1)})

    return {
        "tersedia": True,
        "tahun": tahun, "bulan": bulan, "dasarian": dasarian,
        "rentang_tanggal": f"{hari_awal}-{hari_akhir}",
        "jumlah_pos": len(rincian),
        "data_per_pos": rincian
    }


def get_data_dasarian(engine2, kecamatan_id, tahun, bulan, dasarian):
    hari_akhir_bulan = calendar.monthrange(tahun, bulan)[1]
    rentang = {1: (1, 10), 2: (11, 20), 3: (21, hari_akhir_bulan)}
    if dasarian not in rentang:
        return {"tersedia": False, "pesan": "Dasarian harus 1, 2, atau 3."}
    hari_awal, hari_akhir = rentang[dasarian]

    with engine2.connect() as conn:
        q = text("""
            SELECT curah_hujan FROM curah_hujan_harian
            WHERE id_kecamatan=:kid AND tahun=:thn AND bulan=:bln AND hari BETWEEN :ha AND :hb
        """)
        df = pd.read_sql(q, conn, params={'kid': int(kecamatan_id), 'thn': tahun, 'bln': bulan, 'ha': hari_awal, 'hb': hari_akhir})

    total, maks, hh = 0.0, 0.0, 0
    for _, row in df.iterrows():
        cv = _clean_val(row['curah_hujan'])
        if cv is not None:
            total += cv
            if cv > maks: maks = cv
            if cv >= 1: hh += 1

    return {"tersedia": True, "tahun": tahun, "bulan": bulan, "dasarian": dasarian,
            "rentang_tanggal": f"{hari_awal}-{hari_akhir}", "total_mm": round(total, 1),
            "maksimum_mm": round(maks, 1), "hari_hujan": hh}


# ---------- Rentang ketersediaan data per pos ----------
# Satu query agregat untuk seluruh pos, di-cache di memori supaya dropdown periode
# (Visualisasi & Analisis) tidak memukul database setiap kali filter berubah.
# Rentang kabupaten / seluruh Kaltim dihitung di sisi klien dari daftar ini.
_RENTANG_TTL_DETIK = 600
_rentang_cache = {"ts": 0.0, "data": None}


def get_rentang_data(engine2):
    """Bulan pertama & terakhir yang punya nilai VALID (bukan 9999 / kode tak valid)
    untuk setiap pos aktif. Format ringkas: [{k: id_kecamatan, w: id_wilayah,
    a: YYYYMM awal, b: YYYYMM akhir}]."""
    sekarang = time.time()
    if _rentang_cache["data"] is not None and sekarang - _rentang_cache["ts"] < _RENTANG_TTL_DETIK:
        return _rentang_cache["data"]

    q = text("""
        SELECT c.id_kecamatan AS k, k.id_wilayah AS w,
               MIN(c.tahun * 100 + c.bulan) AS a,
               MAX(c.tahun * 100 + c.bulan) AS b
        FROM curah_hujan_harian c
        JOIN kecamatan k ON k.id_kecamatan = c.id_kecamatan
        WHERE k.aktif = true
          AND c.curah_hujan IS NOT NULL
          AND c.curah_hujan >= 0
          AND (c.curah_hujan <= 887 OR c.curah_hujan = 8888)
        GROUP BY c.id_kecamatan, k.id_wilayah
    """)
    with engine2.connect() as conn:
        df = pd.read_sql(q, conn)

    data = [{"k": int(r.k), "w": int(r.w), "a": int(r.a), "b": int(r.b)} for r in df.itertuples()]
    _rentang_cache["data"] = data
    _rentang_cache["ts"] = sekarang
    return data