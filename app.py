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
DB_URI = os.environ.get('DATABASE_URL', 'postgresql://neondb_owner:***REMOVED***@ep-steep-grass-a1183odj-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')
engine = create_engine(DB_URI, client_encoding='utf8')

# --- DATABASE KEDUA (Historis — untuk Visualisasi & Analisis) ---
DB_URI_2 = os.environ.get('DATABASE_URL_2', 'postgresql://neondb_owner:***REMOVED***@ep-empty-cake-a1t7b4fh-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')
engine2 = create_engine(DB_URI_2, client_encoding='utf8')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/visualisasi')
def visualisasi():
    return render_template('visualisasi.html')

@app.route('/analisis')
def analisis():
    return render_template('analisis.html')

# ==============================================================================
# API VISUALISASI — Database Historis
# ==============================================================================

@app.route('/api/viz/wilayah')
def viz_wilayah():
    try:
        with engine2.connect() as conn:
            df = pd.read_sql(
                "SELECT id_wilayah, nama_wilayah FROM wilayah WHERE tipe='KABUPATEN' AND aktif=true ORDER BY nama_wilayah",
                conn)
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/viz/pos')
def viz_pos():
    wilayah_id = request.args.get('wilayah_id')
    try:
        with engine2.connect() as conn:
            if wilayah_id:
                q = text("SELECT id_kecamatan, nama_kecamatan FROM kecamatan WHERE aktif=true AND id_wilayah=:wid ORDER BY nama_kecamatan")
                df = pd.read_sql(q, conn, params={'wid': int(wilayah_id)})
            else:
                df = pd.read_sql("SELECT id_kecamatan, nama_kecamatan FROM kecamatan WHERE aktif=true ORDER BY nama_kecamatan", conn)
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/viz/tahun')
def viz_tahun():
    try:
        with engine2.connect() as conn:
            df = pd.read_sql("SELECT DISTINCT tahun FROM curah_hujan_harian ORDER BY tahun DESC", conn)
        return jsonify(df['tahun'].tolist())
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/viz/data-kabupaten')
def viz_data_kabupaten():
    """Agregat per pos dalam satu kabupaten, atau per kabupaten jika wilayah_id=all."""
    import calendar
    wilayah_id = request.args.get('wilayah_id')
    tahun  = int(request.args.get('tahun',  2026))
    bulan  = int(request.args.get('bulan',  1))
    days_in_month = int(calendar.monthrange(tahun, bulan)[1])

    try:
        with engine2.connect() as conn:
            if wilayah_id and wilayah_id != 'all':
                # ── PER POS dalam satu kabupaten ──
                q = text("""
                    SELECT k.nama_kecamatan AS label,
                        COALESCE(ROUND(SUM(CASE WHEN c.curah_hujan < 8888 THEN c.curah_hujan ELSE 0 END)::numeric,1),0) AS total,
                        COALESCE(ROUND(SUM(CASE WHEN c.hari <= 10 AND c.curah_hujan < 8888 THEN c.curah_hujan ELSE 0 END)::numeric,1),0) AS das1,
                        COALESCE(ROUND(SUM(CASE WHEN c.hari BETWEEN 11 AND 20 AND c.curah_hujan < 8888 THEN c.curah_hujan ELSE 0 END)::numeric,1),0) AS das2,
                        COALESCE(ROUND(SUM(CASE WHEN c.hari > 20 AND c.curah_hujan < 8888 THEN c.curah_hujan ELSE 0 END)::numeric,1),0) AS das3,
                        COUNT(CASE WHEN c.curah_hujan >= 1 AND c.curah_hujan < 8888 THEN 1 END) AS hh
                    FROM curah_hujan_harian c
                    JOIN kecamatan k ON c.id_kecamatan = k.id_kecamatan
                    WHERE k.id_wilayah = :wid AND c.tahun = :thn AND c.bulan = :bln
                    GROUP BY k.id_kecamatan, k.nama_kecamatan
                    ORDER BY total DESC
                """)
                df = pd.read_sql(q, conn, params={'wid': int(wilayah_id), 'thn': tahun, 'bln': bulan})
                df = df.fillna(0)
                # rerata_harian per pos = total_pos / hari_dalam_bulan
                df['rerata_harian'] = (df['total'].astype(float) / days_in_month).round(2)

            else:
                # ── PER KABUPATEN (tingkat provinsi) ──
                # Step 1: ambil data per pos per wilayah
                q = text("""
                    SELECT k.id_wilayah,
                        k.id_kecamatan,
                        COALESCE(SUM(CASE WHEN c.curah_hujan < 8888 THEN c.curah_hujan ELSE 0 END),0) AS pos_total,
                        COALESCE(SUM(CASE WHEN c.hari <= 10 AND c.curah_hujan < 8888 THEN c.curah_hujan ELSE 0 END),0) AS pos_das1,
                        COALESCE(SUM(CASE WHEN c.hari BETWEEN 11 AND 20 AND c.curah_hujan < 8888 THEN c.curah_hujan ELSE 0 END),0) AS pos_das2,
                        COALESCE(SUM(CASE WHEN c.hari > 20 AND c.curah_hujan < 8888 THEN c.curah_hujan ELSE 0 END),0) AS pos_das3,
                        COUNT(CASE WHEN c.curah_hujan >= 1 AND c.curah_hujan < 8888 THEN 1 END) AS pos_hh,
                        w.nama_wilayah AS label
                    FROM curah_hujan_harian c
                    JOIN kecamatan k ON c.id_kecamatan = k.id_kecamatan
                    JOIN wilayah w ON k.id_wilayah = w.id_wilayah
                    WHERE c.tahun = :thn AND c.bulan = :bln
                    GROUP BY k.id_wilayah, k.id_kecamatan, w.nama_wilayah
                """)
                df_pos = pd.read_sql(q, conn, params={'thn': tahun, 'bln': bulan})
                df_pos = df_pos.fillna(0)

                # Step 2: agregasi per kabupaten di Python
                # total  = SUM(pos_total) / COUNT(pos yang pos_total > 0)   [pos 0 dikecualikan]
                # das1/2/3 = RERATA per pos yang nilainya > 0
                def agg_kab(grp):
                    nonzero_total = (grp['pos_total'] > 0).sum()
                    nonzero_das1  = (grp['pos_das1']  > 0).sum()
                    nonzero_das2  = (grp['pos_das2']  > 0).sum()
                    nonzero_das3  = (grp['pos_das3']  > 0).sum()
                    return pd.Series({
                        'total': round(grp['pos_total'].sum() / max(nonzero_total, 1), 1),
                        'das1':  round(grp['pos_das1'].sum()  / max(nonzero_das1,  1), 1),
                        'das2':  round(grp['pos_das2'].sum()  / max(nonzero_das2,  1), 1),
                        'das3':  round(grp['pos_das3'].sum()  / max(nonzero_das3,  1), 1),
                        'hh':    grp['pos_hh'].sum(),
                        'n_pos': nonzero_total
                    })

                df = df_pos.groupby(['id_wilayah', 'label']).apply(agg_kab).reset_index()

                # rerata_harian = total / hari_dalam_bulan (total sudah rerata per pos)
                df['rerata_harian'] = (
                    df['total'].astype(float) / days_in_month
                ).round(2)

                df = df.sort_values('total', ascending=False)

        return jsonify(df.fillna(0).to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/viz/tahunan-summary')
def viz_tahunan_summary():
    """
    Monthly aggregates per entity for a given year.
    Modes:
      - kecamatan_id provided  → single pos, 12-month stats
      - wilayah_id (not all)   → per-pos monthly stats within that kab
      - wilayah_id == all/None → per-kab monthly stats (provinsi)
    """
    import math
    tahun = int(request.args.get('tahun', 2023))
    wilayah_id   = request.args.get('wilayah_id')
    kecamatan_id = request.args.get('kecamatan_id')

    def safe_float(v):
        try:
            f = float(v)
            return 0.0 if (math.isnan(f) or math.isinf(f)) else round(f, 1)
        except Exception:
            return 0.0

    def build_monthly_rows(grp):
        rows = []
        for m in range(1, 13):
            r = grp[grp['bulan'] == m]
            if len(r) > 0:
                rows.append({
                    'bulan':  m,
                    'total':  safe_float(r['total'].iloc[0]),
                    'maks':   safe_float(r['maks'].iloc[0]),
                    'rerata': safe_float(r['rerata'].iloc[0]),
                    'hh':     int(r['hh'].iloc[0])
                })
            else:
                rows.append({'bulan': m, 'total': 0, 'maks': 0, 'rerata': 0, 'hh': 0})
        return rows

    try:
        with engine2.connect() as conn:
            # ── MODE 1: Specific pos ──
            if kecamatan_id:
                q = text("""
                    SELECT c.bulan,
                        COALESCE(ROUND(SUM(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE 0 END)::numeric,1),0) AS total,
                        COALESCE(MAX(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE NULL END),0)              AS maks,
                        COALESCE(ROUND(AVG(CASE WHEN curah_hujan >= 1 AND curah_hujan < 8888 THEN curah_hujan ELSE NULL END)::numeric,1),0) AS rerata,
                        COUNT(CASE WHEN curah_hujan >= 1 AND curah_hujan < 8888 THEN 1 END)                       AS hh,
                        k.nama_kecamatan AS label, w.nama_wilayah AS kab_label
                    FROM curah_hujan_harian c
                    JOIN kecamatan k ON c.id_kecamatan = k.id_kecamatan
                    JOIN wilayah w ON k.id_wilayah = w.id_wilayah
                    WHERE c.id_kecamatan = :kid AND c.tahun = :thn
                    GROUP BY c.bulan, k.nama_kecamatan, w.nama_wilayah
                    ORDER BY c.bulan
                """)
                df = pd.read_sql(q, conn, params={'kid': int(kecamatan_id), 'thn': tahun}).fillna(0)
                lbl = df['label'].iloc[0] if len(df) > 0 else 'Pos'
                return jsonify({
                    'mode': 'pos',
                    'entities': [{'label': lbl, 'data': build_monthly_rows(df)}]
                })

            # ── MODE 2: Specific kab → per-pos breakdown ──
            elif wilayah_id and wilayah_id != 'all':
                q = text("""
                    SELECT c.bulan, k.id_kecamatan,
                        k.nama_kecamatan AS label,
                        COALESCE(ROUND(SUM(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE 0 END)::numeric,1),0) AS total,
                        COALESCE(MAX(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE NULL END),0)              AS maks,
                        COALESCE(ROUND(AVG(CASE WHEN curah_hujan >= 1 AND curah_hujan < 8888 THEN curah_hujan ELSE NULL END)::numeric,1),0) AS rerata,
                        COUNT(CASE WHEN curah_hujan >= 1 AND curah_hujan < 8888 THEN 1 END)                       AS hh
                    FROM curah_hujan_harian c
                    JOIN kecamatan k ON c.id_kecamatan = k.id_kecamatan
                    WHERE k.id_wilayah = :wid AND c.tahun = :thn
                    GROUP BY c.bulan, k.id_kecamatan, k.nama_kecamatan
                    ORDER BY k.nama_kecamatan, c.bulan
                """)
                df = pd.read_sql(q, conn, params={'wid': int(wilayah_id), 'thn': tahun}).fillna(0)
                entities = []
                for kid, grp in df.groupby('id_kecamatan'):
                    entities.append({'label': grp['label'].iloc[0], 'data': build_monthly_rows(grp)})
                return jsonify({'mode': 'kab', 'entities': entities})

            # ── MODE 3: All → per-kab aggregated ──
            else:
                # Get per-pos per-month data, then aggregate per kab
                q = text("""
                    SELECT c.bulan, k.id_wilayah, k.id_kecamatan, w.nama_wilayah AS label,
                        SUM(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE 0 END) AS pos_total,
                        MAX(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE NULL END) AS pos_maks,
                        AVG(CASE WHEN curah_hujan >= 1 AND curah_hujan < 8888 THEN curah_hujan ELSE NULL END) AS pos_rerata,
                        COUNT(CASE WHEN curah_hujan >= 1 AND curah_hujan < 8888 THEN 1 END) AS pos_hh
                    FROM curah_hujan_harian c
                    JOIN kecamatan k ON c.id_kecamatan = k.id_kecamatan
                    JOIN wilayah w ON k.id_wilayah = w.id_wilayah
                    WHERE c.tahun = :thn
                    GROUP BY c.bulan, k.id_wilayah, k.id_kecamatan, w.nama_wilayah
                    ORDER BY w.nama_wilayah, c.bulan
                """)
                df_pos = pd.read_sql(q, conn, params={'thn': tahun}).fillna(0)
                # Aggregate per kab per month in Python
                # total = akumulasi semua pos / jumlah pos yang punya nilai (>0)
                def kab_agg(grp):
                    nonzero_total = grp[grp['pos_total'] > 0]['pos_total'].count()
                    return pd.Series({
                        'total':  grp['pos_total'].sum() / max(nonzero_total, 1),
                        'maks':   grp['pos_maks'].max(),
                        'rerata': grp[grp['pos_rerata'] > 0]['pos_rerata'].mean() if (grp['pos_rerata'] > 0).any() else 0,
                        'hh':     grp['pos_hh'].sum()
                    })
                df_kab = df_pos.groupby(['id_wilayah', 'label', 'bulan']).apply(kab_agg).reset_index()
                entities = []
                for wid, grp in df_kab.groupby('id_wilayah'):
                    entities.append({'label': grp['label'].iloc[0], 'data': build_monthly_rows(grp)})
                return jsonify({'mode': 'provinsi', 'entities': entities})

    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/viz/dekade-summary')
def viz_dekade_summary():
    """
    10-year (dekade) aggregates.
    tahun_awal: start year. period = tahun_awal to tahun_awal+9
    Validation: tahun_awal > 2018 → invalid; missing years → flag warning
    Modes:
      kecamatan_id → per pos (all metrics + stats)
      wilayah_id   → per-pos aggregate within kab
      (none)       → per-kab provinsi
    """
    import math, calendar
    tahun_awal   = int(request.args.get('tahun_awal', 2016))
    wilayah_id   = request.args.get('wilayah_id')
    kecamatan_id = request.args.get('kecamatan_id')
    tahun_akhir  = tahun_awal + 9
    tahun_range  = list(range(tahun_awal, tahun_akhir + 1))

    if tahun_awal > 2018:
        return jsonify({'error': f'Periode mulai {tahun_awal} tidak valid. Gunakan tahun awal ≤ 2018.'})

    def safe(v):
        try:
            f = float(v)
            return 0.0 if (math.isnan(f) or math.isinf(f)) else round(f, 2)
        except: return 0.0

    try:
        with engine2.connect() as conn:

            # ── Helper: check available years ──
            yr_check = pd.read_sql(
                text("SELECT DISTINCT tahun FROM curah_hujan_harian WHERE tahun BETWEEN :ta AND :tb ORDER BY tahun"),
                conn, params={'ta': tahun_awal, 'tb': tahun_akhir}
            )
            available_years = yr_check['tahun'].tolist()
            missing_years   = [y for y in tahun_range if y not in available_years]

            if kecamatan_id:
                kid = int(kecamatan_id)
                # Per-pos: monthly & yearly aggregates
                q = text("""
                    SELECT tahun, bulan,
                        SUM(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE 0 END)              AS total,
                        MAX(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE NULL END)            AS maks,
                        COUNT(CASE WHEN curah_hujan >= 50  AND curah_hujan < 8888 THEN 1 END)       AS cnt_lebat,
                        COUNT(CASE WHEN curah_hujan >= 100 AND curah_hujan < 8888 THEN 1 END)       AS cnt_ekstrem
                    FROM curah_hujan_harian
                    WHERE id_kecamatan=:kid AND tahun BETWEEN :ta AND :tb
                    GROUP BY tahun, bulan ORDER BY tahun, bulan
                """)
                df = pd.read_sql(q, conn, params={'kid':kid,'ta':tahun_awal,'tb':tahun_akhir}).fillna(0)

                # Monthly averages over 10 years
                monthly = []
                for m in range(1, 13):
                    rows = df[df['bulan'] == m]
                    monthly.append({
                        'bulan': m,
                        'rerata_total': safe(rows['total'].sum() / len(tahun_range)),
                        'maks': safe(rows['maks'].max()),
                        'cnt_lebat':  int(rows['cnt_lebat'].sum()),
                        'cnt_ekstrem':int(rows['cnt_ekstrem'].sum())
                    })

                # Yearly maks
                yearly = []
                for y in tahun_range:
                    rows = df[df['tahun'] == y]
                    yearly.append({'tahun': y, 'maks': safe(rows['maks'].max()), 'total': safe(rows['total'].sum())})

                # Stats: annual totals
                annual_totals = [sum(df[df['tahun']==y]['total'].sum() for _ in [1]) for y in tahun_range]
                # More precisely:
                annual_totals = []
                for y in tahun_range:
                    s = df[df['tahun']==y]['total'].sum()
                    annual_totals.append(float(s))
                n = len(tahun_range)
                rerata_tahunan = sum(annual_totals) / n

                # Monthly means (12 months)
                monthly_means = [monthly[m]['rerata_total'] for m in range(12)]
                rerata_bulanan = sum(monthly_means) / 12

                # Std dev, CV, kurtosis of annual_totals
                mean_a = rerata_tahunan
                var_a  = sum((v - mean_a)**2 for v in annual_totals) / n
                std_a  = math.sqrt(var_a)
                cv_a   = (std_a / mean_a * 100) if mean_a > 0 else 0
                m4_a   = sum((v - mean_a)**4 for v in annual_totals) / n
                kurt_a = ((m4_a / var_a**2) - 3) if var_a > 0 else 0
                rentang_a = max(annual_totals) - min(annual_totals)

                # Pos label
                lbl_row = pd.read_sql(text("SELECT nama_kecamatan FROM kecamatan WHERE id_kecamatan=:k"), conn, params={'k':kid})
                lbl = lbl_row['nama_kecamatan'].iloc[0] if len(lbl_row) else 'Pos'

                return jsonify({
                    'mode': 'pos', 'label': lbl,
                    'tahun_awal': tahun_awal, 'tahun_akhir': tahun_akhir,
                    'missing_years': missing_years,
                    'monthly': monthly, 'yearly': yearly,
                    'stats': {
                        'rerata_tahunan': round(rerata_tahunan,1),
                        'rerata_bulanan':  round(rerata_bulanan,1),
                        'rentang': round(rentang_a,1),
                        'std': round(std_a,1), 'cv': round(cv_a,1),
                        'kurtosis': round(kurt_a,2)
                    }
                })

            elif wilayah_id and wilayah_id != 'all':
                wid = int(wilayah_id)
                q = text("""
                    SELECT c.tahun, c.bulan, k.id_kecamatan,
                        SUM(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE 0 END)              AS total,
                        MAX(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE NULL END)            AS maks,
                        COUNT(CASE WHEN curah_hujan >= 50  AND curah_hujan < 8888 THEN 1 END)       AS cnt_lebat,
                        COUNT(CASE WHEN curah_hujan >= 100 AND curah_hujan < 8888 THEN 1 END)       AS cnt_ekstrem
                    FROM curah_hujan_harian c
                    JOIN kecamatan k ON c.id_kecamatan = k.id_kecamatan
                    WHERE k.id_wilayah=:wid AND c.tahun BETWEEN :ta AND :tb
                    GROUP BY c.tahun, c.bulan, k.id_kecamatan
                """)
                df = pd.read_sql(q, conn, params={'wid':wid,'ta':tahun_awal,'tb':tahun_akhir}).fillna(0)

                # Aggregate per month (avg across pos with data)
                monthly = []
                for m in range(1, 13):
                    rows = df[df['bulan'] == m]
                    # per pos per year total for this month
                    pos_yr = rows.groupby(['id_kecamatan','tahun'])['total'].sum().reset_index()
                    # mean over 10 years per pos, then mean across pos
                    pos_means = pos_yr.groupby('id_kecamatan')['total'].mean()
                    rerata = pos_means.mean() if len(pos_means) else 0
                    monthly.append({
                        'bulan': m,
                        'rerata_total': safe(rerata),
                        'maks': safe(rows['maks'].max()),
                        'cnt_lebat':  int(rows['cnt_lebat'].sum()),
                        'cnt_ekstrem':int(rows['cnt_ekstrem'].sum())
                    })

                yearly = []
                for y in tahun_range:
                    rows = df[df['tahun']==y]
                    # per pos total
                    pos_totals = rows.groupby('id_kecamatan')['total'].sum()
                    nonzero = pos_totals[pos_totals>0]
                    total_kab = nonzero.sum() / max(len(nonzero),1)
                    yearly.append({'tahun':y, 'maks':safe(rows['maks'].max()), 'total':safe(total_kab)})

                wlbl = pd.read_sql(text("SELECT nama_wilayah FROM wilayah WHERE id_wilayah=:w"), conn, params={'w':wid})
                lbl = wlbl['nama_wilayah'].iloc[0] if len(wlbl) else 'Kabupaten'

                return jsonify({
                    'mode':'kab', 'label':lbl,
                    'tahun_awal':tahun_awal,'tahun_akhir':tahun_akhir,
                    'missing_years':missing_years,
                    'monthly':monthly,'yearly':yearly
                })

            else:
                # Provinsi: per kab
                q = text("""
                    SELECT c.tahun, c.bulan, k.id_wilayah, k.id_kecamatan, w.nama_wilayah AS label,
                        SUM(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE 0 END) AS pos_total,
                        MAX(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE NULL END) AS pos_maks
                    FROM curah_hujan_harian c
                    JOIN kecamatan k ON c.id_kecamatan=k.id_kecamatan
                    JOIN wilayah w ON k.id_wilayah=w.id_wilayah
                    WHERE c.tahun BETWEEN :ta AND :tb
                    GROUP BY c.tahun, c.bulan, k.id_wilayah, k.id_kecamatan, w.nama_wilayah
                """)
                df = pd.read_sql(q, conn, params={'ta':tahun_awal,'tb':tahun_akhir}).fillna(0)

                # Yearly total per kab (avg of nonzero pos)
                entities_yearly = {}
                entities_monthly = {}
                for wid_g, grp_kab in df.groupby('id_wilayah'):
                    lbl = grp_kab['label'].iloc[0]
                    yearly = []
                    for y in tahun_range:
                        rows_y = grp_kab[grp_kab['tahun']==y]
                        pos_totals = rows_y.groupby('id_kecamatan')['pos_total'].sum()
                        nz = pos_totals[pos_totals>0]
                        total = nz.sum()/max(len(nz),1)
                        yearly.append({'tahun':y,'total':safe(total)})
                    entities_yearly[str(wid_g)] = {'label':lbl,'yearly':yearly}

                    monthly = []
                    for m in range(1,13):
                        rows_m = grp_kab[grp_kab['bulan']==m]
                        pos_yr = rows_m.groupby(['id_kecamatan','tahun'])['pos_total'].sum().reset_index()
                        pos_means = pos_yr.groupby('id_kecamatan')['pos_total'].mean()
                        rerata = pos_means.mean() if len(pos_means) else 0
                        monthly.append({'bulan':m,'rerata_total':safe(rerata)})
                    entities_monthly[str(wid_g)] = {'label':lbl,'monthly':monthly}

                return jsonify({
                    'mode':'provinsi',
                    'tahun_awal':tahun_awal,'tahun_akhir':tahun_akhir,
                    'missing_years':missing_years,
                    'entities_yearly': list(entities_yearly.values()),
                    'entities_monthly': list(entities_monthly.values())
                })

    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/viz/data-pos')
def viz_data_pos():
    """Data harian satu pos dalam satu bulan, termasuk rekap dasarian."""
    kecamatan_id = request.args.get('kecamatan_id')
    tahun  = request.args.get('tahun',  2026)
    bulan  = request.args.get('bulan',  1)
    if not kecamatan_id:
        return jsonify({'error': 'kecamatan_id diperlukan'})
    try:
        with engine2.connect() as conn:
            # Data harian
            q_daily = text("""
                SELECT c.hari, c.tanggal::text AS tanggal, c.curah_hujan, c.keterangan,
                       k.nama_kecamatan, w.nama_wilayah
                FROM curah_hujan_harian c
                JOIN kecamatan k ON c.id_kecamatan = k.id_kecamatan
                JOIN wilayah w ON k.id_wilayah = w.id_wilayah
                WHERE c.id_kecamatan = :kid AND c.tahun = :thn AND c.bulan = :bln
                ORDER BY c.hari
            """)
            df_d = pd.read_sql(q_daily, conn, params={'kid': int(kecamatan_id), 'thn': int(tahun), 'bln': int(bulan)})

            # Rekap dasarian
            q_das = text("""
                SELECT
                    CASE
                        WHEN hari <= 10 THEN 'das1'
                        WHEN hari BETWEEN 11 AND 20 THEN 'das2'
                        ELSE 'das3'
                    END AS dasarian,
                    ROUND(SUM(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE 0 END)::numeric,1) AS total,
                    MAX(CASE WHEN curah_hujan < 8888 THEN curah_hujan ELSE NULL END) AS maks,
                    (array_agg(hari ORDER BY curah_hujan DESC NULLS LAST))[1] AS hari_maks,
                    (array_agg(tanggal::text ORDER BY curah_hujan DESC NULLS LAST))[1] AS tgl_maks,
                    COUNT(CASE WHEN curah_hujan >= 1 AND curah_hujan < 8888 THEN 1 END) AS hh,
                    COUNT(CASE WHEN curah_hujan < 1 OR curah_hujan IS NULL THEN 1 END) AS hth
                FROM curah_hujan_harian
                WHERE id_kecamatan = :kid AND tahun = :thn AND bulan = :bln
                GROUP BY dasarian ORDER BY dasarian
            """)
            df_das = pd.read_sql(q_das, conn, params={'kid': int(kecamatan_id), 'thn': int(tahun), 'bln': int(bulan)})

        return jsonify({
            'daily': df_d.fillna(0).to_dict(orient='records'),
            'dasarian': df_das.fillna(0).to_dict(orient='records')
        })
    except Exception as e:
        return jsonify({'error': str(e)})

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
            SELECT nama_pos, kecamatan, kabupaten, latitude, longitude, 
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
        query_daily = text(f"SELECT nama_pos, latitude, longitude, curah_hujan FROM curah_hujan_harian WHERE tanggal = '{date_param}'")
        df_daily = pd.read_sql(query_daily, conn)
        df_daily['curah_hujan'] = df_daily['curah_hujan'].fillna(0).astype(float)

        # 2. Query Akumulasi Bulanan s.d Tanggal Terpilih (Bubble Peta)
        query_accum = text(f"""
            SELECT nama_pos, latitude, longitude, SUM(curah_hujan) as total_akumulasi
            FROM curah_hujan_harian
            WHERE EXTRACT(YEAR FROM tanggal) = {year} 
              AND EXTRACT(MONTH FROM tanggal) = {month} 
              AND EXTRACT(DAY FROM tanggal) <= {day}
            GROUP BY nama_pos, latitude, longitude
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
                nama_pos, latitude, longitude,
                SUM(CASE WHEN EXTRACT(DAY FROM tanggal) <= 10 THEN curah_hujan ELSE 0 END) as das1_val,
                SUM(CASE WHEN EXTRACT(DAY FROM tanggal) BETWEEN 11 AND 20 THEN curah_hujan ELSE 0 END) as das2_val,
                SUM(CASE WHEN EXTRACT(DAY FROM tanggal) > 20 THEN curah_hujan ELSE 0 END) as das3_val
            FROM curah_hujan_harian
            WHERE EXTRACT(YEAR FROM tanggal) = {year} 
              AND EXTRACT(MONTH FROM tanggal) = {month}
            GROUP BY nama_pos, latitude, longitude
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

# ==============================================================================
# TAMBAHKAN KODE INI KE app.py — SEBELUM BARIS: if __name__ == '__main__':
# ==============================================================================

@app.route('/api/analisis/matriks')
def analisis_matriks():
    """
    Matriks CH Harian per pos, per rentang tahun.
    Params: kecamatan_id, tahun_awal, tahun_akhir
    Returns: {years: [ {tahun, days, dasarian, monthly, yearly_summary, days_in_month}, ... ]}

    Aturan nilai:
      - 0–887       → valid
      - 8888 / 8888.0 → 0.1 mm (Trace), ditampilkan sebagai nilai sangat kecil
      - 9999          → tidak tersedia → '-'
      - x > 887 (bukan 8888) → tidak valid → '-'

    Persentase = (hari valid / total hari dalam bulan) * 100
    Hari valid = baris dengan curah_hujan antara 0 dan 887 inklusif, PLUS 8888.
    """
    import calendar, math

    kecamatan_id = request.args.get('kecamatan_id')
    tahun_awal   = request.args.get('tahun_awal',  type=int)
    tahun_akhir  = request.args.get('tahun_akhir', type=int)

    if not kecamatan_id:
        return jsonify({'error': 'kecamatan_id diperlukan'})
    if not tahun_awal or not tahun_akhir:
        return jsonify({'error': 'tahun_awal dan tahun_akhir diperlukan'})
    if tahun_akhir < tahun_awal:
        return jsonify({'error': 'tahun_akhir tidak boleh lebih kecil dari tahun_awal'})
    if (tahun_akhir - tahun_awal + 1) > 20:
        return jsonify({'error': 'Maksimal 20 tahun dalam satu permintaan'})

    def is_valid(v):
        """Return True jika nilai termasuk dalam perhitungan persentase."""
        if v is None: return False
        try:
            n = float(v)
        except: return False
        if math.isnan(n): return False
        if n == 8888: return True        # trace
        if 0 <= n <= 887: return True
        return False  # 9999, >887 (selain 8888)

    def display_val(v):
        """Return float untuk JSON; None → None."""
        if v is None: return None
        try:
            n = float(v)
        except: return None
        if math.isnan(n): return None
        if n == 8888: return 8888        # frontend tangani sebagai trace
        if n == 9999: return 9999        # frontend tampilkan sebagai '-'
        if n > 887:   return 9999        # tidak valid → samakan ke 9999
        return round(n, 1)

    try:
        with engine2.connect() as conn:
            # Ambil semua data sekaligus untuk rentang tahun
            q = text("""
                SELECT c.tahun, c.bulan, c.hari, c.curah_hujan
                FROM curah_hujan_harian c
                WHERE c.id_kecamatan = :kid
                  AND c.tahun BETWEEN :ta AND :tb
                ORDER BY c.tahun, c.bulan, c.hari
            """)
            df = pd.read_sql(q, conn, params={
                'kid': int(kecamatan_id),
                'ta':  tahun_awal,
                'tb':  tahun_akhir
            })

        years_result = []

        for tahun in range(tahun_awal, tahun_akhir + 1):
            df_yr = df[df['tahun'] == tahun]

            # days_in_month untuk tahun ini
            dim = {m: calendar.monthrange(tahun, m)[1] for m in range(1, 13)}

            # Build pivot: row per hari (1–31), col per bulan (m1..m12)
            days_list = []
            for d in range(1, 32):
                row = {'hari': d}
                for m in range(1, 13):
                    cell = df_yr[(df_yr['bulan'] == m) & (df_yr['hari'] == d)]
                    if len(cell) > 0:
                        row[f'm{m}'] = display_val(cell['curah_hujan'].iloc[0])
                    else:
                        row[f'm{m}'] = None
                days_list.append(row)

            # ── Dasarian per bulan ──
            def dasarian_sum(bulan, das_start, das_end):
                sub = df_yr[
                    (df_yr['bulan'] == bulan) &
                    (df_yr['hari'] >= das_start) &
                    (df_yr['hari'] <= das_end)
                ]
                total = 0
                for _, r in sub.iterrows():
                    v = r['curah_hujan']
                    if v is None: continue
                    try: n = float(v)
                    except: continue
                    if math.isnan(n): continue
                    if n == 8888: total += 0.1
                    elif 0 <= n <= 887: total += n
                    # lainnya tidak dihitung
                return round(total, 1) if total > 0 else 0

            # Akhir bulan untuk das3
            def das3_end(m): return dim[m]

            das1 = {m: dasarian_sum(m, 1,  10)        for m in range(1,13)}
            das2 = {m: dasarian_sum(m, 11, 20)        for m in range(1,13)}
            das3 = {m: dasarian_sum(m, 21, das3_end(m)) for m in range(1,13)}

            # ── Monthly summary per bulan ──
            monthly_jumlah = {}
            monthly_maks   = {}
            monthly_hh     = {}
            monthly_pct    = {}

            for m in range(1, 13):
                sub = df_yr[df_yr['bulan'] == m]
                total = 0; maks = 0; hh = 0; valid_days = 0
                for _, r in sub.iterrows():
                    v = r['curah_hujan']
                    if v is None: continue
                    try: n = float(v)
                    except: continue
                    if math.isnan(n): continue
                    if n == 8888:
                        total += 0.1
                        valid_days += 1
                        # 8888 bukan hujan (< 1mm)
                    elif 0 <= n <= 887:
                        total      += n
                        valid_days += 1
                        if n >= 1:
                            hh += 1
                        if n > maks: maks = n
                    # 9999 / >887: abaikan

                monthly_jumlah[m] = round(total, 1) if total > 0 else 0
                monthly_maks[m]   = round(maks, 1)
                monthly_hh[m]     = hh
                # Persentase = valid_days / total_hari_bulan * 100
                total_days        = dim[m]
                monthly_pct[m]    = round(valid_days / total_days * 100, 2) if total_days > 0 else 0

            # ── Yearly summary ──
            yearly_jumlah = round(sum(monthly_jumlah.values()), 1)
            yearly_maks   = round(max((monthly_maks[m] for m in range(1,13)), default=0), 1)
            yearly_hh     = sum(monthly_hh.values())

            years_result.append({
                'tahun':        tahun,
                'days':         days_list,
                'days_in_month': dim,
                'dasarian': {
                    'das1': das1,
                    'das2': das2,
                    'das3': das3
                },
                'monthly': {
                    'jumlah': monthly_jumlah,
                    'maks':   monthly_maks,
                    'hh':     monthly_hh,
                    'pct':    monthly_pct
                },
                'yearly_summary': {
                    'jumlah': yearly_jumlah,
                    'maks':   yearly_maks,
                    'hh':     yearly_hh
                }
            })

        return jsonify({'years': years_result})

    except Exception as e:
        return jsonify({'error': str(e)})
    
# ==============================================================================

@app.route('/api/analisis/bulanan')
def analisis_bulanan():
    """
    Data CH bulanan per pos untuk rentang bulan dalam satu tahun.
    Params: kecamatan_id, tahun, bulan_awal, bulan_akhir
    Returns:
      {
        months: [ {bulan, total, maks, hh}, ... ],
        daily_by_month: { "1": [{hari, curah_hujan}, ...], "2": [...], ... }
      }
    """
    import calendar, math

    kecamatan_id = request.args.get('kecamatan_id')
    tahun        = request.args.get('tahun',      type=int)
    bulan_awal   = request.args.get('bulan_awal', type=int, default=1)
    bulan_akhir  = request.args.get('bulan_akhir',type=int, default=12)

    if not kecamatan_id:
        return jsonify({'error': 'kecamatan_id diperlukan'})
    if not tahun:
        return jsonify({'error': 'tahun diperlukan'})
    if bulan_akhir < bulan_awal:
        return jsonify({'error': 'bulan_akhir tidak boleh lebih kecil dari bulan_awal'})

    def is_valid(v):
        if v is None: return False
        try: n = float(v)
        except: return False
        if math.isnan(n): return False
        if n == 8888: return True          # trace = valid
        return 0 <= n <= 887

    def clean_val(v):
        """Return clean numeric for sum/max, None if invalid."""
        if v is None: return None
        try: n = float(v)
        except: return None
        if math.isnan(n): return None
        if n == 9999 or (n > 887 and n != 8888): return None
        if n == 8888: return 0.1
        return n

    try:
        with engine2.connect() as conn:
            q = text("""
                SELECT c.bulan, c.hari, c.curah_hujan
                FROM curah_hujan_harian c
                WHERE c.id_kecamatan = :kid
                  AND c.tahun = :thn
                  AND c.bulan BETWEEN :ba AND :bb
                ORDER BY c.bulan, c.hari
            """)
            df = pd.read_sql(q, conn, params={
                'kid': int(kecamatan_id),
                'thn': tahun,
                'ba':  bulan_awal,
                'bb':  bulan_akhir
            })

        months_result     = []
        daily_by_month    = {}

        for bulan in range(bulan_awal, bulan_akhir + 1):
            df_b = df[df['bulan'] == bulan]
            total = 0.0; maks = 0.0; hh = 0

            daily_list = []
            for _, row in df_b.iterrows():
                raw = row['curah_hujan']
                cv  = clean_val(raw)
                # store raw for frontend display
                daily_list.append({
                    'hari': int(row['hari']),
                    'curah_hujan': float(raw) if raw is not None and not math.isnan(float(raw) if raw is not None else float('nan')) else None
                })
                if cv is not None:
                    total += cv
                    if cv > maks: maks = cv
                    if cv >= 1:   hh  += 1

            daily_by_month[str(bulan)] = daily_list
            months_result.append({
                'bulan': bulan,
                'total': round(total, 1) if total > 0 else 0,
                'maks':  round(maks,  1) if maks  > 0 else 0,
                'hh':    hh
            })

        return jsonify({
            'months':         months_result,
            'daily_by_month': daily_by_month
        })

    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/analisis/pos-info')
def analisis_pos_info():
    """
    Ambil latitude & longitude satu pos (kecamatan) dari DB historis.
    Params: kecamatan_id
    Returns: {latitude, longitude}
    """
    kecamatan_id = request.args.get('kecamatan_id')
    if not kecamatan_id:
        return jsonify({'error': 'kecamatan_id diperlukan'})
    try:
        with engine2.connect() as conn:
            q = text("""
                SELECT latitude, longitude
                FROM kecamatan
                WHERE id_kecamatan = :kid
                LIMIT 1
            """)
            df = pd.read_sql(q, conn, params={'kid': int(kecamatan_id)})
        if len(df) == 0:
            return jsonify({'error': 'Pos tidak ditemukan'})
        row = df.iloc[0]
        return jsonify({
            'latitude': float(row['latitude']) if row['latitude'] is not None else None,
            'longitude':   float(row['longitude'])   if row['longitude']   is not None else None
        })
    except Exception as e:
        return jsonify({'error': str(e)})
if __name__ == '__main__':
    app.run(debug=True, port=5000)