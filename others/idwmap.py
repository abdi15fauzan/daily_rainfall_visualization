import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point
from matplotlib.colors import ListedColormap, BoundaryNorm
import matplotlib.patches as mpatches
from scipy.spatial.distance import cdist

# ==========================================
# 1. KONFIGURASI
# ==========================================
csv_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTfOhV4FOzPfnrQYG7mIpR0ejzzgUvjc3c0MyIkzHU-aFmewiQuv-zczJB0rwO-T0oUF7aY5CXJVLOY/pub?gid=1029389640&single=true&output=csv"
# Path GeoJSON (sesuai path komputer Anda)
geojson_path = r"C:\Users\Abdi Fatah Fauzan\Documents\projekwebgis\Project_stage1\WilayahKaltim1.geojson" 
output_image = "hasil_peta_sebaran_CH_Harian.png"

# Pilih kolom data
col_target = 'Curah Hujan' 

# ==========================================
# 2. LOAD & BERSIHKAN DATA
# ==========================================
print("Membaca data...")
try:
    df = pd.read_csv(csv_url)
except Exception as e:
    print(f"Gagal mengunduh CSV: {e}")
    exit()

df.columns = df.columns.str.strip()

# Deteksi kolom koordinat
if 'X' in df.columns and 'Y' in df.columns:
    col_lat, col_lon = 'X', 'Y'
elif 'Lintang' in df.columns and 'Bujur' in df.columns:
    col_lat, col_lon = 'Lintang', 'Bujur'
else:
    print("ERROR: Kolom koordinat tidak ditemukan.")
    exit()

# Konversi numerik
for col in [col_lat, col_lon, col_target]:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# --- LOGIKA KHUSUS (8888 & 9999) ---
print("Menerapkan koreksi data (8888 -> 0.1)...")
# Ubah 8888 menjadi 0.1 (Hujan ringan/tak terukur)
df.loc[df[col_target] == 8888, col_target] = 0.1
# -----------------------------------

# Filter Data Valid
# (Nilai 9999 otomatis terbuang karena > 5000)
df_clean = df[
    (df[col_target] < 5000) & 
    (df[col_lat].notna()) & 
    (df[col_lon].notna())
].copy()

print(f"Data valid untuk interpolasi: {len(df_clean)} titik.")

# ==========================================
# 3. PROSES GRID & INTERPOLASI IDW
# ==========================================
lat_data = df_clean[col_lat].values
lon_data = df_clean[col_lon].values
z_data = df_clean[col_target].values

# Load Batas Wilayah
try:
    gdf_boundary = gpd.read_file(geojson_path)
    min_lon, min_lat, max_lon, max_lat = gdf_boundary.total_bounds
except Exception as e:
    print(f"Peringatan: GeoJSON error ({e}). Menggunakan batas data.")
    gdf_boundary = None
    min_lon, max_lon = lon_data.min() - 0.1, lon_data.max() + 0.1
    min_lat, max_lat = lat_data.min() - 0.1, lat_data.max() + 0.1

# BUAT GRID (Resolusi Tinggi 1000x1000)
grid_x = np.linspace(min_lon, max_lon, 1000) 
grid_y = np.linspace(min_lat, max_lat, 1000)
xi, yi = np.meshgrid(grid_x, grid_y)

# --- BAGIAN UTAMA: SET POWER = 7 ---
power = 7 
print(f"Sedang memproses Interpolasi dengan Power {power}...")
# -----------------------------------

coords_station = np.vstack((lon_data, lat_data)).T
coords_grid = np.vstack((xi.flatten(), yi.flatten())).T

# Hitung Jarak (Vectorized)
dists = cdist(coords_grid, coords_station)
dists = np.where(dists == 0, 1e-10, dists)

# Rumus IDW
weights = 1.0 / (dists ** power)
sum_weights = np.sum(weights, axis=1)
zi_flat = np.sum(weights * z_data, axis=1) / sum_weights
zi = zi_flat.reshape(xi.shape)

# ==========================================
# 4. CLIPPING (MASKING) - FIX WARNING
# ==========================================
if gdf_boundary is not None:
    from matplotlib.path import Path
    
    # Perbaikan: Menggunakan union_all() agar tidak ada warning merah
    if hasattr(gdf_boundary.geometry, 'union_all'):
        boundary_poly = gdf_boundary.geometry.union_all()
    else:
        boundary_poly = gdf_boundary.geometry.unary_union
    
    if boundary_poly.geom_type == 'MultiPolygon':
        polys = list(boundary_poly.geoms)
    else:
        polys = [boundary_poly]
    
    mask = np.zeros(len(coords_grid), dtype=bool)
    
    for poly in polys:
        path = Path(np.array(poly.exterior.coords))
        mask = mask | path.contains_points(coords_grid)
    
    zi_flat = zi.flatten()
    zi_flat[~mask] = np.nan
    zi = zi_flat.reshape(xi.shape)

# ==========================================
# 5. VISUALISASI
# ==========================================
fig, ax = plt.subplots(figsize=(12, 12))

# Warna sesuai Legenda ArcGIS
colors = ['#B0B0B0', '#00FF00', '#FFFF00', '#FF7F00', '#FF0000', '#9400D3']
bounds = [0, 0.5, 20, 50, 100, 150, 500] 

# Colormap Diskrit
cmap_discrete = ListedColormap(colors)
norm = BoundaryNorm(bounds, cmap_discrete.N)

# Plotting
im = ax.imshow(zi, extent=(min_lon, max_lon, min_lat, max_lat), 
               origin='lower', cmap=cmap_discrete, norm=norm, alpha=1.0)

# Batas Wilayah
if gdf_boundary is not None:
    gdf_boundary.plot(ax=ax, facecolor='none', edgecolor='black', linewidth=0.8)

# --- KONFIGURASI HANYA PETA ---
# Menghapus sumbu (axis), kotak (box), dan label
ax.axis('off')

# Menghilangkan margin di sekitar peta
plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
ax.margins(0,0)

# Simpan dengan transparent=True dan pad_inches=0 agar tidak ada ruang putih berlebih
plt.savefig(output_image, dpi=300, bbox_inches='tight', pad_inches=0, transparent=True)
print(f"Selesai! Gambar disimpan: {output_image}")
plt.show()