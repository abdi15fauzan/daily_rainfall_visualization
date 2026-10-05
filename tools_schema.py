# tools_schema.py — VERSI FINAL KONSOLIDASI (9 tools)

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_daftar_kabupaten",
            "description": "Ambil daftar semua kabupaten/kota di Kalimantan Timur beserta id_wilayah-nya. Hanya panggil kalau nama kabupaten tidak ada di daftar referensi yang sudah diberikan di system prompt.",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_daftar_pos",
            "description": "Ambil daftar pos/kecamatan dalam satu kabupaten beserta id_kecamatan-nya. Hanya panggil kalau nama pos tidak ada di daftar referensi yang sudah diberikan di system prompt.",
            "parameters": {
                "type": "object",
                "properties": {"wilayah_id": {"type": "integer"}},
                "required": ["wilayah_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_data_curah_hujan_bulanan",
            "description": "Ambil ringkasan curah hujan bulanan (total mm, maksimum harian, jumlah hari hujan) untuk satu pos pada rentang bulan tertentu di satu tahun. Hasilnya menyertakan field 'tahun' yang sudah benar — selalu pakai tahun dari hasil ini, jangan menebak sendiri.",
            "parameters": {
                "type": "object",
                "properties": {
                    "kecamatan_id": {"type": "integer"},
                    "tahun": {"type": "integer"},
                    "bulan_awal": {"type": "integer", "minimum": 1, "maximum": 12},
                    "bulan_akhir": {"type": "integer", "minimum": 1, "maximum": 12}
                },
                "required": ["kecamatan_id", "tahun"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_curah_hujan_harian",
            "description": "Ambil nilai curah hujan pada SATU tanggal spesifik untuk SATU pos. Pakai ini kalau user sebut tanggal tertentu untuk satu pos saja.",
            "parameters": {
                "type": "object",
                "properties": {
                    "kecamatan_id": {"type": "integer"},
                    "tahun": {"type": "integer"},
                    "bulan": {"type": "integer", "minimum": 1, "maximum": 12},
                    "hari": {"type": "integer", "minimum": 1, "maximum": 31}
                },
                "required": ["kecamatan_id", "tahun", "bulan", "hari"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_curah_hujan_harian_wilayah",
            "description": "Ambil nilai curah hujan pada SATU tanggal spesifik untuk SEMUA pos dalam satu kabupaten/kota sekaligus (daftar per pos, BUKAN dirata-ratakan). Pakai ini kalau user minta 'semua/seluruh data curah hujan di [kabupaten/kota]' pada tanggal tertentu. Kosongkan wilayah_id (jangan disertakan) kalau user minta 'seluruh Kalimantan Timur'/'seluruh Kaltim' tanpa sebut kabupaten tertentu.",
            "parameters": {
                "type": "object",
                "properties": {
                    "wilayah_id": {"type": "integer", "description": "Kosongkan/jangan sertakan untuk mengambil SELURUH pos se-Kaltim."},
                    "tahun": {"type": "integer"},
                    "bulan": {"type": "integer", "minimum": 1, "maximum": 12},
                    "hari": {"type": "integer", "minimum": 1, "maximum": 31}
                },
                "required": ["tahun", "bulan", "hari"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_statistik_curah_hujan",
            "description": "Hitung statistik (rata_rata, total, maksimum, minimum, modus, std_dev, kurtosis) curah hujan untuk SATU pos pada rentang tanggal bebas (hari/dasarian/bulan/tahun/rentang apa pun). rata_rata = total nilai valid \u00f7 jumlah hari data valid (kode 9999 dikecualikan dari pembagi, kode 8888 dihitung sebagai 0.0). WAJIB pakai tool ini untuk pertanyaan statistik atau akumulasi/total — JANGAN pernah menghitung atau menjumlahkan sendiri, kamu sering salah hitung.",
            "parameters": {
                "type": "object",
                "properties": {
                    "kecamatan_id": {"type": "integer"},
                    "tanggal_awal": {"type": "string", "description": "Format YYYY-MM-DD"},
                    "tanggal_akhir": {"type": "string", "description": "Format YYYY-MM-DD"},
                    "metrik": {"type": "string", "enum": ["rata_rata", "total", "maksimum", "minimum", "modus", "std_dev", "kurtosis"]}
                },
                "required": ["kecamatan_id", "tanggal_awal", "tanggal_akhir", "metrik"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_statistik_wilayah",
            "description": "Hitung statistik gabungan lintas-pos. Kalau wilayah_id diisi: hitung metrik tiap pos dalam kabupaten itu, lalu rata-ratakan antar pos (bobot sama per pos). Kalau wilayah_id dikosongkan (seluruh Kaltim): hitung metrik tiap pos, rata-ratakan per kabupaten, lalu rata-ratakan lagi antar kabupaten (bobot sama per kabupaten, bukan per pos). Pakai untuk pertanyaan level kabupaten/kota/provinsi (satu angka gabungan), BUKAN untuk satu pos spesifik dan BUKAN untuk daftar per-pos (pakai get_curah_hujan_harian_wilayah untuk itu).",
            "parameters": {
                "type": "object",
                "properties": {
                    "wilayah_id": {"type": "integer", "description": "Kosongkan/jangan sertakan untuk statistik gabungan SELURUH pos se-Kaltim."},
                    "tanggal_awal": {"type": "string", "description": "Format YYYY-MM-DD"},
                    "tanggal_akhir": {"type": "string", "description": "Format YYYY-MM-DD"},
                    "metrik": {"type": "string", "enum": ["rata_rata", "total", "maksimum", "minimum", "modus", "std_dev", "kurtosis"]}
                },
                "required": ["tanggal_awal", "tanggal_akhir", "metrik"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_akumulasi_hari_hujan",
            "description": "Hitung total hari hujan DAN hari tidak hujan untuk satu pos pada rentang bulan tertentu di satu tahun. WAJIB pakai ini untuk pertanyaan hari hujan/tidak hujan lintas-bulan — JANGAN jumlahkan sendiri.",
            "parameters": {
                "type": "object",
                "properties": {
                    "kecamatan_id": {"type": "integer"},
                    "tahun": {"type": "integer"},
                    "bulan_awal": {"type": "integer", "minimum": 1, "maximum": 12},
                    "bulan_akhir": {"type": "integer", "minimum": 1, "maximum": 12}
                },
                "required": ["kecamatan_id", "tahun", "bulan_awal", "bulan_akhir"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_perbandingan_pos_wilayah",
            "description": "Bandingkan rata-rata curah hujan SATU pos dengan rata-rata wilayahnya (kabupaten tempat pos itu berada) pada periode tertentu, termasuk status di atas/di bawah rata-rata wilayah. Pakai kalau user minta perbandingan satu pos dengan wilayahnya.",
            "parameters": {
                "type": "object",
                "properties": {
                    "kecamatan_id": {"type": "integer"},
                    "tanggal_awal": {"type": "string", "description": "Format YYYY-MM-DD"},
                    "tanggal_akhir": {"type": "string", "description": "Format YYYY-MM-DD"}
                },
                "required": ["kecamatan_id", "tanggal_awal", "tanggal_akhir"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_dasarian_wilayah",
            "description": "Ambil akumulasi dasarian (DAS1/DAS2/DAS3) untuk SEMUA pos dalam satu kabupaten/kota sekaligus, dalam satu bulan. Pakai kalau user minta dasarian untuk seluruh/semua pos di satu kabupaten, bukan satu pos saja (kalau satu pos, pakai get_data_dasarian).",
            "parameters": {
                "type": "object",
                "properties": {
                    "wilayah_id": {"type": "integer"},
                    "tahun": {"type": "integer"},
                    "bulan": {"type": "integer", "minimum": 1, "maximum": 12},
                    "dasarian": {"type": "integer", "enum": [1, 2, 3]}
                },
                "required": ["wilayah_id", "tahun", "bulan", "dasarian"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_data_dasarian",
            "description": "Ambil ringkasan curah hujan untuk satu dasarian (DAS1=tanggal 1-10, DAS2=tanggal 11-20, DAS3=tanggal 21-akhir bulan) dalam satu bulan untuk satu pos. Pakai ini kalau user sebut istilah 'dasarian', 'DAS1', 'DAS2', atau 'DAS3'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "kecamatan_id": {"type": "integer"},
                    "tahun": {"type": "integer"},
                    "bulan": {"type": "integer", "minimum": 1, "maximum": 12},
                    "dasarian": {"type": "integer", "enum": [1, 2, 3]}
                },
                "required": ["kecamatan_id", "tahun", "bulan", "dasarian"]
            }
        }
    }
]