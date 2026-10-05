import os
from groq import Groq
from dotenv import load_dotenv

# 1. Membaca file .env untuk mengambil API Key secara aman
load_dotenv()

# 2. Mengambil API Key dari environment variable
api_key = os.environ.get("GROQ_API_KEY")

if not api_key:
    print("❌ ERROR: GROQ_API_KEY tidak ditemukan di file .env Anda!")
    print("Pastikan Anda sudah membuat file .env dan mengisinya dengan: GROQ_API_KEY=gsk_xxxx")
    exit()

# 3. Inisialisasi client Groq menggunakan API Key Anda
client = Groq(api_key=api_key)

try:
    print("⏳ Sedang mengirim pertanyaan ke Groq Cloud menggunakan model gpt-oss-20b...")
    
    # 4. Melakukan pemanggilan ke API Groq
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": "Anda adalah asisten AI ramah yang ahli dalam analisis data curah hujan."
            },
            {
                "role": "user",
                "content": "Halo! Apakah kamu sudah aktif? Jawab dengan menyapa dan perkenalkan dirimu secara singkat dalam Bahasa Indonesia.",
            }
        ],
        model="openai/gpt-oss-20b", # Menggunakan model pilihan Anda yang super cepat
        temperature=0.7,
        max_completion_tokens=150   # Batasan panjang teks balasan biar hemat token
    )

    # 5. Menampilkan hasil balasan dari AI ke terminal
    print("\n✅ KONEKSI BERHASIL!")
    print("--- Balasan dari AI Groq ---")
    print(chat_completion.choices[0].message.content)
    print("----------------------------")

except Exception as e:
    print(f"\n❌ Terjadi error saat memanggil API: {e}")
    print("Solusi: Periksa kembali apakah text API Key di file .env sudah benar (tanpa tanda kutip) atau kuota harian Anda habis.")
