# Wahaku - WhatsApp AI Assistant

Wahaku adalah asisten AI canggih berbasis WhatsApp yang dibangun menggunakan **Go (Golang)** dan library **whatsmeow**. Aplikasi ini dirancang untuk mengotomatisasi interaksi WhatsApp dengan memanfaatkan kekuatan berbagai model AI terkemuka.

## 🚀 Fitur Utama

- **Multi-Provider AI Support**:
  - **Google Gemini**: Integrasi native dengan model terbaru (Gemini 1.5 Pro, Flash, dll).
  - **Google Vertex AI**: Dukungan enterprise dengan Service Account dan pemilihan region.
  - **OpenAI**: Kompatibilitas dengan GPT-3.5, GPT-4, dan GPT-4o.
  - **Groq**: Inferensi ultra-cepat dengan model Llama 3 dan Mixtral.
  - **Qwen (Alibaba Cloud)**: Model bahasa besar dari Alibaba.
  - **BytePlus (Doubao)**: Integrasi dengan model AI dari ByteDance.

- **Integrasi WhatsApp yang Kuat**:
  - Login via QR Code (Scanning langsung dari aplikasi WhatsApp).
  - Mendukung pesan teks, gambar, dan dokumen.
  - **Broadcast**: Kirim pesan massal ke banyak kontak sekaligus.
  - **Status Filter**: Otomatis mengabaikan update status untuk mencegah spam log.

- **Knowledge Base Dinamis**:
  - **Google Sheets Integration**: Gunakan spreadsheet sebagai sumber pengetahuan yang mudah diedit.
  - **Context Awareness**: Mengingat konteks percakapan (Chat History).

- **Manajemen Pengguna & Dashboard**:
  - **Multi-User System**: Mendukung pendaftaran user baru dengan persetujuan Admin.
  - **Modern Dashboard**: Antarmuka web responsif (Tailwind CSS) untuk memantau koneksi, log, dan pengaturan.
  - **Role-Based Access**: Admin dapat mengelola user lain.

- **Fitur Tambahan**:
  - **Follow-up Scheduler**: Penjadwalan pesan otomatis untuk tindak lanjut.
  - **Secure Configuration**: Penyimpanan kredensial yang aman.

## 🛠️ Prasyarat Teknologi

- **Go**: Versi 1.21 atau lebih baru.
- **Git**: Untuk manajemen versi.
- **Database**: SQLite (bawaan) atau PostgreSQL (opsional).

## 📦 Instalasi & Menjalankan

1. **Clone Repository**
   ```bash
   git clone https://github.com/udaBasrianto/wahakuGo.git
   cd wahakuGo/wa-server/go-app
   ```

2. **Install Dependencies**
   ```bash
   go mod tidy
   ```

3. **Jalankan Aplikasi**
   ```bash
   go run main.go
   ```
   Aplikasi akan berjalan pada port default `4500`.

4. **Akses Dashboard**
   Buka browser dan kunjungi:
   `http://localhost:4500`

## ⚙️ Konfigurasi

Saat pertama kali dijalankan, file `config.json` akan dibuat secara otomatis. Anda dapat mengonfigurasi:
- **Server Port**: Port aplikasi web.
- **Admin Credentials**: Username dan password untuk login dashboard.
- **AI Providers**: API Key dan pengaturan model untuk setiap provider AI.

## 📂 Struktur Proyek

- `main.go`: Logika utama backend server dan integrasi WA.
- `views/`: Template HTML untuk antarmuka pengguna (Dashboard, Login, Register).
- `uploads/`: Direktori penyimpanan sementara untuk file media.
- `wahaku.db`: Database SQLite lokal untuk data user dan sesi.

## 🤝 Kontribusi

Kontribusi sangat diterima! Silakan buat *Pull Request* atau laporkan *Issue* jika menemukan bug atau memiliki ide fitur baru.

## 📄 Lisensi

Project ini dilisensikan di bawah [MIT License](LICENSE).
