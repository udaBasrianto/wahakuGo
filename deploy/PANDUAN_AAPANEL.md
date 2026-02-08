# Panduan Deploy Wahaku di aaPanel

Panduan ini akan membantu Anda mengupload dan menjalankan aplikasi Wahaku di VPS yang menggunakan panel manajemen **aaPanel**.

## 1. Persiapan File
Di dalam folder `deploy` repository ini, sudah tersedia file-file yang dibutuhkan:
- `wahaku-linux`: Binary untuk VPS biasa (Intel/AMD x64).
- `wahaku-linux-arm64`: Binary untuk VPS ARM (Oracle ARM, AWS Graviton, dll).
- `wahaku.service`: File konfigurasi service untuk menjalankan aplikasi di background.
- `config.json`: Template konfigurasi.
- `views/`: Folder tampilan web.

## 2. Upload File ke VPS
1. Login ke **aaPanel**.
2. Buka menu **Files**.
3. Masuk ke direktori `/www/wwwroot/`.
4. Buat folder baru, misalnya `wahaku`.
5. Upload file-file berikut dari komputer Anda ke folder `/www/wwwroot/wahaku/`:
   - **Pilih salah satu binary sesuai arsitektur VPS Anda**:
     - Jika pakai Intel/AMD: Upload `wahaku-linux`.
     - Jika pakai ARM (arch64): Upload `wahaku-linux-arm64`.
   - **PENTING**: Rename file yang diupload tadi menjadi `wahaku` agar command-nya konsisten.
   - `config.json` (Edit isinya sesuai kebutuhan, terutama port dan API Key).
   - Folder `views` (Upload seluruh folder beserta isinya).
   - Folder `uploads` (Buat folder kosong ini jika belum ada).

   **Struktur Akhir Folder `/www/wwwroot/wahaku/`:**
   ```
   /www/wwwroot/wahaku/
   ├── wahaku          (File binary executable)
   ├── config.json     (File konfigurasi)
   ├── views/          (Folder template HTML)
   ├── uploads/        (Folder penyimpanan gambar)
   └── wahaku.db       (Akan otomatis dibuat saat aplikasi jalan)
   ```

## 3. Atur Permission
1. Di File Manager aaPanel, klik kanan pada file `wahaku` (binary yang tadi diupload).
2. Pilih **Permission**.
3. Set permission ke **755** (Owner: Read, Write, Execute).
4. Klik **Ok**.

## 4. Konfigurasi Service (Supervisor)
Cara paling mudah menjalankan aplikasi Go di aaPanel adalah menggunakan **Supervisor**.

1. Buka **App Store** di aaPanel.
2. Cari dan install **Supervisor** (biasanya bernama "Daemon process manager").
3. Buka setting Supervisor, klik **Add Daemon**.
   - **Name**: `wahaku`
   - **Run User**: `root`
   - **Run Dir**: `/www/wwwroot/wahaku/`
   - **Start Command**: `/www/wwwroot/wahaku/wahaku`
   - **Processes**: `1`
4. Klik **Confirm/Add**.
5. Pastikan statusnya **Running** (Hijau). Jika merah, cek log errornya.

## 5. Setting Reverse Proxy (Agar bisa diakses via Domain)
Jika Anda ingin mengakses dashboard via domain (misal: `bot.domainanda.com`) tanpa mengetik port:

1. Di aaPanel, menu **Website** -> **Add Site**.
   - **Domain**: `bot.domainanda.com`
   - **PHP Version**: `Static` (atau Pure Static).
2. Setelah website dibuat, klik nama websitenya untuk membuka pengaturan.
3. Masuk ke menu **Reverse Proxy**.
4. Klik **Add Reverse Proxy**.
   - **Name**: `wahaku-app`
   - **Target URL**: `http://127.0.0.1:4500` (Sesuaikan port dengan `config.json`).
   - **Sent Domain**: `$host`
5. Klik **Submit**.

## 6. Selesai!
Sekarang Anda bisa mengakses dashboard Wahaku melalui domain Anda.
- Buka `http://bot.domainanda.com`
- Login dengan username/password yang ada di `config.json` (Default: `admin` / `password`).
- Scan QR Code WhatsApp dan mulai gunakan botnya.

---

### Troubleshooting
- **Aplikasi tidak jalan?**
  Cek log di Supervisor. Kemungkinan masalah permission atau config error.
- **Port 4500 sudah terpakai?**
  Ubah port di `config.json` menjadi port lain (misal 8080), lalu update juga setting Reverse Proxy.
- **Bot tidak merespon?**
  Pastikan server memiliki koneksi internet dan bisa mengakses API WhatsApp/Google.
