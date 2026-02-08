# Panduan Deploy Wahaku di aaPanel

Panduan ini akan membantu Anda mengupload dan menjalankan aplikasi Wahaku di VPS yang menggunakan panel manajemen **aaPanel**.

## 1. Persiapan File
Anda bisa memilih dua metode: **Upload Manual** atau **Via Git (Recommended)**.

### Metode A: Via Git (Paling Mudah & Cepat)
Gunakan metode ini jika VPS Anda memiliki akses internet.

**Langkah 0: Setup SSH Key (Wajib jika Repo Private)**
Jika repository Anda **Private**, Anda harus menambahkan SSH Key VPS ke GitHub.
1. Copy Public Key dari VPS/aaPanel (biasanya diawali `ssh-ed25519` atau `ssh-rsa`).
2. Buka GitHub -> Repository Ini -> **Settings**.
3. Menu **Deploy keys** -> **Add deploy key**.
4. Paste key tersebut, beri judul "aaPanel VPS", dan centang "Allow write access" (opsional).
5. Klik **Add key**.

**Langkah 1: Clone & Setup**
1. **Buka Terminal aaPanel** (atau SSH ke server).
2. Masuk ke folder root web:
   ```bash
   cd /www/wwwroot/
   ```
3. Clone repository (Gunakan URL SSH jika sudah setup key):
   ```bash
   git clone git@github.com:udaBasrianto/wahakuGo.git wahaku
   ```
   *Atau jika public:* `git clone https://github.com/udaBasrianto/wahakuGo.git wahaku`

4. Masuk ke folder aplikasi:
   ```bash
   cd wahaku
   ```
5. **Jalankan Script Setup Otomatis**:
   Script ini akan otomatis memilih binary yang tepat (x64/ARM), mengatur permission, dan menyiapkan config.
   ```bash
   bash deploy/setup.sh
   ```
6. **Edit Config** (Jika perlu):
   Buka file `config.json` di File Manager aaPanel dan sesuaikan isinya.

### Metode B: Upload Manual
Di dalam folder `deploy` repository ini...
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

1. Di aaPanel, menu **Website** -> **Add Site** (tab **PHP Project**).
   - **Domain**: `bot.domainanda.com`
   - **PHP Version**: Pilih **Static** (Paling bawah/atas biasanya).
   - *Jangan pilih Node atau PHP versi lain, karena kita hanya butuh Nginx-nya saja.*
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
- **Git Error: Permission denied (.git/FETCH_HEAD)**
  Masalah ini terjadi karena folder dibuat oleh user lain (misal root/www), tapi Anda login sebagai user biasa (misal opc).
  Perbaiki permission dengan perintah:
  ```bash
  sudo chown -R $USER:$USER /www/wwwroot/chat.yaakhi.id
  ```
  *(Ganti path sesuai lokasi project Anda)*

- **Git Error: detected dubious ownership**
  Jika saat `git pull` muncul error ini, jalankan perintah berikut di terminal:
  ```bash
  git config --global --add safe.directory /www/wwwroot/wahaku
  ```
  *(Sesuaikan path folder jika berbeda)*

- **Aplikasi tidak jalan?**
  Cek log di Supervisor. Kemungkinan masalah permission atau config error.
- **Port 4500 sudah terpakai?**
  Ubah port di `config.json` menjadi port lain (misal 8080), lalu update juga setting Reverse Proxy.
- **Bot tidak merespon?**
  Pastikan server memiliki koneksi internet dan bisa mengakses API WhatsApp/Google.
