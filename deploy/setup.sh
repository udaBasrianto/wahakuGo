#!/bin/bash

# Script Setup Otomatis Wahaku (aaPanel/Linux)
# Cara pakai: bash deploy/setup.sh

echo "========================================="
echo "   WAHAKU AI ASSISTANT - AUTO SETUP      "
echo "========================================="

# 1. Deteksi Arsitektur CPU
ARCH=$(uname -m)
echo "[INFO] Deteksi Arsitektur Sistem: $ARCH"

if [[ "$ARCH" == "aarch64" ]]; then
    echo "[INFO] Menggunakan binary untuk ARM64..."
    if [ -f "deploy/wahaku-linux-arm64" ]; then
        cp deploy/wahaku-linux-arm64 wahaku
    else
        echo "[ERROR] File deploy/wahaku-linux-arm64 tidak ditemukan!"
        exit 1
    fi
elif [[ "$ARCH" == "x86_64" ]]; then
    echo "[INFO] Menggunakan binary untuk x64 (Intel/AMD)..."
    if [ -f "deploy/wahaku-linux" ]; then
        cp deploy/wahaku-linux wahaku
    else
        echo "[ERROR] File deploy/wahaku-linux tidak ditemukan!"
        exit 1
    fi
else
    echo "[WARN] Arsitektur tidak dikenali otomatis. Silakan copy binary secara manual."
fi

# 2. Set Permission Eksekusi
if [ -f "wahaku" ]; then
    chmod +x wahaku
    echo "[OK] Permission eksekusi diberikan ke file 'wahaku'."
else
    echo "[ERROR] Gagal menyiapkan file binary 'wahaku'."
    exit 1
fi

# 3. Cek Konfigurasi
if [ ! -f "config.json" ]; then
    echo "[INFO] File config.json belum ada. Membuat dari template..."
    cp deploy/config.json config.json
    echo "[OK] config.json dibuat. JANGAN LUPA EDIT isinya nanti!"
else
    echo "[INFO] config.json sudah ada. Melewati langkah ini (tidak ditimpa)."
fi

# 4. Buat Folder Uploads
if [ ! -d "uploads" ]; then
    mkdir -p uploads
    echo "[OK] Folder 'uploads' dibuat."
fi

# 5. Informasi Path untuk Supervisor
CURRENT_DIR=$(pwd)
echo "========================================="
echo " SETUP SELESAI!"
echo "========================================="
echo "Gunakan informasi berikut untuk setting Supervisor di aaPanel:"
echo ""
echo "Run Dir       : $CURRENT_DIR"
echo "Start Command : $CURRENT_DIR/wahaku"
echo ""
echo "========================================="
