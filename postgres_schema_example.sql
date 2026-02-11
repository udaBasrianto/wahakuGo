-- Contoh Skema Database untuk Akhimedia (PostgreSQL)
-- Silakan jalankan script ini di Query Tab HeidiSQL setelah terhubung ke database 'wahaku_db'

-- Tabel Layanan / Produk
CREATE TABLE IF NOT EXISTS services (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    price DECIMAL(15, 2),
    is_active BOOLEAN DEFAULT TRUE
);

-- Data Dummy Layanan
INSERT INTO services (name, description, price) VALUES 
('Website Company Profile', 'Website profesional untuk profil bisnis, include domain & hosting 1 tahun.', 1500000),
('Web Toko Online', 'Website e-commerce dengan fitur keranjang belanja dan pembayaran otomatis.', 3500000),
('Sistem Informasi Custom', 'Aplikasi web khusus sesuai alur bisnis perusahaan (ERP, CRM, dll).', 5000000),
('Jasa SEO & Digital Marketing', 'Optimasi website agar muncul di halaman 1 Google dan manajemen iklan.', 1000000);

-- Tabel Portfolio
CREATE TABLE IF NOT EXISTS portfolios (
    id SERIAL PRIMARY KEY,
    project_name VARCHAR(150),
    client_name VARCHAR(100),
    year_completed INTEGER,
    description TEXT
);

-- Data Dummy Portfolio
INSERT INTO portfolios (project_name, client_name, year_completed, description) VALUES 
('Sistem Akademik Kampus', 'Universitas ABC', 2024, 'Sistem manajemen data mahasiswa dan nilai.'),
('E-Katalog UMKM', 'Dinas Koperasi', 2023, 'Platform marketplace untuk UMKM lokal.');
