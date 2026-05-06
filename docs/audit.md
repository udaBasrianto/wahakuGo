# Audit Kode Aplikasi Wahaku

Berdasarkan audit mendalam terhadap kode aplikasi Wahaku (aplikasi bot WhatsApp multi-user dengan web dashboard), berikut adalah ringkasan temuan keamanan, kualitas kode, dan status perbaikan.

**Build Version**: 1.3.0-MultiTenant | **Audit Date**: 2026-05-06

---

## **Ringkasan Aplikasi**
- **Bahasa**: Go (Fiber web framework)
- **Fitur Utama**: Bot WhatsApp AI, multi-user, multi-tenant, dashboard web, integrasi AI providers (Gemini, OpenAI, BytePlus, dll.), database (SQLite/PostgreSQL/MySQL), Google Sheets
- **Keamanan Dasar**: Password hashing (bcrypt), CSRF protection, rate limiting, session management, account lockout

---

## **Temuan Keamanan**

### **Positif (Good Practices)**
- ✅ **Password Security**: Menggunakan bcrypt untuk hashing password, dengan migrasi otomatis dari plaintext ke hash.
- ✅ **Session Management**: Cookie HTTPOnly, Secure (di production), SameSite Lax, expiration 24 jam.
- ✅ **CSRF Protection**: Middleware CSRF dengan token header, cookie non-readable by JS.
- ✅ **Rate Limiting**: 5 request per menit per IP untuk auth endpoints.
- ✅ **Account Lockout**: Akun dikunci 15 menit setelah 5 gagal login berturut-turut. *(BARU)*
- ✅ **OTP Resend Cooldown**: Per-user cooldown 60 detik untuk resend OTP. *(BARU)*
- ✅ **Input Validation**: Validasi panjang username/password, format email (strict), sanitasi filename upload.
- ✅ **Password Complexity**: Minimal 8 karakter, 1 huruf kapital, 1 angka. *(BARU)*
- ✅ **Email Validation**: Validasi format email yang lebih ketat (bukan hanya cek `@`). *(BARU)*
- ✅ **Prepared Statements**: SQL queries menggunakan parameterized queries (mencegah SQL injection).
- ✅ **Environment Variables**: Overlay API keys dari env variables untuk keamanan.
- ✅ **File Upload Security**: Validasi ekstensi, ukuran max 10MB, filename sanitization dengan random suffix.
- ✅ **OTP Security**: OTP 6-digit cryptographically secure, expiry 5 menit, rate limited resend.
- ✅ **SSRF Protection**: `validateOutboundBaseURL()` memblokir private IP, localhost, non-HTTPS.
- ✅ **Path Traversal Protection**: Blokir `..` di URL, blokir akses langsung ke `config.json`, `.env`, `wahaku.db`.
- ✅ **Audit Logging**: Log terstruktur untuk semua auth events dan admin actions. *(BARU)*
- ✅ **Body Limit**: Diturunkan dari 50MB ke 10MB untuk mencegah DoS. *(BARU)*
- ✅ **OTP Bypass Fix**: Registrasi tidak lagi mengaktifkan akun otomatis saat bot offline. *(BARU)*

---

## **Risiko yang Sudah Diperbaiki**

| # | Temuan | Severity | Status |
|---|--------|----------|--------|
| 1 | OTP bypass saat bot offline (register langsung aktif) | 🔴 High | ✅ Fixed |
| 2 | Tidak ada account lockout setelah gagal login | 🔴 High | ✅ Fixed |
| 3 | OTP resend tidak ada cooldown per user | 🔴 High | ✅ Fixed |
| 4 | Password policy lemah (hanya panjang minimal) | 🟡 Medium | ✅ Fixed |
| 5 | Email validation terlalu lemah (`@` saja) | 🟡 Medium | ✅ Fixed |
| 6 | Body limit 50MB (DoS vector) | 🟡 Medium | ✅ Fixed |
| 7 | Tidak ada audit logging | 🟡 Medium | ✅ Fixed |

---

## **Risiko yang Masih Ada**

### **Medium Risk**
- ⚠️ **XSS Potential**: Penggunaan `innerHTML` di frontend (dashboard) untuk render data dinamis. Pastikan escape jika ada user input yang di-render.
- ⚠️ **Error Messages**: Beberapa error expose internal details (e.g., "Database Error"). Sebaiknya gunakan pesan generik.
- ⚠️ **No HTTPS Enforcement**: Di production, pastikan HTTPS dan redirect HTTP ke HTTPS.

### **Low Risk**
- ⚠️ **Dependency Vulnerabilities**: Go modules perlu diperiksa untuk CVEs (gunakan `govulncheck` atau `trivy`).
- ⚠️ **Chat History In-Memory**: `chatHistories` tidak di-persist ke database — restart = history hilang.
- ⚠️ **No Health Check Endpoint**: Tidak ada `/health` endpoint untuk monitoring.

### **Code Quality**
- ⚠️ `main.go` sangat panjang (~5000+ baris) — perlu dipecah ke package terpisah (`handlers/`, `models/`, `middleware/`, `services/`).
- ⚠️ Tidak ada unit tests untuk fungsi kritis.
- ⚠️ Logging inconsistent (beberapa log ke stdout, beberapa tidak).

---

## **Detail Perbaikan yang Dilakukan**

### 1. Account Lockout (`main.go`)
```go
// Setelah 5 gagal login berturut-turut, akun dikunci 15 menit
const loginMaxFailures  = 5
const loginLockDuration = 15 * time.Minute
```
- `recordLoginFailure(tenantID, username)` — dipanggil setiap password salah
- `resetLoginFailures(tenantID, username)` — dipanggil setelah login berhasil
- `checkAccountLocked(tenantID, username)` — dicek sebelum verifikasi password

### 2. OTP Resend Cooldown (`main.go`)
```go
const otpResendCooldown = 60 * time.Second
```
- `checkOTPResendCooldown(userID)` — dicek sebelum generate OTP baru
- `recordOTPResend(userID)` — dipanggil setelah OTP berhasil dikirim

### 3. Password Complexity (`main.go`)
```go
func validatePasswordComplexity(password string) string
// Minimal 8 karakter, 1 huruf kapital, 1 angka
```
Diterapkan di: register, profile update.

### 4. Email Validation (`main.go`)
```go
func validateEmail(email string) bool
// Cek panjang, posisi @, domain, TLD
```
Diterapkan di: register, profile update.

### 5. OTP Bypass Fix (`main.go`)
Sebelum:
```go
// Bot offline → akun langsung aktif (BERBAHAYA)
return c.JSON(fiber.Map{"success": true, "require_otp": false, ...})
```
Sesudah:
```go
// Bot offline → akun tetap inactive, menunggu persetujuan admin
return c.JSON(fiber.Map{"success": true, "pending_approval": true, ...})
```

### 6. Audit Logging (`main.go`)
```go
func logAudit(event string, userID, tenantID int, ip, detail string)
```
Events yang di-log:
- `LOGIN_SUCCESS` / `LOGIN_FAILED`
- `OTP_VERIFY_SUCCESS` / `OTP_VERIFY_FAILED`
- `REGISTER_SUCCESS`
- `LOGOUT`
- `ADMIN_USER_CREATE` / `ADMIN_USER_DELETE`
- `ADMIN_CONFIG_UPDATE`

### 7. Body Limit (`main.go`)
```go
// Sebelum: 50MB
// Sesudah: 10MB
BodyLimit: 10 * 1024 * 1024,
```

---

## **Rekomendasi Selanjutnya**

1. **Scan dependencies**: Jalankan `govulncheck ./...` secara berkala.
2. **Escape output di frontend**: Ganti `innerHTML` dengan `textContent` atau gunakan DOMPurify.
3. **Enforce HTTPS**: Tambahkan redirect HTTP → HTTPS di production (via reverse proxy atau middleware).
4. **Persist chat history**: Simpan ke database agar tidak hilang saat restart.
5. **Refactor main.go**: Pecah ke package terpisah untuk maintainability.
6. **Tambah unit tests**: Minimal untuk `hashPassword`, `validatePasswordComplexity`, `validateEmail`, `validateOutboundBaseURL`.
7. **Health check endpoint**: Tambahkan `GET /health` untuk monitoring.
