package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"database/sql"

	"golang.org/x/crypto/bcrypt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/csrf"
	"github.com/gofiber/fiber/v2/middleware/session"
	_ "github.com/lib/pq" // PostgreSQL Driver
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// --- CONFIG ---
type ProviderConfig struct {
	APIKey  string `json:"api_key"`
	Model   string `json:"model"`
	BaseURL string `json:"base_url"`
}

type ModelDetail struct {
	ID            string `json:"id"`
	Provider      string `json:"provider,omitempty"`
	Status        string `json:"status,omitempty"`
	ContextWindow string `json:"context_window,omitempty"`
	InputCost     string `json:"input_cost,omitempty"`
	OutputCost    string `json:"output_cost,omitempty"`
}

type DBConfig struct {
	Enabled  bool   `json:"enabled"` // Added field
	Type     string `json:"type"`    // "mysql" or "postgres"
	Host     string `json:"host"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Name     string `json:"name"`
}

type SheetConfig struct {
	SpreadsheetID   string `json:"spreadsheet_id"`
	CredentialsJSON string `json:"credentials_json"`
}

type Config struct {
	ActiveProvider string                    `json:"active_provider"`
	AdminUsername  string                    `json:"admin_username"`
	AdminPassword  string                    `json:"admin_password"`
	SystemPrompt   string                    `json:"system_prompt"`
	SavedPrompts   map[string]string         `json:"saved_prompts"`
	KnowledgeURLs  []string                  `json:"knowledge_urls"`
	KnowledgeFiles []string                  `json:"knowledge_files"`
	Providers      map[string]ProviderConfig `json:"providers"`
	Database       DBConfig                  `json:"database"`
	Sheet          SheetConfig               `json:"sheet"`
	AppPort        string                    `json:"app_port"`
}

var (
	// MULTI-USER CLIENTS
	// Map: UserID -> DeviceJID -> *whatsmeow.Client
	userClients = make(map[int]map[string]*whatsmeow.Client)
	// Map: UserID -> DeviceJID -> QRCode
	userQRCodes = make(map[int]map[string]string)
	// Map: UserID -> DeviceJID -> Status
	userStatuses = make(map[int]map[string]string)
	clientMutex  sync.Mutex

	cfg           Config
	configFile    = "config.json"
	container     *sqlstore.Container
	mu            sync.Mutex                  // Global Mutex (General)
	knowledgeText string                      // Combined scraped & file text
	appDB         *sql.DB                     // Application Database (MySQL)
	dbSchema      string                      // Table schema for AI
	sheetsService *sheets.Service             // Google Sheets Service
	sheetSchema   string                      // Sheet names & headers for AI
	sessionStore  *session.Store              // Session Store
	authDB        *sql.DB                     // SQLite for Users
	chatHistories = make(map[string][]string) // Chat History Memory
	historyMutex  sync.Mutex                  // Mutex for Chat History

	// Simple in-memory rate limiter for auth endpoints
	authRateLimitMap = make(map[string][]time.Time)
	authRateLimitMux sync.RWMutex

	// Tenant-specific knowledge cache
	tenantKnowledge   = make(map[int]string)
	knowledgeMutex    sync.RWMutex
)

const redactedSecret = "__REDACTED__"

type User struct {
	ID       int    `json:"id"`
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
	IsAdmin  bool   `json:"is_admin"`
	IsActive bool   `json:"is_active"`
}

type UserDevice struct {
	ID        int       `json:"id"`
	UserID    int       `json:"user_id"`
	DeviceJID string    `json:"device_jid"`
	Alias     string    `json:"alias"` // Optional name for device
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type FollowupTask struct {
	ID            int       `json:"id"`
	UserID        int       `json:"user_id"`
	TenantID      int       `json:"tenant_id"`
	JID           string    `json:"jid"`
	ScheduledTime time.Time `json:"scheduled_time"`
	Instruction   string    `json:"instruction"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}

// ========== PASSWORD UTILITIES ==========

// hashPassword creates a bcrypt hash from a plain password
func hashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// checkPassword compares a plain password with a bcrypt hash
// Returns nil if match, error otherwise
func checkPassword(password, hash string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}

// isPasswordHashed checks if a password string is already bcrypt hashed
func isPasswordHashed(password string) bool {
	// bcrypt hashes start with $2a$, $2b$, or $2y$
	return strings.HasPrefix(password, "$2a$") ||
		strings.HasPrefix(password, "$2b$") ||
		strings.HasPrefix(password, "$2y$")
}

// migrateUserPassword upgrades a plaintext password to bcrypt hash
func migrateUserPassword(userID int, plaintextPassword string) {
	hashed, err := hashPassword(plaintextPassword)
	if err != nil {
		log.Println("Failed to migrate password for user", userID, ":", err)
		return
	}
	_, err = authDB.Exec("UPDATE users SET password = ? WHERE id = ?", hashed, userID)
	if err != nil {
		log.Println("Failed to update password during migration for user", userID, ":", err)
	} else {
		log.Printf("Password migrated to bcrypt for user ID: %d", userID)
	}
}

// generateSecureOTP creates a 6-digit cryptographically secure OTP
func generateSecureOTP() (string, error) {
	// Generate random number between 0 and 999999
	otpInt, err := crand.Int(crand.Reader, big.NewInt(1000000))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%06d", otpInt), nil
}

// rateLimitMiddleware limits auth requests to 5 per minute per IP
func rateLimitMiddleware(c *fiber.Ctx) error {
	ip := c.IP()
	if ip == "" {
		ip = "unknown"
	}

	now := time.Now()
	window := time.Minute
	maxAttempts := 5

	authRateLimitMux.Lock()
	defer authRateLimitMux.Unlock()

	// Get timestamps for this IP
	timestamps := authRateLimitMap[ip]

	// Remove timestamps older than window
	cutoff := now.Add(-window)
	newTimestamps := make([]time.Time, 0, maxAttempts)
	for _, ts := range timestamps {
		if ts.After(cutoff) {
			newTimestamps = append(newTimestamps, ts)
		}
	}

	// Check if limit exceeded
	if len(newTimestamps) >= maxAttempts {
		// Calculate reset time (time of oldest attempt + window)
		oldest := newTimestamps[0]
		resetTime := oldest.Add(window)
		waitDuration := time.Until(resetTime)
		if waitDuration < 0 {
			waitDuration = 0
		}
		return c.Status(429).JSON(fiber.Map{
			"success": false,
			"message": fmt.Sprintf("Terlalu banyak permintaan. Coba lagi dalam %s", waitDuration.Round(time.Second)),
		})
	}

	// Add current timestamp and allow
	newTimestamps = append(newTimestamps, now)
	authRateLimitMap[ip] = newTimestamps

	return c.Next()
}

func isProduction() bool {
	env := strings.ToLower(os.Getenv("APP_ENV"))
	return env == "production" || env == "prod"
}

func requireAdmin(c *fiber.Ctx) error {
	if isAdmin, ok := c.Locals("isAdmin").(bool); ok && isAdmin {
		return c.Next()
	}
	return c.Status(403).JSON(fiber.Map{"success": false, "message": "Requires Admin privileges"})
}

// Tenant identification middleware
func tenantMiddleware(c *fiber.Ctx) error {
	// Get tenant ID from header (X-Tenant-ID)
	tenantIDStr := c.Get("X-Tenant-ID")
	if tenantIDStr == "" {
		// Default to tenant ID 1 if not specified
		c.Locals("tenantID", 1)
		return c.Next()
	}

	// Parse tenant ID
	var tenantID int
	if _, err := fmt.Sscan(tenantIDStr, &tenantID); err != nil {
		return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid tenant ID"})
	}

	// Verify tenant exists
	var exists bool
	err := authDB.QueryRow("SELECT EXISTS(SELECT 1 FROM tenants WHERE id = ?)", tenantID).Scan(&exists)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database error"})
	}
	if !exists {
		return c.Status(404).JSON(fiber.Map{"success": false, "message": "Tenant not found"})
	}

	// Set tenant ID in context
	c.Locals("tenantID", tenantID)
	return c.Next()
}

func redactSecret(value string) string {
	if value == "" {
		return ""
	}
	return redactedSecret
}

func isRedactedSecret(value string) bool {
	return value == redactedSecret || value == "********"
}

func redactedConfig(src Config) Config {
	dst := src
	dst.AdminPassword = redactSecret(dst.AdminPassword)
	dst.Database.Password = redactSecret(dst.Database.Password)
	dst.Sheet.CredentialsJSON = redactSecret(dst.Sheet.CredentialsJSON)

	if src.Providers != nil {
		dst.Providers = make(map[string]ProviderConfig, len(src.Providers))
		for name, provider := range src.Providers {
			provider.APIKey = redactSecret(provider.APIKey)
			dst.Providers[name] = provider
		}
	}

	return dst
}

func preserveRedactedSecrets(next *Config, current Config) {
	if isRedactedSecret(next.AdminPassword) {
		next.AdminPassword = current.AdminPassword
	}
	if isRedactedSecret(next.Database.Password) {
		next.Database.Password = current.Database.Password
	}
	if isRedactedSecret(next.Sheet.CredentialsJSON) {
		next.Sheet.CredentialsJSON = current.Sheet.CredentialsJSON
	}

	if next.Providers == nil {
		next.Providers = make(map[string]ProviderConfig)
	}
	for name, provider := range next.Providers {
		if isRedactedSecret(provider.APIKey) {
			if existing, ok := current.Providers[name]; ok {
				provider.APIKey = existing.APIKey
				next.Providers[name] = provider
			}
		}
	}
}

func main() {
	// 1. Load Config
	loadConfig()

	// Overlay sensitive credentials from environment variables for security
	overlayEnvConfig()

	// Connect to DB & Sheets in background
	go func() {
		connectAppDB()
		connectSheets()
	}()

	// 2. Setup Database
	dbLog := waLog.Stdout("Database", "ERROR", true)
	var err error

	// Open Shared SQLite Connection to prevent locking issues
	sharedDB, err := sql.Open("sqlite", "file:wahaku.db?_pragma=foreign_keys(1)&_busy_timeout=5000")
	if err != nil {
		log.Fatal("Failed to open Shared DB:", err)
	}
	sharedDB.SetMaxOpenConns(5)
	sharedDB.SetMaxIdleConns(5)
	sharedDB.SetConnMaxLifetime(time.Hour)

	// Init Whatsmeow with shared DB
	container = sqlstore.NewWithDB(sharedDB, "sqlite", dbLog)

	// Ensure tables are created (force migration if needed)
	if err := container.Upgrade(context.Background()); err != nil {
		log.Println("Whatsmeow Store Upgrade Warning:", err)
		// Fallback: Create table manually if Upgrade failed
		_, execErr := sharedDB.Exec(`CREATE TABLE IF NOT EXISTS whatsmeow_device (
			jid TEXT PRIMARY KEY,
			registration_id INTEGER,
			noise_key BLOB,
			identity_key BLOB,
			signed_pre_key BLOB,
			signed_pre_key_id INTEGER,
			signed_pre_key_sig BLOB,
			adv_secret_key BLOB,
			created_at DATETIME,
			os TEXT,
			platform TEXT,
			require_full_sync BOOLEAN
		);`)
		if execErr != nil {
			log.Println("Manual Table Creation Failed:", execErr)
		}
	}

	// Init Auth DB (Shared)
	authDB = sharedDB

	// Create Tables
	_, err = authDB.Exec(`CREATE TABLE IF NOT EXISTS tenants (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT UNIQUE,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		username TEXT UNIQUE,
		email TEXT,
		password TEXT,
		is_admin BOOLEAN DEFAULT 0,
		is_active BOOLEAN DEFAULT 0,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id)
	);
	CREATE TABLE IF NOT EXISTS user_devices (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		user_id INTEGER,
		device_jid TEXT,
		alias TEXT,
		status TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id),
		FOREIGN KEY(user_id) REFERENCES users(id)
	);
	CREATE TABLE IF NOT EXISTS followup_tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		user_id INTEGER,
		jid TEXT,
		scheduled_time DATETIME,
		instruction TEXT,
		status TEXT DEFAULT 'pending',
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id),
		FOREIGN KEY(user_id) REFERENCES users(id)
	);
	CREATE TABLE IF NOT EXISTS tenant_knowledge_files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL,
		filename TEXT NOT NULL,
		original_name TEXT,
		uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id),
		UNIQUE(tenant_id, filename)
	);`)
	if err != nil {
		log.Fatal("Failed to create tables:", err)
	}

	// Migration: Add email column if not exists
	var emailColCount int
	authDB.QueryRow("SELECT COUNT(*) FROM pragma_table_info('users') WHERE name='email'").Scan(&emailColCount)
	if emailColCount == 0 {
		log.Println("Migrating DB: Adding email column to users table...")
		authDB.Exec("ALTER TABLE users ADD COLUMN email TEXT")
	}

	// Migration: Add is_primary to user_devices
	var isPrimaryColCount int
	authDB.QueryRow("SELECT COUNT(*) FROM pragma_table_info('user_devices') WHERE name='is_primary'").Scan(&isPrimaryColCount)
	if isPrimaryColCount == 0 {
		log.Println("Migrating DB: Adding is_primary column to user_devices table...")
		authDB.Exec("ALTER TABLE user_devices ADD COLUMN is_primary BOOLEAN DEFAULT 0")
	}

	// Migration: Move device_jid from users to user_devices if exists
	var deviceJIDColCount int
	authDB.QueryRow("SELECT COUNT(*) FROM pragma_table_info('users') WHERE name='device_jid'").Scan(&deviceJIDColCount)
	if deviceJIDColCount > 0 {
		log.Println("Migrating DB: Moving device_jid to user_devices table...")
		// Select existing
		type migrationData struct {
			UserID int
			JID    string
		}
		var dataToMigrate []migrationData

		// Get tenant ID from context (not available during migration, using default)
		var tenantID int = 1 // Default tenant ID

		rows, err := authDB.Query("SELECT id, device_jid FROM users WHERE device_jid IS NOT NULL AND device_jid != '' AND tenant_id = ?", tenantID)
		if err == nil && rows != nil {
			for rows.Next() {
				var uid int
				var jid string
				if err := rows.Scan(&uid, &jid); err == nil {
					dataToMigrate = append(dataToMigrate, migrationData{UserID: uid, JID: jid})
				}
			}
			rows.Close()
		}

		// Insert to new table
		for _, d := range dataToMigrate {
			_, err := authDB.Exec("INSERT INTO user_devices (tenant_id, user_id, device_jid, alias, status) VALUES (?, ?, ?, 'Main Device', 'CONNECTED')", tenantID, d.UserID, d.JID)
			if err != nil {
				log.Println("Migration Insert Error:", err)
			}
		}
		// Note: We don't drop column in SQLite easily, so we just ignore it
	}

	// Create default tenant if not exists
	var tenantCount int
	authDB.QueryRow("SELECT COUNT(*) FROM tenants").Scan(&tenantCount)
	if tenantCount == 0 {
		authDB.Exec("INSERT INTO tenants (name) VALUES (?)", "default")
		log.Println("Created default tenant")
	}

	// Create Admin if not exists
	var count int
	authDB.QueryRow("SELECT COUNT(*) FROM users WHERE username = ?", cfg.AdminUsername).Scan(&count)
	if count == 0 {
		adminPassword := cfg.AdminPassword
		if adminPassword == "" {
			log.Println("Initial admin user was not created because admin_password/ADMIN_PASSWORD is empty")
		} else {
			if adminPassword != "" && !isPasswordHashed(adminPassword) {
				if hashed, hashErr := hashPassword(adminPassword); hashErr == nil {
					adminPassword = hashed
				} else {
					log.Println("Failed to hash initial admin password:", hashErr)
				}
			}
			// Get default tenant ID
			var tenantID int
			authDB.QueryRow("SELECT id FROM tenants WHERE name = ?", "default").Scan(&tenantID)
			authDB.Exec("INSERT INTO users (username, password, tenant_id, is_admin, is_active) VALUES (?, ?, ?, 1, 1)", cfg.AdminUsername, adminPassword, tenantID)
		}
	}

	// 3. Initialize Clients for existing users (lazy load or eager load)
	// For now, we will lazy load on request or login, BUT we need the Admin/System bot for OTP.
	// Let's try to load the Admin's client immediately.
	go initAdminClient()

	// Start Follow-up Scheduler
	go processFollowups()

	// Load knowledge base for all tenants
	go refreshKnowledge()

	// 5. Setup Fiber
	sessionStore = session.New(session.Config{
		KeyLookup:      "cookie:session_id",
		CookieHTTPOnly: true,
		CookieSecure:   isProduction(),
		CookieSameSite: "lax",
		Expiration:     24 * time.Hour,
	})

	app := fiber.New(fiber.Config{
		BodyLimit: 50 * 1024 * 1024, // 50MB Limit
	})
	allowedOrigins := os.Getenv("CORS_ALLOW_ORIGINS")
	if allowedOrigins == "" {
		allowedOrigins = "http://localhost:" + cfg.AppPort + ",http://127.0.0.1:" + cfg.AppPort
	}
	app.Use(cors.New(cors.Config{
		AllowOrigins:     allowedOrigins,
		AllowCredentials: true,
	}))

	// CSRF Protection for state-changing endpoints
	csrfMiddleware := csrf.New(csrf.Config{
		KeyLookup:      "header:X-CSRF-Token",
		CookieName:     "csrf_token",
		CookieHTTPOnly: false, // Must be readable by JavaScript
		CookieSecure:   isProduction(),
		CookieSameSite: "Lax",
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			if c.Accepts("application/json") == "application/json" {
				return c.Status(403).JSON(fiber.Map{
					"success": false,
					"message": "CSRF token invalid or missing",
				})
			}
			return c.Status(403).SendString("CSRF token invalid or missing")
		},
	})
	app.Use(csrfMiddleware)
	app.Use(tenantMiddleware)

	// Middleware Auth
	app.Use(func(c *fiber.Ctx) error {
		// Whitelist Static Assets
		path := c.Path()
		if strings.HasSuffix(path, ".css") || strings.HasSuffix(path, ".js") || strings.HasSuffix(path, ".png") || strings.HasSuffix(path, ".jpg") || strings.HasSuffix(path, ".ico") || strings.HasSuffix(path, ".svg") {
			return c.Next()
		}

		// Whitelist Routes
		if path == "/login" || path == "/register" || strings.HasPrefix(path, "/api/auth") {
			return c.Next()
		}

		// Check Session
		sess, err := sessionStore.Get(c)
		if err != nil {
			return c.Redirect("/login")
		}

		if sess.Get("authenticated") != true {
			// If API request, return JSON 401
			if strings.HasPrefix(c.Path(), "/api") {
				return c.Status(401).JSON(fiber.Map{"success": false, "message": "Unauthorized"})
			}
			// Otherwise redirect to login
			return c.Redirect("/login")
		}

		// Set Locals
		if uid := sess.Get("userID"); uid != nil {
			c.Locals("userID", uid)
		}
		if isAdmin := sess.Get("isAdmin"); isAdmin != nil {
			c.Locals("isAdmin", isAdmin)
		}
		// Set tenantID from context (set by tenantMiddleware)
		if tenantID := c.Locals("tenantID"); tenantID != nil {
			sess.Set("tenantID", tenantID)
			c.Locals("tenantID", tenantID)
		}

		return c.Next()
	})

	// Ensure uploads directory exists
	os.MkdirAll("uploads", 0755)

	// Serve Static Files
	app.Get("/login", func(c *fiber.Ctx) error {
		return c.SendFile("./views/login.html")
	})
	app.Get("/register", func(c *fiber.Ctx) error {
		return c.SendFile("./views/register.html")
	})
	app.Get("/dashboard", func(c *fiber.Ctx) error {
		return c.SendFile("./views/index.html")
	})
	app.Static("/", "./views")

	// API Routes
	api := app.Group("/api")

	// Auth Routes
	auth := api.Group("/auth")
	// Apply rate limiting to all auth endpoints
	auth.Use(rateLimitMiddleware)
	auth.Post("/login", func(c *fiber.Ctx) error {
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid Request"})
		}

		// Check DB
		var user User
		var err error

		// Determine if login is by Email or Username (Phone)
		isEmailLogin := strings.Contains(req.Username, "@")

		// Get tenant ID from context (set by tenantMiddleware)
		tenantIDVal := c.Locals("tenantID")
		var tenantID int
		if tenantIDVal != nil {
			tenantID = tenantIDVal.(int)
		} else {
			// Default to tenant 1 if not set (should not happen due to middleware)
			tenantID = 1
		}

		query := "SELECT id, username, COALESCE(email, ''), COALESCE(password, ''), COALESCE(is_admin, 0), COALESCE(is_active, 0) FROM users WHERE username = ? AND tenant_id = ?"
		if isEmailLogin {
			query = "SELECT id, username, COALESCE(email, ''), COALESCE(password, ''), COALESCE(is_admin, 0), COALESCE(is_active, 0) FROM users WHERE email = ? AND tenant_id = ?"
		}

		err = authDB.QueryRow(query, req.Username, tenantID).Scan(&user.ID, &user.Username, &user.Email, &user.Password, &user.IsAdmin, &user.IsActive)

		log.Printf("[LOGIN DEBUG] User: %s, ID: %d, Active: %v, Admin: %v, Err: %v", req.Username, user.ID, user.IsActive, user.IsAdmin, err)

		if err == sql.ErrNoRows {
			return c.Status(401).JSON(fiber.Map{"success": false, "message": "User tidak ditemukan"})
		} else if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database Error"})
		}

		// Check Active
		if !user.IsActive {
			log.Printf("[LOGIN DEBUG] User %s is inactive. Returning pending_approval.", req.Username)
			return c.JSON(fiber.Map{"success": true, "pending_approval": true, "message": "Akun Anda sedang menunggu persetujuan admin."})
		}

		// Check Password (MANDATORY)
		if req.Password == "" {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Password wajib diisi"})
		}

		passwordMatch := false

		// Handle legacy plaintext passwords OR bcrypt hashed passwords
		if user.Password != "" {
			if isPasswordHashed(user.Password) {
				// New: bcrypt verification
				err = checkPassword(req.Password, user.Password)
				if err == nil {
					passwordMatch = true
				}
			} else {
				// Legacy: plaintext comparison (migration path)
				if req.Password == user.Password {
					passwordMatch = true
					// Migrate to hashed password on successful login
					go migrateUserPassword(user.ID, req.Password)
				}
			}
		}

		if !passwordMatch {
			return c.Status(401).JSON(fiber.Map{"success": false, "message": "Password salah"})
		}

		// Password is Valid -> Send OTP

		// Helper to finalize login without OTP (Fallback)
		finalizeLogin := func() error {
			sess, err := sessionStore.Get(c)
			if err == nil {
				sess.Set("authenticated", true)
				sess.Set("userID", user.ID)
				sess.Set("isAdmin", user.IsAdmin)
				// Set tenantID from context
				tenantID := c.Locals("tenantID").(int)
				sess.Set("tenantID", tenantID)
				sess.Save()
				return c.JSON(fiber.Map{"success": true, "message": "Login Berhasil (OTP Skipped)"})
			}
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Session Error"})
		}

		// GET SYSTEM BOT (Admin Bot) to send OTP
		sysClient := getSystemBot(tenantID)

		// Check if System Bot is connected
		if sysClient == nil || !sysClient.IsConnected() {
			if user.Password != "" {
				log.Printf("[LOGIN WARN] System Bot offline, skipping OTP for user: %s", user.Username)
				return finalizeLogin()
			}
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Sistem Bot belum terhubung, tidak bisa kirim OTP."})
		}

		// Generate OTP
		otp, err := generateSecureOTP()
		if err != nil {
			log.Println("Failed to generate OTP:", err)
			if user.Password != "" {
				log.Printf("[LOGIN WARN] Failed to generate OTP, skipping for password-authenticated user: %s", user.Username)
				return finalizeLogin()
			}
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal generate OTP"})
		}

		// Save OTP to session
		sess, err := sessionStore.Get(c)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Session Error"})
		}

		sess.Set("otp", otp)
		sess.Set("otp_expiry", time.Now().Add(5*time.Minute).Unix())
		sess.Set("temp_auth", true)
		sess.Set("pending_user_id", user.ID)
		sess.Set("pending_is_admin", user.IsAdmin)
		sess.Set("tenantID", tenantID)
		if err := sess.Save(); err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Failed to save session"})
		}

		// Send OTP via WhatsApp
		targetJID := types.NewJID(user.Username, types.DefaultUserServer)

		if sysClient.Store.ID != nil && targetJID.User == sysClient.Store.ID.User {
			targetJID = *sysClient.Store.ID
			targetJID.Device = 0
		}

		msg := &waE2E.Message{
			Conversation: proto.String("🔐 Kode Login Wahaku Dashboard: *" + otp + "*\n\nJangan berikan kode ini kepada siapapun."),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := sysClient.SendMessage(ctx, targetJID, msg)
		if err != nil {
			log.Println("Failed to send OTP (Timeout/Error):", err)
			if user.Password != "" {
				log.Printf("[LOGIN WARN] Failed to send OTP, skipping for password-authenticated user: %s", user.Username)
				return finalizeLogin()
			}
			return c.JSON(fiber.Map{"success": false, "message": "Gagal kirim OTP (Timeout). Pastikan bot terhubung."})
		}

		log.Printf("OTP Sent to %s (ID: %s, Length: %d)", targetJID.User, resp.ID, len(otp))
		return c.JSON(fiber.Map{"success": true, "require_otp": true, "message": "OTP dikirim ke WhatsApp"})
	})

	auth.Post("/verify", func(c *fiber.Ctx) error {
		var req struct {
			OTP string `json:"otp"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid Request"})
		}

		sess, err := sessionStore.Get(c)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Session Error"})
		}

		if sess.Get("temp_auth") != true {
			return c.Status(401).JSON(fiber.Map{"success": false, "message": "Sesi tidak valid, silakan login ulang"})
		}

		storedOTP := sess.Get("otp")
		expiryVal := sess.Get("otp_expiry")

		if storedOTP == nil || expiryVal == nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "OTP kadaluarsa atau tidak ditemukan"})
		}

		expiry := expiryVal.(int64)
		if time.Now().Unix() > expiry {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "OTP sudah kadaluarsa"})
		}

		if req.OTP == storedOTP.(string) {
			userID := sess.Get("pending_user_id").(int)
			isAdmin := sess.Get("pending_is_admin").(bool)
			tenantID := sess.Get("tenantID")
			var tenantIDInt int
			if tenantID != nil {
				tenantIDInt = tenantID.(int)
			} else {
				// Default to tenant 1 if not set (should not happen)
				tenantIDInt = 1
			}

			log.Printf("[OTP SUCCESS] UserID: %d. Activating user...", userID)

			_, err := authDB.Exec("UPDATE users SET is_active = 1 WHERE id = ? AND tenant_id = ?", userID, tenantIDInt)
			if err != nil {
				log.Println("Failed to activate user:", err)
			}

			sess.Set("authenticated", true)
			sess.Set("userID", userID)
			sess.Set("isAdmin", isAdmin)
			sess.Set("tenantID", tenantIDInt)
			sess.Delete("otp")
			sess.Delete("otp_expiry")
			sess.Delete("temp_auth")
			sess.Delete("pending_user_id")
			sess.Delete("pending_is_admin")
			sess.Save()
			return c.JSON(fiber.Map{"success": true})
		}

		return c.Status(401).JSON(fiber.Map{"success": false, "message": "Kode OTP salah"})
	})

	// Resend OTP endpoint
	auth.Post("/resend-otp", func(c *fiber.Ctx) error {
		sess, err := sessionStore.Get(c)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Session Error"})
		}

		// Check if there's a pending verification
		pendingUserID := sess.Get("pending_user_id")
		if pendingUserID == nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Tidak ada permintaan verifikasi yang active"})
		}

		userID := pendingUserID.(int)

		// Get tenant ID from session
		tenantIDVal := sess.Get("tenantID")
		var tenantID int
		if tenantIDVal != nil {
			tenantID = tenantIDVal.(int)
		} else {
			// Default to tenant 1 if not set (should not happen)
			tenantID = 1
		}

		// Fetch user from database
		var user User
		err = authDB.QueryRow("SELECT id, username, COALESCE(email, ''), COALESCE(password, ''), COALESCE(is_admin, 0), COALESCE(is_active, 0) FROM users WHERE id = ? AND tenant_id = ?", userID, tenantID).
			Scan(&user.ID, &user.Username, &user.Email, &user.Password, &user.IsAdmin, &user.IsActive)
		if err != nil {
			return c.Status(404).JSON(fiber.Map{"success": false, "message": "User tidak ditemukan"})
		}

		// Check if system bot is available
		sysClient := getSystemBot(tenantID)
		if sysClient == nil || !sysClient.IsConnected() {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Sistem Bot belum terhubung, tidak bisa kirim OTP"})
		}

		// Generate new OTP
		otp, err := generateSecureOTP()
		if err != nil {
			log.Println("Failed to generate OTP for resend:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal generate OTP"})
		}

		// Update session with new OTP
		sess.Set("otp", otp)
		sess.Set("otp_expiry", time.Now().Add(5*time.Minute).Unix())
		sess.Save()

		// Send OTP via WhatsApp
		targetJID := types.NewJID(user.Username, types.DefaultUserServer)
		if sysClient.Store.ID != nil && targetJID.User == sysClient.Store.ID.User {
			targetJID = *sysClient.Store.ID
			targetJID.Device = 0
		}

		msg := &waE2E.Message{
			Conversation: proto.String("🔐 Kode Verifikasi (Ulang): *" + otp + "*\n\nMasukkan kode ini untuk menyelesaikan verifikasi."),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := sysClient.SendMessage(ctx, targetJID, msg)
		if err != nil {
			log.Println("Failed to send resend OTP:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal kirim OTP"})
		}

		log.Printf("OTP Resent to %s: ID=%s", targetJID.User, resp.ID)
		return c.JSON(fiber.Map{"success": true, "message": "Kode OTP baru telah dikirim"})
	})

	auth.Post("/register", func(c *fiber.Ctx) error {
		var req struct {
			Username string `json:"username"`
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid Request"})
		}

		if req.Email != "" {
			if !strings.Contains(req.Email, "@") {
				return c.Status(400).JSON(fiber.Map{"success": false, "message": "Format Email tidak valid"})
			}
			// Get tenant ID from context (set by tenantMiddleware)
			tenantIDVal := c.Locals("tenantID")
			var tenantID int
			if tenantIDVal != nil {
				tenantID = tenantIDVal.(int)
			} else {
				// Default to tenant 1 if not set (should not happen due to middleware)
				tenantID = 1
			}
			var count int
			authDB.QueryRow("SELECT COUNT(*) FROM users WHERE email = ? AND tenant_id = ?", req.Email, tenantID).Scan(&count)
			if count > 0 {
				return c.Status(400).JSON(fiber.Map{"success": false, "message": "Email sudah terdaftar"})
			}
		}

		if len(req.Username) < 10 {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Nomor WhatsApp tidak valid (Wajib)"})
		}
		if len(req.Password) < 8 {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Password minimal 8 karakter"})
		}

		// Hash password before storing
		hashedPassword, err := hashPassword(req.Password)
		if err != nil {
			log.Println("Failed to hash password:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal memproses password"})
		}

		// Get tenant ID from context (set by tenantMiddleware)
		tenantIDVal := c.Locals("tenantID")
		var tenantID int
		if tenantIDVal != nil {
			tenantID = tenantIDVal.(int)
		} else {
			// Default to tenant 1 if not set (should not happen due to middleware)
			tenantID = 1
		}

		_, err = authDB.Exec("INSERT INTO users (username, email, password, tenant_id, is_admin, is_active) VALUES (?, ?, ?, ?, 0, 0)", req.Username, req.Email, hashedPassword, tenantID)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Nomor WhatsApp atau Email sudah terdaftar"})
		}

		// SEND OTP
		sysClient := getSystemBot(tenantID)
		if sysClient == nil || !sysClient.IsConnected() {
			return c.JSON(fiber.Map{"success": true, "require_otp": false, "message": "Pendaftaran berhasil. Bot belum terhubung, tidak bisa kirim OTP."})
		}

		otp, err := generateSecureOTP()
		if err != nil {
			log.Println("Failed to generate OTP:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal generate OTP"})
		}

		// Get user ID for OTP session
		var userID int
		authDB.QueryRow("SELECT id FROM users WHERE username = ? AND tenant_id = ?", req.Username, tenantID).Scan(&userID)

		sess, err := sessionStore.Get(c)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Session Error"})
		}

		sess.Set("otp", otp)
		sess.Set("otp_expiry", time.Now().Add(5*time.Minute).Unix())
		sess.Set("temp_auth", true)
		sess.Set("pending_user_id", userID)
		sess.Set("pending_is_admin", false)
		sess.Set("tenantID", tenantID)
		sess.Save()

		targetJID := types.NewJID(req.Username, types.DefaultUserServer)

		if sysClient.Store.ID != nil && targetJID.User == sysClient.Store.ID.User {
			targetJID = *sysClient.Store.ID
			targetJID.Device = 0
		}

		msg := &waE2E.Message{
			Conversation: proto.String("🔐 Kode Verifikasi Pendaftaran Wahaku: *" + otp + "*\n\nMasukkan kode ini untuk menyelesaikan pendaftaran."),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := sysClient.SendMessage(ctx, targetJID, msg)
		if err != nil {
			log.Println("Failed to send OTP (Register):", err)
			return c.JSON(fiber.Map{"success": true, "require_otp": true, "message": "Pendaftaran berhasil, tapi gagal kirim OTP. Silakan login ulang."})
		}

		log.Printf("Register OTP Sent to %s (ID: %s, Length: %d)", targetJID.User, resp.ID, len(otp))

		return c.JSON(fiber.Map{"success": true, "require_otp": true, "message": "Pendaftaran berhasil. Masukkan kode OTP yang dikirim ke WhatsApp."})
	})

	auth.Post("/logout", func(c *fiber.Ctx) error {
		sess, err := sessionStore.Get(c)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Session Error"})
		}

		if err := sess.Destroy(); err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Failed to destroy session"})
		}

		return c.JSON(fiber.Map{"success": true})
	})

	api.Get("/status", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)

		clientMutex.Lock()
		defer clientMutex.Unlock()

		status := "DISCONNECTED"
		qr := ""

		// Check statuses
		if statuses, ok := userStatuses[userID]; ok {
			for _, s := range statuses {
				if s == "CONNECTED" {
					status = "CONNECTED"
					break
				}
			}
			// If we are pairing, show that
			if s, ok := statuses["NEW"]; ok {
				if status != "CONNECTED" {
					status = s
				}
			}
		}

		// Check QR
		if qrs, ok := userQRCodes[userID]; ok {
			if q, ok := qrs["NEW"]; ok {
				qr = q
			}
		}

		return c.JSON(fiber.Map{
			"status": status,
			"qr":     qr,
		})
	})

	api.Get("/config", requireAdmin, func(c *fiber.Ctx) error {
		// Get tenant-specific knowledge files
		tenantID := c.Locals("tenantID").(int)
		config := redactedConfig(cfg)

		// Query knowledge files for this tenant
		rows, err := authDB.Query("SELECT filename FROM tenant_knowledge_files WHERE tenant_id = ?", tenantID)
		if err != nil {
			log.Println("Error querying knowledge files:", err)
		} else {
			defer rows.Close()
			var files []string
			for rows.Next() {
				var filename string
				if err := rows.Scan(&filename); err == nil {
					files = append(files, filename)
				}
			}
			config.KnowledgeFiles = files
		}

		return c.JSON(config)
	})

	api.Post("/config", requireAdmin, func(c *fiber.Ctx) error {
		var newCfg Config
		if err := c.BodyParser(&newCfg); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}
		preserveRedactedSecrets(&newCfg, cfg)
		cfg = newCfg
		saveConfig()
		go connectAppDB()
		go connectSheets()
		go refreshKnowledge()
		return c.JSON(fiber.Map{"success": true})
	})

	// Follow-up Routes (unchanged)
	api.Post("/followup", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		var req struct {
			JID          string `json:"jid"`
			DelayMinutes int    `json:"delay_minutes"`
			Instruction  string `json:"instruction"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid JSON"})
		}

		scheduledTime := time.Now().Add(time.Duration(req.DelayMinutes) * time.Minute)
		_, err := authDB.Exec("INSERT INTO followup_tasks (tenant_id, user_id, jid, scheduled_time, instruction, status) VALUES (?, ?, ?, ?, ?, 'pending')",
			tenantID, userID, req.JID, scheduledTime, req.Instruction)

		if err != nil {
			log.Println("Error creating followup:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database Error"})
		}

		return c.JSON(fiber.Map{"success": true, "message": "Follow-up scheduled"})
	})

	api.Get("/followup", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		rows, err := authDB.Query("SELECT id, jid, scheduled_time, instruction, status FROM followup_tasks WHERE user_id = ? AND tenant_id = ? AND status = 'pending' ORDER BY scheduled_time ASC", userID, tenantID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Database error"})
		}
		defer rows.Close()

		var tasks []FollowupTask
		for rows.Next() {
			var t FollowupTask
			if err := rows.Scan(&t.ID, &t.JID, &t.ScheduledTime, &t.Instruction, &t.Status); err == nil {
				tasks = append(tasks, t)
			}
		}
		return c.JSON(fiber.Map{"success": true, "tasks": tasks})
	})

	api.Delete("/followup/:id", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		id := c.Params("id")

		res, err := authDB.Exec("DELETE FROM followup_tasks WHERE id = ? AND user_id = ? AND tenant_id = ?", id, userID, tenantID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Database error"})
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "Task not found"})
		}
		return c.JSON(fiber.Map{"success": true})
	})

	// Upload/Delete File (with security validations)
	api.Post("/upload", func(c *fiber.Ctx) error {
		file, err := c.FormFile("file")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "No file uploaded"})
		}

		// Validate file size (max 10MB)
		if file.Size > 10*1024*1024 {
			return c.Status(400).JSON(fiber.Map{"error": "File terlalu besar (maksimal 10MB)"})
		}

		// Validate file extension
		ext := strings.ToLower(filepath.Ext(file.Filename))
		allowedExts := map[string]bool{
			".pdf":  true,
			".txt":  true,
			".md":   true,
			".doc":  true,
			".docx": true,
		}
		if !allowedExts[ext] {
			return c.Status(400).JSON(fiber.Map{"error": "Tipe file tidak diizinkan. Hanya PDF, TXT, MD, DOC, DOCX"})
		}

		// Get tenant ID
		tenantID := c.Locals("tenantID").(int)

		// Sanitize filename: generate server-side name to avoid path traversal and collisions.
		suffix, err := crand.Int(crand.Reader, big.NewInt(1000000))
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to generate safe filename"})
		}
		safeFilename := fmt.Sprintf("%s_%06d%s", time.Now().Format("20060102_150405"), suffix.Int64(), ext)
		path := "uploads/" + safeFilename

		if err := c.SaveFile(file, path); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to save file"})
		}

		// Insert into tenant_knowledge_files
		_, err = authDB.Exec("INSERT INTO tenant_knowledge_files (tenant_id, filename, original_name) VALUES (?, ?, ?)",
			tenantID, safeFilename, file.Filename)
		if err != nil {
			// Remove the saved file if DB insert fails
			os.Remove(path)
			log.Println("Error inserting tenant knowledge file:", err)
			return c.Status(500).JSON(fiber.Map{"error": "Failed to register file"})
		}

		// Rebuild knowledge for this tenant
		go rebuildTenantKnowledge(tenantID)

		return c.JSON(fiber.Map{"success": true, "filename": safeFilename, "original_filename": file.Filename})
	})

	api.Post("/delete-file", func(c *fiber.Ctx) error {
		var req struct {
			Filename string `json:"filename"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}
		filename := filepath.Base(req.Filename)
		tenantID := c.Locals("tenantID").(int)

		// Check if file belongs to this tenant
		var count int
		err := authDB.QueryRow("SELECT COUNT(*) FROM tenant_knowledge_files WHERE filename = ? AND tenant_id = ?", filename, tenantID).Scan(&count)
		if err != nil {
			log.Println("Error checking file ownership:", err)
			return c.Status(500).JSON(fiber.Map{"error": "Database error"})
		}
		if count == 0 {
			return c.Status(404).JSON(fiber.Map{"success": false, "message": "File tidak ditemukan di daftar."})
		}

		// Delete from DB
		authDB.Exec("DELETE FROM tenant_knowledge_files WHERE filename = ? AND tenant_id = ?", filename, tenantID)

		// Delete file from disk
		os.Remove("uploads/" + filename)

		// Rebuild knowledge for this tenant
		go rebuildTenantKnowledge(tenantID)

		return c.JSON(fiber.Map{"success": true})
	})

	api.Post("/control", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		var body struct {
			Command string `json:"command"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid Body"})
		}

		switch body.Command {
		case "logout":
			// Logout User's Client
			cli := getUserClient(userID)
			if cli != nil {
				cli.Logout(context.Background())
				cli.Disconnect()
				// Remove association in DB
				authDB.Exec("UPDATE users SET device_jid = '' WHERE id = ?", userID)
				// Remove from memory
				clientMutex.Lock()
				delete(userClients, userID)
				delete(userStatuses, userID)
				delete(userQRCodes, userID)
				clientMutex.Unlock()
			}
			return c.JSON(fiber.Map{"success": true, "message": "Device Logged out"})
		case "restart":
			cli := getUserClient(userID)
			if cli != nil {
				cli.Disconnect()
				time.Sleep(1 * time.Second)
				cli.Connect()
			}
			return c.JSON(fiber.Map{"success": true, "message": "Restarting connection..."})
		default:
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Perintah tidak dikenal"})
		}
	})

	api.Post("/test-db", func(c *fiber.Ctx) error {
		if appDB == nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Database belum terhubung."})
		}
		if err := appDB.Ping(); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Gagal ping database: " + err.Error()})
		}
		return c.JSON(fiber.Map{"success": true, "message": "Koneksi Database Berhasil! Schema:\n" + dbSchema})
	})

	api.Post("/test-sheet", func(c *fiber.Ctx) error {
		if sheetsService == nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Google Sheets belum terhubung."})
		}
		_, err := sheetsService.Spreadsheets.Get(cfg.Sheet.SpreadsheetID).Do()
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Gagal koneksi Sheets: " + err.Error()})
		}
		return c.JSON(fiber.Map{"success": true, "message": "Koneksi Sheets Berhasil! Available Sheets:\n" + sheetSchema})
	})

	// User Info
	api.Get("/me", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		isAdmin := c.Locals("isAdmin").(bool)
		tenantID := c.Locals("tenantID").(int)
		var username string
		authDB.QueryRow("SELECT username FROM users WHERE id = ? AND tenant_id = ?", userID, tenantID).Scan(&username)

		return c.JSON(fiber.Map{
			"id":       userID,
			"username": username,
			"is_admin": isAdmin,
		})
	})

	api.Get("/profile", func(c *fiber.Ctx) error {
		return c.Redirect("/api/me")
	})

	api.Post("/profile", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}
		if req.Username == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Username cannot be empty"})
		}
		var count int
		tenantID := c.Locals("tenantID").(int)
		authDB.QueryRow("SELECT COUNT(*) FROM users WHERE username = ? AND id != ? AND tenant_id = ?", req.Username, userID, tenantID).Scan(&count)
		if count > 0 {
			return c.Status(400).JSON(fiber.Map{"error": "Username already taken"})
		}

		if req.Password != "" {
			// Hash password before updating
			hashedPassword, err := hashPassword(req.Password)
			if err != nil {
				log.Println("Failed to hash password during profile update:", err)
				return c.Status(500).JSON(fiber.Map{"error": "Failed to process password"})
			}
			authDB.Exec("UPDATE users SET username = ?, password = ? WHERE id = ? AND tenant_id = ?", req.Username, hashedPassword, userID, tenantID)
		} else {
			authDB.Exec("UPDATE users SET username = ? WHERE id = ? AND tenant_id = ?", req.Username, userID, tenantID)
		}
		return c.JSON(fiber.Map{"success": true})
	})

	// --- Device Management (New Multi-Device) ---

	// List Devices
	api.Get("/device/list", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)

		// Get from DB
		tenantIDVal := c.Locals("tenantID")
		var tenantID int
		if tenantIDVal != nil {
			tenantID = tenantIDVal.(int)
		} else {
			// Default to tenant 1 if not set (should not happen due to middleware)
			tenantID = 1
		}

		rows, err := authDB.Query("SELECT device_jid, alias, status, is_primary FROM user_devices WHERE user_id = ? AND tenant_id = ?", userID, tenantID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Database error"})
		}
		defer rows.Close()

		var devices []fiber.Map
		for rows.Next() {
			var jid, alias, status string
			var isPrimary bool
			if err := rows.Scan(&jid, &alias, &status, &isPrimary); err == nil {
				// Check real-time status from memory
				clientMutex.Lock()
				if userStatuses[userID] != nil {
					if realStatus, ok := userStatuses[userID][jid]; ok {
						status = realStatus
					}
				}
				clientMutex.Unlock()

				devices = append(devices, fiber.Map{
					"jid":        jid,
					"alias":      alias,
					"status":     status,
					"is_primary": isPrimary,
				})
			}
		}

		// Also check for "NEW" (pending QR)
		clientMutex.Lock()
		if userClients[userID] != nil {
			if _, ok := userClients[userID]["NEW"]; ok {
				status := "STARTING"
				if s, ok := userStatuses[userID]["NEW"]; ok {
					status = s
				}

				// Only show "NEW" if it is NOT connected yet.
				// If connected, the real device JID is already in the DB list above.
				if status != "CONNECTED" {
					qr := ""
					if q, ok := userQRCodes[userID]["NEW"]; ok {
						qr = q
					}
					devices = append(devices, fiber.Map{
						"jid":    "NEW",
						"alias":  "Pairing New Device...",
						"status": status,
						"qr":     qr,
					})
				}
			}
		}
		clientMutex.Unlock()

		return c.JSON(fiber.Map{"success": true, "devices": devices})
	})

	// Get QR for New Device
	api.Get("/device/qr", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)

		clientMutex.Lock()
		defer clientMutex.Unlock()

		if userClients[userID] != nil {
			if _, ok := userClients[userID]["NEW"]; ok {
				status := "STARTING"
				if s, ok := userStatuses[userID]["NEW"]; ok {
					status = s
				}
				qr := ""
				if q, ok := userQRCodes[userID]["NEW"]; ok {
					qr = q
				}

				return c.JSON(fiber.Map{
					"success": true,
					"qr":      qr,
					"status":  status,
					"message": status,
				})
			}
		}

		return c.JSON(fiber.Map{"success": false, "message": "No pairing in progress"})
	})

	// Add Device (Start New Session)
	api.Post("/device/add", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)

		// Force cleanup existing "NEW" session to ensure fresh QR
		clientMutex.Lock()
		if cli, ok := userClients[userID]["NEW"]; ok && cli != nil {
			go func(c *whatsmeow.Client) {
				c.Disconnect()
			}(cli)
			delete(userClients[userID], "NEW")
			delete(userStatuses[userID], "NEW")
			delete(userQRCodes[userID], "NEW")
		}
		clientMutex.Unlock()

		tenantID := c.Locals("tenantID").(int)
		go startUserDevice(userID, "", tenantID) // Empty string = New Device
		return c.JSON(fiber.Map{"success": true, "message": "Initializing new device pairing..."})
	})

	// Delete Device
	api.Delete("/device/:jid", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		rawJid := c.Params("jid")

		jidStr, err := url.QueryUnescape(rawJid)
		if err != nil {
			log.Println("Error decoding JID:", err)
			jidStr = rawJid // Fallback
		}

		clientMutex.Lock()
		// If it's a running client, logout and disconnect
		if userClients[userID] != nil {
			if cli, ok := userClients[userID][jidStr]; ok && cli != nil {
				go func(client *whatsmeow.Client) {
					client.Logout(context.Background())
					client.Disconnect()
				}(cli)
				delete(userClients[userID], jidStr)
				if userStatuses[userID] != nil {
					delete(userStatuses[userID], jidStr)
				}
				if userQRCodes[userID] != nil {
					delete(userQRCodes[userID], jidStr)
				}
			}
		}
		clientMutex.Unlock()

		log.Printf("Deleting device from DB: UserID=%d, JID=%s", userID, jidStr)

		// Remove from DB
		tenantID := c.Locals("tenantID").(int)
		authDB.Exec("DELETE FROM user_devices WHERE user_id = ? AND device_jid = ? AND tenant_id = ?", userID, jidStr, tenantID)

		return c.JSON(fiber.Map{"success": true})
	})

	// Set Primary Device
	api.Post("/device/:jid/primary", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		rawJid := c.Params("jid")

		jidStr, err := url.QueryUnescape(rawJid)
		if err != nil {
			jidStr = rawJid
		}

		// 1. Reset all for this user
		_, err = authDB.Exec("UPDATE user_devices SET is_primary = 0 WHERE user_id = ? AND tenant_id = ?", userID, tenantID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": err.Error()})
		}

		// 2. Set new primary
		_, err = authDB.Exec("UPDATE user_devices SET is_primary = 1 WHERE user_id = ? AND device_jid = ? AND tenant_id = ?", userID, jidStr, tenantID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": err.Error()})
		}

		return c.JSON(fiber.Map{"success": true})
	})

	// Chat Contacts
	api.Get("/chat-contacts", func(c *fiber.Ctx) error {
		// Return all for now, ideally filter by user
		historyMutex.Lock()
		defer historyMutex.Unlock()
		contacts := []string{}
		for jid := range chatHistories {
			contacts = append(contacts, jid)
		}
		return c.JSON(fiber.Map{"success": true, "contacts": contacts})
	})

	// Test AI
	api.Post("/test-ai", func(c *fiber.Ctx) error {
		var req struct {
			Prompt  string `json:"prompt"`
			Message string `json:"message"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid JSON"})
		}
		prompt := req.Prompt
		if prompt == "" {
			prompt = req.Message
		}
		tenantID := c.Locals("tenantID").(int)
		reply := callAI(tenantID, prompt)
		return c.JSON(fiber.Map{"success": true, "reply": reply})
	})

	// Delete Connections
	api.Post("/connections/sheet/delete", func(c *fiber.Ctx) error {
		cfg.Sheet = SheetConfig{} // Clear config
		saveConfig()
		sheetsService = nil
		sheetSchema = ""
		return c.JSON(fiber.Map{"success": true, "message": "Google Sheets connection removed"})
	})

	api.Post("/connections/mysql/delete", func(c *fiber.Ctx) error {
		cfg.Database = DBConfig{Type: "postgres", Host: "localhost", Port: "5432"} // Reset to default
		saveConfig()
		appDB = nil
		dbSchema = ""
		return c.JSON(fiber.Map{"success": true, "message": "Database connection removed"})
	})

	// Send Message API (User Specific)
	api.Post("/send-message", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		var req struct {
			Phone   string `json:"phone"`
			Message string `json:"message"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid JSON"})
		}

		if req.Phone == "" || req.Message == "" {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Phone and Message required"})
		}

		jid := req.Phone
		if !strings.Contains(jid, "@s.whatsapp.net") {
			jid = strings.ReplaceAll(jid, "+", "")
			jid = strings.ReplaceAll(jid, "-", "")
			jid = strings.ReplaceAll(jid, " ", "")
			if strings.HasPrefix(jid, "08") {
				jid = "62" + jid[1:]
			}
			jid = jid + "@s.whatsapp.net"
		}

		remoteJID, err := types.ParseJID(jid)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid Phone Number"})
		}

		// Use User's Client
		cli := getUserClient(userID)
		if cli == nil || !cli.IsConnected() {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "WhatsApp Anda belum terhubung. Silakan scan QR code di dashboard."})
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, err = cli.SendMessage(ctx, remoteJID, &waE2E.Message{
			Conversation: proto.String(req.Message),
		})

		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Failed to send message: " + err.Error()})
		}

		return c.JSON(fiber.Map{"success": true})
	})

	// --- USER MANAGEMENT ROUTES (Admin Only) ---
	userGroup := api.Group("/users")

	userGroup.Get("/", func(c *fiber.Ctx) error {
		if isAdmin, ok := c.Locals("isAdmin").(bool); !ok || !isAdmin {
			return c.Status(403).JSON(fiber.Map{"error": "Requires Admin privileges"})
		}
		tenantID := c.Locals("tenantID").(int)
		rows, err := authDB.Query("SELECT id, username, is_admin, is_active FROM users WHERE tenant_id = ? ORDER BY id DESC", tenantID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Database error"})
		}
		defer rows.Close()
		var users []User
		for rows.Next() {
			var u User
			if err := rows.Scan(&u.ID, &u.Username, &u.IsAdmin, &u.IsActive); err == nil {
				users = append(users, u)
			}
		}
		return c.JSON(fiber.Map{"users": users})
	})

	userGroup.Post("/", func(c *fiber.Ctx) error {
		if isAdmin, ok := c.Locals("isAdmin").(bool); !ok || !isAdmin {
			return c.Status(403).JSON(fiber.Map{"error": "Requires Admin privileges"})
		}
		tenantID := c.Locals("tenantID").(int)
		var req User
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}
		if req.Username == "" || req.Password == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Username and Password required"})
		}
		hashedPassword, err := hashPassword(req.Password)
		if err != nil {
			log.Println("Failed to hash admin-created password:", err)
			return c.Status(500).JSON(fiber.Map{"error": "Failed to process password"})
		}
		_, err = authDB.Exec("INSERT INTO users (username, password, tenant_id, is_admin, is_active) VALUES (?, ?, ?, ?, ?)",
			req.Username, hashedPassword, tenantID, req.IsAdmin, req.IsActive)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Username already exists or Database error"})
		}
		return c.JSON(fiber.Map{"success": true})
	})

	userGroup.Put("/:id", func(c *fiber.Ctx) error {
		if isAdmin, ok := c.Locals("isAdmin").(bool); !ok || !isAdmin {
			return c.Status(403).JSON(fiber.Map{"error": "Requires Admin privileges"})
		}
		id := c.Params("id")
		tenantID := c.Locals("tenantID").(int)
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
			IsAdmin  bool   `json:"is_admin"`
			IsActive bool   `json:"is_active"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}
		if req.Password != "" {
			hashedPassword, err := hashPassword(req.Password)
			if err != nil {
				log.Println("Failed to hash admin-updated password:", err)
				return c.Status(500).JSON(fiber.Map{"error": "Failed to process password"})
			}
			authDB.Exec("UPDATE users SET username = ?, password = ?, is_admin = ?, is_active = ? WHERE id = ? AND tenant_id = ?",
				req.Username, hashedPassword, req.IsAdmin, req.IsActive, id, tenantID)
		} else {
			authDB.Exec("UPDATE users SET username = ?, is_admin = ?, is_active = ? WHERE id = ? AND tenant_id = ?",
				req.Username, req.IsAdmin, req.IsActive, id, tenantID)
		}
		return c.JSON(fiber.Map{"success": true})
	})

	userGroup.Delete("/:id", func(c *fiber.Ctx) error {
		if isAdmin, ok := c.Locals("isAdmin").(bool); !ok || !isAdmin {
			return c.Status(403).JSON(fiber.Map{"error": "Requires Admin privileges"})
		}
		id := c.Params("id")
		myID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		idInt, _ := strconv.Atoi(id)
		if idInt == myID {
			return c.Status(400).JSON(fiber.Map{"error": "Cannot delete yourself"})
		}
		authDB.Exec("DELETE FROM users WHERE id = ? AND tenant_id = ?", id, tenantID)
		return c.JSON(fiber.Map{"success": true})
	})

	api.Get("/models", fetchModelsHandler)
	api.Get("/version", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"success":    true,
			"version":    "1.2.0-MultiUser",
			"build_time": "2026-02-09",
			"features":   []string{"multi-device", "otp-login", "admin-bypass"},
		})
	})

	log.Println("Wahaku Service Starting...")
	log.Println("Version: 1.2.0-MultiUser")
	log.Println("Server running on http://localhost:" + cfg.AppPort)
	log.Fatal(app.Listen(":" + cfg.AppPort))
}

// --- MULTI USER LOGIC ---

func initAdminClient() {
	// Initialize admin clients for all tenants
	rows, err := authDB.Query("SELECT id FROM tenants")
	if err != nil {
		log.Println("Error querying tenants:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tenantID int
		if err := rows.Scan(&tenantID); err != nil {
			continue
		}

		// Find Admin ID for this tenant
		var adminID int
		err := authDB.QueryRow("SELECT id FROM users WHERE is_admin = 1 AND tenant_id = ? LIMIT 1", tenantID).Scan(&adminID)
		if err != nil {
			log.Printf("Admin not found for tenant %d\n", tenantID)
			continue
		}

		log.Printf("Initializing Admin Bot for Tenant %d (UserID: %d)\n", tenantID, adminID)
		startAllUserDevices(adminID, tenantID)
	}
}

func getSystemBot(tenantID int) *whatsmeow.Client {
	// Return Admin's client for OTP sending
	var adminID int
	err := authDB.QueryRow("SELECT id FROM users WHERE is_admin = 1 AND tenant_id = ? LIMIT 1", tenantID).Scan(&adminID)
	if err != nil {
		return nil
	}

	// 1. Try Primary Device
	var primaryJID string
	err = authDB.QueryRow("SELECT device_jid FROM user_devices WHERE user_id = ? AND is_primary = 1 AND tenant_id = ?", adminID, tenantID).Scan(&primaryJID)
	if err == nil && primaryJID != "" {
		if cli := getUserDeviceClient(adminID, primaryJID); cli != nil {
			return cli
		}
	}

	// 2. Fallback to any connected device
	return getUserClient(adminID)
}

func getUserDeviceClient(userID int, jid string) *whatsmeow.Client {
	clientMutex.Lock()
	defer clientMutex.Unlock()
	if clients, ok := userClients[userID]; ok {
		if cli, ok := clients[jid]; ok && cli != nil && cli.IsConnected() {
			return cli
		}
	}
	return nil
}

func getUserClient(userID int) *whatsmeow.Client {
	clientMutex.Lock()
	defer clientMutex.Unlock()

	if devices, ok := userClients[userID]; ok {
		for _, cli := range devices {
			if cli != nil && cli.IsConnected() {
				return cli
			}
		}
	}
	return nil
}

func startAllUserDevices(userID int, tenantID int) {
	rows, err := authDB.Query("SELECT device_jid FROM user_devices WHERE user_id = ? AND tenant_id = ?", userID, tenantID)
	if err != nil {
		log.Println("Error querying user devices:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var jidStr string
		if err := rows.Scan(&jidStr); err == nil {
			go startUserDevice(userID, jidStr, tenantID)
		}
	}
}

func startUserDevice(userID int, deviceJIDStr string, tenantID int) *whatsmeow.Client {
	// 1. Initialize maps if needed
	clientMutex.Lock()
	if userClients[userID] == nil {
		userClients[userID] = make(map[string]*whatsmeow.Client)
		userQRCodes[userID] = make(map[string]string)
		userStatuses[userID] = make(map[string]string)
	}

	// Check if already running
	key := deviceJIDStr
	if key == "" {
		key = "NEW"
	}

	if cli, ok := userClients[userID][key]; ok && cli != nil {
		clientMutex.Unlock()
		return cli
	}
	clientMutex.Unlock()

	// 2. Load or Create Device Store
	var deviceStore *store.Device
	var err error

	if deviceJIDStr != "" {
		// Load existing device
		jid, _ := types.ParseJID(deviceJIDStr)
		deviceStore, err = container.GetDevice(context.Background(), jid)
		if err != nil {
			log.Println("Failed to get device from store:", err)
			return nil
		}
	} else {
		// New Device
		deviceStore = container.NewDevice()
	}

	if deviceStore == nil {
		return nil
	}

	// 3. Create Client
	logTag := fmt.Sprintf("Client-%d", userID)
	if deviceJIDStr != "" {
		logTag += "-" + deviceJIDStr
	}
	clientLog := waLog.Stdout(logTag, "INFO", true)
	cli := whatsmeow.NewClient(deviceStore, clientLog)

	// 4. Add Event Handler (Closure to capture userID and client)
	cli.AddEventHandler(func(evt interface{}) {
		handleUserEvent(userID, cli, evt)
	})

	// 5. Save to Map
	clientMutex.Lock()
	userClients[userID][key] = cli
	userStatuses[userID][key] = "STARTING"
	clientMutex.Unlock()

	// 6. Connect
	go func() {
		if cli.Store.ID == nil {
			// Need QR
			clientMutex.Lock()
			userStatuses[userID][key] = "QR_READY"
			clientMutex.Unlock()

			// Get QR Channel
			qrChan, _ := cli.GetQRChannel(context.Background())
			if err := cli.Connect(); err != nil {
				log.Println("Connect Error for user", userID, ":", err)
				return
			}

			for evt := range qrChan {
				if evt.Event == "code" {
					clientMutex.Lock()
					if userQRCodes[userID] == nil {
						userQRCodes[userID] = make(map[string]string)
					}
					userQRCodes[userID][key] = evt.Code
					userStatuses[userID][key] = "QR_READY"
					clientMutex.Unlock()
					log.Println("QR Generated for User", userID, "Key:", key)
				}
			}
		} else {
			// Already paired
			clientMutex.Lock()
			userStatuses[userID][key] = "CONNECTING"
			clientMutex.Unlock()

			if err := cli.Connect(); err != nil {
				log.Println("Connect Error for user", userID, ":", err)
				clientMutex.Lock()
				userStatuses[userID][key] = "DISCONNECTED"
				clientMutex.Unlock()
			} else {
				clientMutex.Lock()
				userStatuses[userID][key] = "CONNECTED"
				clientMutex.Unlock()
			}
		}
	}()

	return cli
}

func handleUserEvent(userID int, cli *whatsmeow.Client, evt interface{}) {
	// Determine the key (JID or "NEW")
	var key string
	if cli.Store.ID == nil {
		key = "NEW"
	} else {
		key = cli.Store.ID.String()
	}

	switch v := evt.(type) {
	case *events.Message:
		if v.Info.IsFromMe || v.Info.IsGroup || v.Info.Sender.User == "status" {
			return
		}
		msg := v.Message.GetConversation()
		if msg == "" {
			msg = v.Message.GetExtendedTextMessage().GetText()
		}
		if msg == "" {
			return
		}

		log.Printf("[User %d][%s] Received: %s", userID, key, msg)

		// History
		chatID := v.Info.Chat.String()
		userChatKey := fmt.Sprintf("%d:%s", userID, chatID)

		historyMutex.Lock()
		chatHistories[userChatKey] = append(chatHistories[userChatKey], "User: "+msg)
		if len(chatHistories[userChatKey]) > 20 {
			chatHistories[userChatKey] = chatHistories[userChatKey][len(chatHistories[userChatKey])-20:]
		}
		historyMutex.Unlock()

		// AI Reply
		go func() {
			log.Printf("[User %d] Received message: %s", userID, msg)

			// Get tenant ID for this user
			var tenantID int
			err := authDB.QueryRow("SELECT tenant_id FROM users WHERE id = ?", userID).Scan(&tenantID)
			if err != nil {
				log.Println("Error getting tenant for user", userID, ":", err)
				tenantID = 1 // fallback
			}

			// Send typing indicator to WhatsApp
			ctxTyping, cancelTyping := context.WithTimeout(context.Background(), 5*time.Second)
			if err := cli.SendChatPresence(ctxTyping, v.Info.Chat, types.ChatPresenceComposing, types.ChatPresenceMediaText); err != nil {
				log.Printf("[User %d] Failed to send typing indicator: %v", userID, err)
			} else {
				log.Printf("[User %d] Typing indicator sent", userID)
			}
			cancelTyping()

			// Add random delay to mimic human typing (5-30 seconds) and reduce ban risk
			delaySeconds := time.Duration(rand.Intn(25)+5) * time.Second
			log.Printf("[User %d] Waiting %v before replying...", userID, delaySeconds)
			time.Sleep(delaySeconds)

			reply := callAI(tenantID, msg)
			log.Printf("[User %d] AI Reply: %s", userID, reply)

			historyMutex.Lock()
			chatHistories[userChatKey] = append(chatHistories[userChatKey], "Assistant: "+reply)
			historyMutex.Unlock()

		// Reply using the SAME client that received the message
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, sendErr := cli.SendMessage(ctx, v.Info.Chat, &waE2E.Message{Conversation: &reply})
		if sendErr != nil {
			log.Printf("[User %d] Failed to send reply: %v", userID, sendErr)
		} else {
			log.Printf("[User %d] Message sent successfully to %s", userID, v.Info.Chat.User)
		}

			ctxStop, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
			if err := cli.SendChatPresence(ctxStop, v.Info.Chat, types.ChatPresencePaused, types.ChatPresenceMediaText); err != nil {
				log.Printf("[User %d] Failed to stop typing indicator: %v", userID, err)
			}
			cancelStop()
		}()

	case *events.Connected:
		clientMutex.Lock()
		if userStatuses[userID] == nil {
			userStatuses[userID] = make(map[string]string)
		}
		userStatuses[userID][key] = "CONNECTED"
		// Clear QR code just in case
		if userQRCodes[userID] != nil {
			delete(userQRCodes[userID], key)
		}
		clientMutex.Unlock()
		log.Printf("User %d Device %s Connected", userID, key)

	case *events.Disconnected:
		clientMutex.Lock()
		if userStatuses[userID] == nil {
			userStatuses[userID] = make(map[string]string)
		}
		userStatuses[userID][key] = "DISCONNECTED"
		clientMutex.Unlock()

	case *events.PairSuccess:
		log.Printf("User %d Pair Success: %s", userID, v.ID.String())
		newJID := v.ID.String()

		// Get tenant ID for this user
		var tenantID int
		err := authDB.QueryRow("SELECT tenant_id FROM users WHERE id = ?", userID).Scan(&tenantID)
		if err != nil {
			log.Println("Error getting tenant for user", userID, ":", err)
			tenantID = 1 // fallback
		}

		// 1. Check if device already exists for this user
		var exists int
		err = authDB.QueryRow("SELECT COUNT(*) FROM user_devices WHERE user_id = ? AND device_jid = ? AND tenant_id = ?", userID, newJID, tenantID).Scan(&exists)
		if err != nil {
			log.Println("Error checking device existence:", err)
		}

		if exists > 0 {
			log.Printf("Device %s already exists for user %d. Updating status instead of inserting.", newJID, userID)
			_, err = authDB.Exec("UPDATE user_devices SET status = 'CONNECTED', alias = 'WhatsApp Device' WHERE user_id = ? AND device_jid = ? AND tenant_id = ?", userID, newJID, tenantID)
			if err != nil {
				log.Println("Error updating existing device:", err)
			}
		} else {
			// 2. Save to DB if not exists
			_, err = authDB.Exec("INSERT INTO user_devices (tenant_id, user_id, device_jid, alias, status) VALUES (?, ?, ?, ?, ?)",
				tenantID, userID, newJID, "WhatsApp Device", "CONNECTED")
			if err != nil {
				log.Println("Error saving new device to DB:", err)
			}
		}

		// 2. Update Maps: Add real JID, update NEW status to CONNECTED
		clientMutex.Lock()
		// Mark NEW as CONNECTED so frontend polling finishes
		if _, ok := userClients[userID]["NEW"]; ok {
			userStatuses[userID]["NEW"] = "CONNECTED"
			// We do NOT delete "NEW" here to ensure polling succeeds.
			// It will be cleaned up next time /device/add is called.
		}
		userClients[userID][newJID] = cli
		userStatuses[userID][newJID] = "CONNECTED"
		clientMutex.Unlock()
	}
}

func callAI(tenantID int, prompt string) string {
	const MaxInputLength = 4000
	if len(prompt) > MaxInputLength {
		prompt = prompt[:MaxInputLength]
	}

	// 1. Get Knowledge for tenant
	knowledgeMutex.RLock()
	contextText := tenantKnowledge[tenantID]
	knowledgeMutex.RUnlock()

	if len(contextText) > 15000 {
		contextText = contextText[:15000]
	}

	// 2. Build System Prompt (use global config)
	mu.Lock()
	sysPrompt := cfg.SystemPrompt
	mu.Unlock()
	if sysPrompt == "" {
		sysPrompt = "You are a helpful assistant."
	}

	fullPrompt := fmt.Sprintf("%s\n\nContext:\n%s\n\nUser Question: %s", sysPrompt, contextText, prompt)

	// 3. Choose Provider (global config)
	mu.Lock()
	providerName := cfg.ActiveProvider
	mu.Unlock()
	if providerName == "" {
		providerName = "gemini" // Default
	}

	pConfig, ok := cfg.Providers[providerName]
	if !ok || pConfig.APIKey == "" {
		return "Error: AI Provider not configured."
	}

	// 4. Call API
	switch providerName {
	case "gemini":
		return callGemini(pConfig.APIKey, pConfig.Model, pConfig.BaseURL, fullPrompt)
	case "openai":
		return callOpenAI(pConfig.APIKey, pConfig.Model, pConfig.BaseURL, fullPrompt)
	case "sumopod":
		return callSumopod(pConfig.APIKey, pConfig.Model, pConfig.BaseURL, fullPrompt)
	case "deepseek":
		return callDeepSeek(pConfig.APIKey, pConfig.Model, pConfig.BaseURL, fullPrompt)
	case "byteplus":
		return callBytePlus(pConfig.APIKey, pConfig.Model, pConfig.BaseURL, fullPrompt)
	}

	return "Error: Unknown Provider: " + providerName
}

// --- AI Providers ---

func callGemini(apiKey, model, baseURL, prompt string) string {
	if baseURL == "" {
		baseURL = "https://generativelanguage.googleapis.com/v1beta"
	}
	// Remove trailing slash
	baseURL = strings.TrimSuffix(baseURL, "/")

	url := fmt.Sprintf("%s/models/%s:generateContent?key=%s", baseURL, model, apiKey)

	requestBody, _ := json.Marshal(map[string]interface{}{
		"contents": []map[string]interface{}{
			{"parts": []map[string]string{{"text": prompt}}},
		},
	})

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return "Error calling Gemini: " + err.Error()
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if candidates, ok := result["candidates"].([]interface{}); ok && len(candidates) > 0 {
		if content, ok := candidates[0].(map[string]interface{})["content"].(map[string]interface{}); ok {
			if parts, ok := content["parts"].([]interface{}); ok && len(parts) > 0 {
				return parts[0].(map[string]interface{})["text"].(string)
			}
		}
	}
	// Check for error response
	if errResp, ok := result["error"].(map[string]interface{}); ok {
		return "Gemini Error: " + fmt.Sprintf("%v", errResp["message"])
	}

	return "Error: No response from Gemini"
}

func callOpenAI(apiKey, model, baseURL, prompt string) string {
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	url := baseURL + "/chat/completions"

	requestBody, _ := json.Marshal(map[string]interface{}{
		"model": model,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	})

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "Error calling OpenAI: " + err.Error()
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if errVal, ok := result["error"]; ok {
		if errMsg, ok := errVal.(map[string]interface{})["message"].(string); ok {
			return "OpenAI Error: " + errMsg
		}
		return "OpenAI Error: Unknown"
	}

	if choices, ok := result["choices"].([]interface{}); ok && len(choices) > 0 {
		if message, ok := choices[0].(map[string]interface{})["message"].(map[string]interface{}); ok {
			return message["content"].(string)
		}
	}
	return "Error: No response from OpenAI"
}

func callSumopod(apiKey, model, baseURL, prompt string) string {
	if baseURL == "" {
		baseURL = "https://ai.sumopod.com/v1"
	}
	return callOpenAI(apiKey, model, baseURL, prompt)
}

func callDeepSeek(apiKey, model, baseURL, prompt string) string {
	if baseURL == "" {
		baseURL = "https://api.deepseek.com"
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	url := baseURL + "/chat/completions"

	requestBody, _ := json.Marshal(map[string]interface{}{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": "You are a helpful assistant."},
			{"role": "user", "content": prompt},
		},
		"stream": false,
	})

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "Error calling DeepSeek: " + err.Error()
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if errVal, ok := result["error"]; ok {
		if errMsg, ok := errVal.(map[string]interface{})["message"].(string); ok {
			return "DeepSeek Error: " + errMsg
		}
		return "DeepSeek Error: Unknown"
	}

	if choices, ok := result["choices"].([]interface{}); ok && len(choices) > 0 {
		if message, ok := choices[0].(map[string]interface{})["message"].(map[string]interface{}); ok {
			return message["content"].(string)
		}
	}
	return "Error: No response from DeepSeek"
}

func callBytePlus(apiKey, model, baseURL, prompt string) string {
	if baseURL == "" {
		baseURL = "https://ark.ap-southeast.bytepluses.com/api/v3"
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	url := baseURL + "/chat/completions"

	requestBody, _ := json.Marshal(map[string]interface{}{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": "You are a helpful assistant."},
			{"role": "user", "content": prompt},
		},
	})

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "Error calling BytePlus: " + err.Error()
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if errVal, ok := result["error"]; ok {
		if errMsg, ok := errVal.(map[string]interface{})["message"].(string); ok {
			return "BytePlus Error: " + errMsg
		}
		return "BytePlus Error: Unknown"
	}

	if choices, ok := result["choices"].([]interface{}); ok && len(choices) > 0 {
		if message, ok := choices[0].(map[string]interface{})["message"].(map[string]interface{}); ok {
			return message["content"].(string)
		}
	}
	return "Error: No response from BytePlus"
}

// Placeholder for other functions like loadConfig, saveConfig, connectAppDB, etc.
// Since we are rewriting main.go, we must include them or ensure they are present.
// I will include the missing helper functions below to ensure the file is complete.

func loadConfig() {
	file, err := os.Open(configFile)
	if err != nil {
		// Default Config
		cfg = Config{
			AppPort:       "4500",
			AdminUsername: "admin",
			AdminPassword: "password",
			Providers:     make(map[string]ProviderConfig),
			SavedPrompts:  make(map[string]string),
		}
		saveConfig()
		return
	}
	defer file.Close()
	json.NewDecoder(file).Decode(&cfg)

	// Ensure defaults
	if cfg.AppPort == "" {
		cfg.AppPort = "4500"
	}
	if cfg.Providers == nil {
		cfg.Providers = make(map[string]ProviderConfig)
	}
}

func saveConfig() {
	data, _ := json.MarshalIndent(cfg, "", "  ")
	os.WriteFile(configFile, data, 0644)
}

// overlayEnvConfig overrides configuration with environment variables for security
func overlayEnvConfig() {
	// Provider API Keys from environment variables
	envMappings := map[string]struct {
		provider string
		field    string
		envVar   string
	}{
		"byteplus": {provider: "byteplus", field: "api_key", envVar: "BYTEPLUS_API_KEY"},
		"openai":   {provider: "openai", field: "api_key", envVar: "OPENAI_API_KEY"},
		"sumopod":  {provider: "sumopod", field: "api_key", envVar: "SUMOPOD_API_KEY"},
		"gemini":   {provider: "gemini", field: "api_key", envVar: "GEMINI_API_KEY"},
		"groq":     {provider: "groq", field: "api_key", envVar: "GROQ_API_KEY"},
		"qwen":     {provider: "qwen", field: "api_key", envVar: "QWEN_API_KEY"},
		"deepseek": {provider: "deepseek", field: "api_key", envVar: "DEEPSEEK_API_KEY"},
	}

	for _, mapping := range envMappings {
		if val, exists := os.LookupEnv(mapping.envVar); exists {
			if p, ok := cfg.Providers[mapping.provider]; ok {
				p.APIKey = val
				cfg.Providers[mapping.provider] = p
				log.Printf("API key for %s loaded from environment variable %s", mapping.provider, mapping.envVar)
			}
		}
	}

	// Google Service Account JSON from env var (for Vertex/Gemini)
	if saJSON := os.Getenv("GOOGLE_SERVICE_ACCOUNT_JSON"); saJSON != "" {
		if p, ok := cfg.Providers["vertex"]; ok {
			p.APIKey = saJSON
			cfg.Providers["vertex"] = p
		}
	}

	// Admin credentials from environment (optional)
	if adminUser := os.Getenv("ADMIN_USERNAME"); adminUser != "" {
		cfg.AdminUsername = adminUser
	}
	if adminPass := os.Getenv("ADMIN_PASSWORD"); adminPass != "" {
		cfg.AdminPassword = adminPass
	}
}

func connectAppDB() {
	if !cfg.Database.Enabled {
		return
	}
	if cfg.Database.Type == "" {
		return
	}

	var dsn string
	var driver string

	if cfg.Database.Type == "mysql" {
		driver = "mysql"
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", cfg.Database.User, cfg.Database.Password, cfg.Database.Host, cfg.Database.Port, cfg.Database.Name)
	} else if cfg.Database.Type == "postgres" {
		driver = "postgres"
		dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", cfg.Database.Host, cfg.Database.Port, cfg.Database.User, cfg.Database.Password, cfg.Database.Name)
	} else {
		return
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		log.Println("App DB Connect Error:", err)
		return
	}
	if err := db.Ping(); err != nil {
		log.Println("App DB Ping Error:", err)
		return
	}

	appDB = db
	log.Println("Connected to App Database (" + cfg.Database.Type + ")")

	// Get Schema
	// Simplified schema fetching
	dbSchema = "Schema fetched."
}

func connectSheets() {
	if cfg.Sheet.CredentialsJSON == "" {
		return
	}

	ctx := context.Background()
	creds := []byte(cfg.Sheet.CredentialsJSON)

	srv, err := sheets.NewService(ctx, option.WithCredentialsJSON(creds))
	if err != nil {
		log.Println("Sheets Connect Error:", err)
		return
	}

	sheetsService = srv
	log.Println("Connected to Google Sheets")

	// Fetch Schema
	sheetSchema = "Sheets connected."
}

// rebuildTenantKnowledge rebuilds the knowledge base for a specific tenant
func rebuildTenantKnowledge(tenantID int) {
	var sb strings.Builder

	// Query knowledge files for this tenant
	rows, err := authDB.Query("SELECT filename FROM tenant_knowledge_files WHERE tenant_id = ?", tenantID)
	if err != nil {
		log.Println("Error querying knowledge files for tenant", tenantID, ":", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err == nil {
			content, err := os.ReadFile("uploads/" + filename)
			if err == nil {
				sb.WriteString("\n--- File: " + filename + " ---\n")
				if strings.HasSuffix(filename, ".pdf") {
					// PDF handling placeholder
				} else {
					sb.Write(content)
				}
			}
		}
	}

	knowledgeMutex.Lock()
	tenantKnowledge[tenantID] = sb.String()
	knowledgeMutex.Unlock()
	log.Printf("Tenant %d knowledge updated. Total chars: %d\n", tenantID, len(sb.String()))
}

// refreshKnowledge rebuilds knowledge for all tenants
func refreshKnowledge() {
	// Get all tenant IDs
	rows, err := authDB.Query("SELECT id FROM tenants")
	if err != nil {
		log.Println("Error fetching tenants for knowledge refresh:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tenantID int
		if err := rows.Scan(&tenantID); err == nil {
			rebuildTenantKnowledge(tenantID)
		}
	}
}

func processFollowups() {
	for {
		time.Sleep(1 * time.Minute)

		rows, err := authDB.Query("SELECT id, user_id, tenant_id, jid, instruction FROM followup_tasks WHERE status = 'pending' AND scheduled_time <= datetime('now')")
		if err != nil {
			log.Println("Scheduler Error:", err)
			continue
		}

		var tasks []FollowupTask
		for rows.Next() {
			var t FollowupTask
			if err := rows.Scan(&t.ID, &t.UserID, &t.TenantID, &t.JID, &t.Instruction); err == nil {
				tasks = append(tasks, t)
			}
		}
		rows.Close()

		for _, t := range tasks {
			log.Println("Processing Followup Task:", t.ID)

		// Generate Message
		reply := callAI(t.TenantID, "Generate a follow-up message for: " + t.Instruction)

			// Send
			cli := getUserClient(t.UserID)
			if cli != nil && cli.IsConnected() {
				remoteJID, _ := types.ParseJID(t.JID)
				cli.SendMessage(context.Background(), remoteJID, &waE2E.Message{Conversation: &reply})

				authDB.Exec("UPDATE followup_tasks SET status = 'completed' WHERE id = ?", t.ID)
			} else {
				log.Println("User client not connected for task:", t.ID)
				// Retry later? Or mark failed?
			}
		}
	}
}

func fetchModelsHandler(c *fiber.Ctx) error {
	provider := c.Query("provider")
	apiKey := c.Query("api_key")
	baseURL := c.Query("base_url")

	if apiKey == "" && provider != "ollama" {
		return c.Status(400).JSON(fiber.Map{"error": "API Key required"})
	}

	var models []string
	var err error

	switch provider {
	case "gemini":
		models, err = fetchGeminiModels(apiKey, baseURL)
	case "openai", "sumopod", "groq", "deepseek", "byteplus", "qwen":
		models, err = fetchOpenAICompatibleModels(apiKey, baseURL, provider)
	case "vertex":
		// Vertex requires complex auth (OAuth2 token), skipping for now.
		// Frontend has hardcoded fallback.
		return c.JSON(fiber.Map{"models": []string{}})
	default:
		return c.Status(400).JSON(fiber.Map{"error": "Unknown provider"})
	}

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	if provider == "sumopod" {
		details, detailErr := fetchSumopodModelDetails(apiKey, baseURL, models)
		if detailErr != nil {
			log.Println("Sumopod model details unavailable:", detailErr)
		}
		return c.JSON(fiber.Map{"models": models, "model_details": details})
	}

	return c.JSON(fiber.Map{"models": models})
}

func fetchGeminiModels(apiKey, baseURL string) ([]string, error) {
	if baseURL == "" {
		baseURL = "https://generativelanguage.googleapis.com/v1beta"
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	url := fmt.Sprintf("%s/models?key=%s", baseURL, apiKey)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API Error: %s", string(body))
	}

	var result struct {
		Models []struct {
			Name                       string   `json:"name"`
			SupportedGenerationMethods []string `json:"supportedGenerationMethods"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	var names []string
	for _, m := range result.Models {
		// Filter: Must support generateContent
		isContentGen := false
		for _, method := range m.SupportedGenerationMethods {
			if method == "generateContent" {
				isContentGen = true
				break
			}
		}

		// Fallback: If supportedGenerationMethods is empty/missing, assume text model if it starts with gemini
		if len(m.SupportedGenerationMethods) == 0 && strings.Contains(strings.ToLower(m.Name), "gemini") {
			isContentGen = true
		}

		if isContentGen {
			// Gemini returns "models/gemini-pro", we want "gemini-pro"
			name := strings.TrimPrefix(m.Name, "models/")
			names = append(names, name)
		}
	}
	return names, nil
}

func fetchSumopodModelDetails(apiKey, baseURL string, modelIDs []string) ([]ModelDetail, error) {
	if baseURL == "" {
		baseURL = "https://ai.sumopod.com/v1"
	}
	baseURL = strings.TrimSuffix(baseURL, "/")

	detailsByID := make(map[string]ModelDetail, len(modelIDs))
	for _, id := range modelIDs {
		detailsByID[id] = ModelDetail{ID: id}
	}

	urls := []string{baseURL + "/model/info", baseURL + "/models"}
	var lastErr error
	for _, endpoint := range urls {
		items, err := fetchModelMetadata(endpoint, apiKey)
		if err != nil {
			lastErr = err
			continue
		}
		for _, item := range items {
			id := firstString(item, "id", "model_name", "model", "name")
			if id == "" {
				if params, ok := item["litellm_params"].(map[string]interface{}); ok {
					id = firstString(params, "model", "model_name")
				}
			}
			if id == "" {
				continue
			}

			detail := detailsByID[id]
			if detail.ID == "" {
				detail.ID = id
			}
			if info, ok := item["model_info"].(map[string]interface{}); ok {
				mergeModelDetail(&detail, info)
			}
			if params, ok := item["litellm_params"].(map[string]interface{}); ok {
				if detail.Provider == "" {
					detail.Provider = firstString(params, "custom_llm_provider", "provider")
				}
			}
			mergeModelDetail(&detail, item)
			detailsByID[id] = detail
		}
		break
	}

	details := make([]ModelDetail, 0, len(modelIDs))
	for _, id := range modelIDs {
		detail := detailsByID[id]
		if detail.ID == "" {
			detail.ID = id
		}
		details = append(details, detail)
	}
	return details, lastErr
}

func fetchModelMetadata(endpoint, apiKey string) ([]map[string]interface{}, error) {
	req, _ := http.NewRequest("GET", endpoint, nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API Error: %s", string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	rawItems, ok := result["data"].([]interface{})
	if !ok {
		rawItems, ok = result["model_info"].([]interface{})
	}
	if !ok {
		rawItems, ok = result["models"].([]interface{})
	}
	if !ok {
		return nil, fmt.Errorf("model metadata response did not contain a model list")
	}

	items := make([]map[string]interface{}, 0, len(rawItems))
	for _, raw := range rawItems {
		if item, ok := raw.(map[string]interface{}); ok {
			items = append(items, item)
		}
	}
	return items, nil
}

func mergeModelDetail(detail *ModelDetail, item map[string]interface{}) {
	if detail.Provider == "" {
		detail.Provider = firstString(item, "provider", "custom_llm_provider", "owned_by")
	}
	if detail.Status == "" {
		detail.Status = firstString(item, "status", "discount", "promotion", "pricing_status")
	}
	if detail.ContextWindow == "" {
		detail.ContextWindow = firstValue(item, "context_window", "max_tokens", "max_input_tokens", "input_token_limit")
	}
	if detail.InputCost == "" {
		detail.InputCost = firstValue(item, "input_cost_per_token", "input_cost_per_1m_tokens", "prompt_cost", "input_price")
	}
	if detail.OutputCost == "" {
		detail.OutputCost = firstValue(item, "output_cost_per_token", "output_cost_per_1m_tokens", "completion_cost", "output_price")
	}
}

func firstString(item map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if val, ok := item[key].(string); ok && val != "" {
			return val
		}
	}
	return ""
}

func firstValue(item map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if val, ok := item[key]; ok && val != nil {
			return fmt.Sprintf("%v", val)
		}
	}
	return ""
}

func fetchOpenAICompatibleModels(apiKey, baseURL, provider string) ([]string, error) {
	if baseURL == "" {
		switch provider {
		case "openai":
			baseURL = "https://api.openai.com/v1"
		case "sumopod":
			baseURL = "https://ai.sumopod.com/v1"
		case "groq":
			baseURL = "https://api.groq.com/openai/v1"
		case "deepseek":
			baseURL = "https://api.deepseek.com"
		case "byteplus":
			baseURL = "https://ark.ap-southeast.bytepluses.com/api/v3"
		case "qwen":
			baseURL = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1"
		}
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	url := baseURL + "/models"

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API Error: %s", string(body))
	}

	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	var names []string
	for _, m := range result.Data {
		// Filter out non-chat models
		id := strings.ToLower(m.ID)
		if strings.Contains(id, "embedding") ||
			strings.Contains(id, "audio") ||
			strings.Contains(id, "tts") ||
			strings.Contains(id, "whisper") ||
			strings.Contains(id, "dall-e") ||
			strings.Contains(id, "moderation") ||
			strings.Contains(id, "babbage") ||
			strings.Contains(id, "davinci") ||
			strings.Contains(id, "ada") ||
			strings.Contains(id, "curie") ||
			strings.Contains(id, "gpt-4-base") ||
			strings.Contains(id, "instruct") ||
			strings.Contains(id, "realtime") ||
			strings.Contains(id, "search") ||
			strings.Contains(id, "similarity") ||
			strings.Contains(id, "classifier") {
			continue
		}
		names = append(names, m.ID)
	}
	return names, nil
}
