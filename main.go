package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"database/sql"

	"golang.org/x/crypto/bcrypt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
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
	BrandingName   string                    `json:"branding_name"`
	BrandingLogo   string                    `json:"branding_logo"`
	BrandingVersion string                   `json:"branding_version"`
	BillingEnabled     bool   `json:"billing_enabled"`
	BillingBankEnabled bool   `json:"billing_bank_enabled"`
	BillingBankName    string `json:"billing_bank_name"`
	BillingBankAccount string `json:"billing_bank_account"`
	BillingBankHolder  string `json:"billing_bank_holder"`
	BillingNotes       string `json:"billing_notes"`
	OTPEnabled     bool                      `json:"otp_enabled"`
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
	mu            sync.Mutex      // Global Mutex (General)
	knowledgeText string          // Combined scraped & file text
	appDB         *sql.DB         // Application Database (MySQL)
	dbSchema      string          // Table schema for AI
	sheetsService *sheets.Service // Google Sheets Service
	sheetSchema   string          // Sheet names & headers for AI
	sessionStore  *session.Store  // Session Store
	authDB        *sql.DB
	authDialect   = "sqlite"
	chatHistories = make(map[string][]string) // Chat History Memory
	historyMutex  sync.Mutex                  // Mutex for Chat History

	// Simple in-memory rate limiter for auth endpoints
	authRateLimitMap = make(map[string][]time.Time)
	authRateLimitMux sync.RWMutex

	// Login failure tracking for account lockout (key: "tenantID:username")
	loginFailMap = make(map[string]loginFailRecord)
	loginFailMux sync.Mutex

	// OTP resend cooldown per user (key: userID)
	otpResendMap = make(map[int]time.Time)
	otpResendMux sync.Mutex

	// Tenant-specific knowledge cache
	tenantKnowledge = make(map[int]string)
	knowledgeMutex  sync.RWMutex
)

// loginFailRecord tracks failed login attempts for account lockout
type loginFailRecord struct {
	Count     int
	LastFail  time.Time
	LockedAt  time.Time
}

const redactedSecret = "__REDACTED__"
const buildVersion = "1.3.0-MultiTenant"
const buildTime = "2026-05-04"

type User struct {
	ID       int    `json:"id"`
	Username string `json:"username"`
	WhatsApp string `json:"whatsapp_number"`
	Timezone string `json:"timezone"`
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
	// Recurring configuration
	RepeatType     string    `json:"repeat_type"`     // "none", "daily", "weekly", "monthly"
	RepeatInterval int       `json:"repeat_interval"` // Every N days/weeks/months
	RepeatTimes    int       `json:"repeat_times"`    // Total times to repeat (including first), 0 = infinite
	RepeatDone     int       `json:"repeat_done"`     // How many times executed
	RepeatUntil    time.Time `json:"repeat_until"`    // Optional end date (overrides RepeatTimes)
	LastRun        time.Time `json:"last_run"`        // Last execution time
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
	// Get tenant_id for this user to ensure scoped update
	var tenantID int
	err = authQueryRow("SELECT tenant_id FROM users WHERE id = ?", userID).Scan(&tenantID)
	if err != nil {
		log.Println("Failed to get tenant for user", userID, ":", err)
		return
	}
	_, err = authExec("UPDATE users SET password = ? WHERE id = ? AND tenant_id = ?", hashed, userID, tenantID)
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

// ========== AUDIT LOGGING ==========

// logAudit writes a structured audit log entry to stdout.
// Format: [AUDIT] timestamp | event | userID | tenantID | ip | detail
func logAudit(event string, userID, tenantID int, ip, detail string) {
	log.Printf("[AUDIT] event=%s userID=%d tenantID=%d ip=%s detail=%s",
		event, userID, tenantID, ip, detail)
}

// ========== ACCOUNT LOCKOUT ==========

const (
	loginMaxFailures    = 5               // lock after 5 consecutive failures
	loginLockDuration   = 15 * time.Minute // locked for 15 minutes
	loginFailResetAfter = 30 * time.Minute // reset counter after 30 min of no failures
)

// lockoutKey returns the map key for a given tenant+username combo
func lockoutKey(tenantID int, username string) string {
	return fmt.Sprintf("%d:%s", tenantID, strings.ToLower(strings.TrimSpace(username)))
}

// recordLoginFailure increments the failure counter and locks if threshold reached
func recordLoginFailure(tenantID int, username string) {
	key := lockoutKey(tenantID, username)
	loginFailMux.Lock()
	defer loginFailMux.Unlock()
	rec := loginFailMap[key]
	now := time.Now()
	// Reset counter if last failure was long ago
	if !rec.LastFail.IsZero() && now.Sub(rec.LastFail) > loginFailResetAfter {
		rec.Count = 0
		rec.LockedAt = time.Time{}
	}
	rec.Count++
	rec.LastFail = now
	if rec.Count >= loginMaxFailures {
		rec.LockedAt = now
	}
	loginFailMap[key] = rec
}

// resetLoginFailures clears the failure counter on successful login
func resetLoginFailures(tenantID int, username string) {
	key := lockoutKey(tenantID, username)
	loginFailMux.Lock()
	defer loginFailMux.Unlock()
	delete(loginFailMap, key)
}

// checkAccountLocked returns (locked bool, remainingDuration)
func checkAccountLocked(tenantID int, username string) (bool, time.Duration) {
	key := lockoutKey(tenantID, username)
	loginFailMux.Lock()
	defer loginFailMux.Unlock()
	rec, ok := loginFailMap[key]
	if !ok || rec.LockedAt.IsZero() {
		return false, 0
	}
	elapsed := time.Since(rec.LockedAt)
	if elapsed >= loginLockDuration {
		// Lock expired — clear it
		delete(loginFailMap, key)
		return false, 0
	}
	return true, loginLockDuration - elapsed
}

// ========== OTP RESEND COOLDOWN ==========

const otpResendCooldown = 60 * time.Second

// checkOTPResendCooldown returns (allowed bool, waitDuration)
func checkOTPResendCooldown(userID int) (bool, time.Duration) {
	otpResendMux.Lock()
	defer otpResendMux.Unlock()
	last, ok := otpResendMap[userID]
	if !ok {
		return true, 0
	}
	elapsed := time.Since(last)
	if elapsed >= otpResendCooldown {
		return true, 0
	}
	return false, otpResendCooldown - elapsed
}

// recordOTPResend records the time of the last OTP send for a user
func recordOTPResend(userID int) {
	otpResendMux.Lock()
	defer otpResendMux.Unlock()
	otpResendMap[userID] = time.Now()
}

// ========== PASSWORD COMPLEXITY ==========

// validatePasswordComplexity checks that a password meets minimum complexity requirements:
// at least 8 chars, 1 uppercase letter, 1 digit.
func validatePasswordComplexity(password string) string {
	if len(password) < 8 {
		return "Password minimal 8 karakter"
	}
	hasUpper := false
	hasDigit := false
	for _, ch := range password {
		if ch >= 'A' && ch <= 'Z' {
			hasUpper = true
		}
		if ch >= '0' && ch <= '9' {
			hasDigit = true
		}
	}
	if !hasUpper {
		return "Password harus mengandung minimal 1 huruf kapital"
	}
	if !hasDigit {
		return "Password harus mengandung minimal 1 angka"
	}
	return ""
}

// ========== EMAIL VALIDATION ==========

// validateEmail performs a basic but stricter email format check.
func validateEmail(email string) bool {
	email = strings.TrimSpace(email)
	if len(email) < 5 || len(email) > 254 {
		return false
	}
	atIdx := strings.LastIndex(email, "@")
	if atIdx < 1 {
		return false
	}
	local := email[:atIdx]
	domain := email[atIdx+1:]
	if len(local) == 0 || len(domain) < 3 {
		return false
	}
	dotIdx := strings.LastIndex(domain, ".")
	if dotIdx < 1 || dotIdx == len(domain)-1 {
		return false
	}
	return true
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

func envBool(name string) bool {
	val := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	return val == "1" || val == "true" || val == "yes" || val == "y" || val == "on"
}

func normalizeWhatsAppNumber(input string) string {
	n := strings.TrimSpace(input)
	n = strings.TrimPrefix(n, "+")
	n = strings.ReplaceAll(n, " ", "")
	n = strings.ReplaceAll(n, "-", "")
	if strings.HasPrefix(n, "0") && len(n) > 1 {
		n = "62" + strings.TrimPrefix(n, "0")
	}
	return n
}

func otpDisabled() bool {
	if envBool("FORCE_DISABLE_OTP") {
		return true
	}
	mu.Lock()
	enabled := cfg.OTPEnabled
	mu.Unlock()
	return !enabled
}

func getUserTimeLocation(userID, tenantID int) *time.Location {
	var tz string
	if err := authQueryRow("SELECT COALESCE(timezone, '') FROM users WHERE id = ? AND tenant_id = ?", userID, tenantID).Scan(&tz); err != nil {
		return time.Local
	}
	tz = strings.TrimSpace(tz)
	if tz == "" {
		return time.Local
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.Local
	}
	return loc
}

func columnExists(db *sql.DB, table, column string) bool {
	var count int
	if authDialect == "postgres" {
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2",
			table, column,
		).Scan(&count); err != nil {
			return false
		}
		return count > 0
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM pragma_table_info('%s') WHERE name = ?", table)
	if err := db.QueryRow(query, column).Scan(&count); err != nil {
		return false
	}
	return count > 0
}

func ensureColumn(db *sql.DB, table, column, alterSQL string) {
	if columnExists(db, table, column) {
		return
	}
	if _, err := db.Exec(alterSQL); err != nil {
		log.Println("Failed DB migration for", table, "column", column, ":", err)
	}
}

func initAuthSchema() error {
	if authDialect == "postgres" {
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS tenants (
				id SERIAL PRIMARY KEY,
				name TEXT UNIQUE NOT NULL,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)`,
			`CREATE TABLE IF NOT EXISTS users (
				id SERIAL PRIMARY KEY,
				tenant_id INTEGER NOT NULL DEFAULT 1 REFERENCES tenants(id),
				username TEXT NOT NULL,
				whatsapp_number TEXT,
				timezone TEXT,
				email TEXT,
				password TEXT,
				is_admin BOOLEAN NOT NULL DEFAULT FALSE,
				is_active BOOLEAN NOT NULL DEFAULT FALSE
			)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS users_tenant_username_unique ON users(tenant_id, username)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS users_tenant_email_unique ON users(tenant_id, email) WHERE email IS NOT NULL AND email <> ''`,
			`CREATE TABLE IF NOT EXISTS user_settings (
				tenant_id INTEGER NOT NULL REFERENCES tenants(id),
				user_id INTEGER NOT NULL REFERENCES users(id),
				system_prompt TEXT NOT NULL DEFAULT '',
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				PRIMARY KEY (tenant_id, user_id)
			)`,
			`CREATE TABLE IF NOT EXISTS user_ai_settings (
				tenant_id INTEGER NOT NULL REFERENCES tenants(id),
				user_id INTEGER NOT NULL REFERENCES users(id),
				active_provider TEXT NOT NULL DEFAULT '',
				providers_json TEXT NOT NULL DEFAULT '{}',
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				PRIMARY KEY (tenant_id, user_id)
			)`,
			`CREATE TABLE IF NOT EXISTS user_devices (
				id SERIAL PRIMARY KEY,
				tenant_id INTEGER NOT NULL DEFAULT 1 REFERENCES tenants(id),
				user_id INTEGER REFERENCES users(id),
				device_jid TEXT,
				alias TEXT,
				status TEXT,
				is_primary BOOLEAN NOT NULL DEFAULT FALSE,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS user_devices_unique ON user_devices(tenant_id, user_id, device_jid)`,
			`CREATE TABLE IF NOT EXISTS followup_tasks (
				id SERIAL PRIMARY KEY,
				tenant_id INTEGER NOT NULL DEFAULT 1 REFERENCES tenants(id),
				user_id INTEGER REFERENCES users(id),
				jid TEXT,
				scheduled_time TIMESTAMPTZ,
				instruction TEXT,
				status TEXT NOT NULL DEFAULT 'pending',
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				repeat_type TEXT NOT NULL DEFAULT 'none',
				repeat_interval INTEGER NOT NULL DEFAULT 1,
				repeat_times INTEGER NOT NULL DEFAULT 0,
				repeat_done INTEGER NOT NULL DEFAULT 0,
				repeat_until TIMESTAMPTZ,
				last_run TIMESTAMPTZ
			)`,
			`CREATE TABLE IF NOT EXISTS message_events (
				id SERIAL PRIMARY KEY,
				tenant_id INTEGER NOT NULL DEFAULT 1 REFERENCES tenants(id),
				user_id INTEGER REFERENCES users(id),
				chat_jid TEXT,
				direction TEXT NOT NULL,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)`,
			`CREATE INDEX IF NOT EXISTS message_events_tenant_user_time_idx ON message_events(tenant_id, user_id, created_at)`,
			`CREATE TABLE IF NOT EXISTS tenant_knowledge_files (
				id SERIAL PRIMARY KEY,
				tenant_id INTEGER NOT NULL REFERENCES tenants(id),
				filename TEXT NOT NULL,
				original_name TEXT,
				uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				UNIQUE(tenant_id, filename)
			)`,
		}
		for _, stmt := range stmts {
			if _, err := authExec(stmt); err != nil {
				return err
			}
		}
		return nil
	}
	_, err := authExec(`CREATE TABLE IF NOT EXISTS tenants (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT UNIQUE,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		username TEXT UNIQUE,
		whatsapp_number TEXT,
		timezone TEXT,
		email TEXT,
		password TEXT,
		is_admin BOOLEAN DEFAULT 0,
		is_active BOOLEAN DEFAULT 0,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id)
	);
	CREATE TABLE IF NOT EXISTS user_settings (
		tenant_id INTEGER NOT NULL DEFAULT 1,
		user_id INTEGER NOT NULL,
		system_prompt TEXT DEFAULT '',
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id),
		FOREIGN KEY(user_id) REFERENCES users(id),
		UNIQUE(tenant_id, user_id)
	);
	CREATE TABLE IF NOT EXISTS user_ai_settings (
		tenant_id INTEGER NOT NULL DEFAULT 1,
		user_id INTEGER NOT NULL,
		active_provider TEXT DEFAULT '',
		providers_json TEXT DEFAULT '{}',
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id),
		FOREIGN KEY(user_id) REFERENCES users(id),
		UNIQUE(tenant_id, user_id)
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
		repeat_type TEXT DEFAULT 'none',
		repeat_interval INTEGER DEFAULT 1,
		repeat_times INTEGER DEFAULT 0,
		repeat_done INTEGER DEFAULT 0,
		repeat_until DATETIME,
		last_run DATETIME,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id),
		FOREIGN KEY(user_id) REFERENCES users(id)
	);
	CREATE TABLE IF NOT EXISTS message_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL DEFAULT 1,
		user_id INTEGER,
		chat_jid TEXT,
		direction TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id),
		FOREIGN KEY(user_id) REFERENCES users(id)
	);
	CREATE INDEX IF NOT EXISTS message_events_tenant_user_time_idx ON message_events(tenant_id, user_id, created_at);
	CREATE TABLE IF NOT EXISTS tenant_knowledge_files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tenant_id INTEGER NOT NULL,
		filename TEXT NOT NULL,
		original_name TEXT,
		uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id),
		UNIQUE(tenant_id, filename)
	);`)
	return err
}

func getUserSystemPrompt(userID, tenantID int) string {
	var prompt string
	err := authQueryRow("SELECT COALESCE(system_prompt, '') FROM user_settings WHERE user_id = ? AND tenant_id = ?", userID, tenantID).Scan(&prompt)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(prompt)
}

func setUserSystemPrompt(userID, tenantID int, prompt string) error {
	prompt = strings.TrimSpace(prompt)
	if authDialect == "postgres" {
		_, err := authExec(
			`INSERT INTO user_settings (tenant_id, user_id, system_prompt, updated_at)
			 VALUES (?, ?, ?, NOW())
			 ON CONFLICT (tenant_id, user_id) DO UPDATE SET system_prompt = EXCLUDED.system_prompt, updated_at = NOW()`,
			tenantID, userID, prompt,
		)
		return err
	}
	_, err := authExec(
		`INSERT INTO user_settings (tenant_id, user_id, system_prompt, updated_at)
		 VALUES (?, ?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(tenant_id, user_id) DO UPDATE SET system_prompt = excluded.system_prompt, updated_at = CURRENT_TIMESTAMP`,
		tenantID, userID, prompt,
	)
	return err
}

type UserAIConfig struct {
	ActiveProvider string                    `json:"active_provider"`
	Providers      map[string]ProviderConfig `json:"providers"`
}

func getUserAIConfig(userID, tenantID int) (UserAIConfig, bool) {
	var activeProvider string
	var providersJSON string
	err := authQueryRow("SELECT COALESCE(active_provider, ''), COALESCE(providers_json, '{}') FROM user_ai_settings WHERE user_id = ? AND tenant_id = ?", userID, tenantID).Scan(&activeProvider, &providersJSON)
	if err != nil {
		return UserAIConfig{}, false
	}

	providers := map[string]ProviderConfig{}
	if strings.TrimSpace(providersJSON) != "" {
		_ = json.Unmarshal([]byte(providersJSON), &providers)
	}
	if providers == nil {
		providers = map[string]ProviderConfig{}
	}

	return UserAIConfig{
		ActiveProvider: strings.TrimSpace(activeProvider),
		Providers:      providers,
	}, true
}

func defaultSystemPromptForProvider(providerName, model string) string {
	p := strings.ToLower(strings.TrimSpace(providerName))
	m := strings.ToLower(strings.TrimSpace(model))
	if strings.Contains(m, "code") || strings.Contains(m, "coder") {
		return "You are a helpful coding assistant."
	}
	if p == "sumopod" {
		return "You are a helpful assistant."
	}
	return "You are a helpful assistant."
}

func setUserAIConfig(userID, tenantID int, cfg UserAIConfig) error {
	cfg.ActiveProvider = strings.TrimSpace(cfg.ActiveProvider)
	if cfg.Providers == nil {
		cfg.Providers = map[string]ProviderConfig{}
	}
	raw, err := json.Marshal(cfg.Providers)
	if err != nil {
		return err
	}

	if authDialect == "postgres" {
		_, err := authExec(
			`INSERT INTO user_ai_settings (tenant_id, user_id, active_provider, providers_json, updated_at)
			 VALUES (?, ?, ?, ?, NOW())
			 ON CONFLICT (tenant_id, user_id) DO UPDATE SET active_provider = EXCLUDED.active_provider, providers_json = EXCLUDED.providers_json, updated_at = NOW()`,
			tenantID, userID, cfg.ActiveProvider, string(raw),
		)
		return err
	}

	_, err = authExec(
		`INSERT INTO user_ai_settings (tenant_id, user_id, active_provider, providers_json, updated_at)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(tenant_id, user_id) DO UPDATE SET active_provider = excluded.active_provider, providers_json = excluded.providers_json, updated_at = CURRENT_TIMESTAMP`,
		tenantID, userID, cfg.ActiveProvider, string(raw),
	)
	return err
}

func isPrivateOrLocalIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	if ip4 := ip.To4(); ip4 != nil {
		if ip4[0] == 10 {
			return true
		}
		if ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31 {
			return true
		}
		if ip4[0] == 192 && ip4[1] == 168 {
			return true
		}
		if ip4[0] == 127 {
			return true
		}
		if ip4[0] == 169 && ip4[1] == 254 {
			return true
		}
		return false
	}
	if len(ip) == net.IPv6len {
		if ip.Equal(net.IPv6loopback) {
			return true
		}
		if ip[0]&0xfe == 0xfc {
			return true
		}
		if ip[0] == 0xfe && ip[1]&0xc0 == 0x80 {
			return true
		}
	}
	return false
}

func validateOutboundBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("base_url empty")
	}
	if len(raw) > 512 {
		return "", fmt.Errorf("base_url terlalu panjang")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("base_url tidak valid")
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	u.Fragment = ""
	if u.Scheme != "https" && !(u.Scheme == "http" && envBool("ALLOW_INSECURE_OUTBOUND_HTTP")) {
		return "", fmt.Errorf("scheme base_url harus https")
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", fmt.Errorf("base_url host kosong")
	}
	if (host == "localhost" || host == "127.0.0.1" || host == "::1" || strings.HasSuffix(host, ".local")) && !envBool("ALLOW_PRIVATE_OUTBOUND") {
		return "", fmt.Errorf("base_url host tidak diizinkan")
	}
	if ip := net.ParseIP(host); ip != nil {
		if isPrivateOrLocalIP(ip) && !envBool("ALLOW_PRIVATE_OUTBOUND") {
			return "", fmt.Errorf("base_url ip private tidak diizinkan")
		}
	} else if !envBool("ALLOW_PRIVATE_OUTBOUND") {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		ips, lookupErr := net.DefaultResolver.LookupIP(ctx, "ip", host)
		if lookupErr == nil {
			for _, ip := range ips {
				if isPrivateOrLocalIP(ip) {
					return "", fmt.Errorf("base_url resolve ke ip private tidak diizinkan")
				}
			}
		}
	}
	port := u.Port()
	if port != "" && port != "443" && port != "80" && !envBool("ALLOW_NONSTANDARD_OUTBOUND_PORTS") {
		return "", fmt.Errorf("base_url port tidak diizinkan (gunakan port 443/80 atau set ALLOW_NONSTANDARD_OUTBOUND_PORTS=true)")
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	return u.String(), nil
}

func httpClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &http.Client{Timeout: timeout}
}

type aiMediaCommand struct {
	ImageURL string `json:"url"`
	Caption  string `json:"caption"`
	Text     string `json:"text"`
	Type     string `json:"type"`
}

func parseAIMediaCommand(reply string) (string, string, string) {
	reply = strings.TrimSpace(reply)
	if reply == "" {
		return "", "", ""
	}

	var cmd aiMediaCommand
	if strings.HasPrefix(reply, "{") && strings.HasSuffix(reply, "}") {
		if err := json.Unmarshal([]byte(reply), &cmd); err == nil {
			if strings.ToLower(strings.TrimSpace(cmd.Type)) == "image" && strings.TrimSpace(cmd.ImageURL) != "" {
				caption := strings.TrimSpace(cmd.Caption)
				if caption == "" {
					caption = strings.TrimSpace(cmd.Text)
				}
				return strings.TrimSpace(cmd.ImageURL), caption, ""
			}
		}
	}

	lines := strings.Split(reply, "\n")
	var imageURL string
	var caption string
	rest := make([]string, 0, len(lines))
	for _, line := range lines {
		raw := strings.TrimSpace(line)
		upper := strings.ToUpper(raw)
		switch {
		case strings.HasPrefix(upper, "IMAGE_URL:") || strings.HasPrefix(upper, "IMAGE_URL=") || strings.HasPrefix(upper, "IMAGE:") || strings.HasPrefix(upper, "IMAGE="):
			sep := strings.IndexAny(raw, ":=")
			if sep >= 0 {
				imageURL = strings.TrimSpace(raw[sep+1:])
				continue
			}
		case strings.HasPrefix(upper, "CAPTION:") || strings.HasPrefix(upper, "CAPTION="):
			sep := strings.IndexAny(raw, ":=")
			if sep >= 0 {
				caption = strings.TrimSpace(raw[sep+1:])
				continue
			}
		}
		rest = append(rest, line)
	}

	if strings.TrimSpace(imageURL) == "" {
		return "", "", reply
	}

	if strings.TrimSpace(caption) == "" {
		caption = strings.TrimSpace(strings.Join(rest, "\n"))
	}
	return strings.TrimSpace(imageURL), strings.TrimSpace(caption), ""
}

func loadImageBytes(imageURL string) ([]byte, string, error) {
	imageURL = strings.TrimSpace(imageURL)
	if imageURL == "" {
		return nil, "", fmt.Errorf("image_url kosong")
	}
	if strings.HasPrefix(strings.ToLower(imageURL), "data:image/") {
		comma := strings.Index(imageURL, ",")
		if comma < 0 {
			return nil, "", fmt.Errorf("data url tidak valid")
		}
		meta := imageURL[:comma]
		b64 := imageURL[comma+1:]
		if !strings.Contains(strings.ToLower(meta), ";base64") {
			return nil, "", fmt.Errorf("data url harus base64")
		}
		data, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, "", fmt.Errorf("decode base64 gagal")
		}
		mime := "image/jpeg"
		if strings.HasPrefix(strings.ToLower(meta), "data:image/png") {
			mime = "image/png"
		} else if strings.HasPrefix(strings.ToLower(meta), "data:image/webp") {
			mime = "image/webp"
		}
		return data, mime, nil
	}
	if strings.HasPrefix(imageURL, "/uploads/") {
		name := path.Base(imageURL)
		if name == "." || name == "/" || name == "" {
			return nil, "", fmt.Errorf("path uploads tidak valid")
		}
		b, err := os.ReadFile(filepath.Join("uploads", name))
		if err != nil {
			return nil, "", fmt.Errorf("gagal baca file uploads")
		}
		return b, http.DetectContentType(b), nil
	}

	u, err := url.Parse(imageURL)
	if err != nil {
		return nil, "", fmt.Errorf("url tidak valid")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, "", fmt.Errorf("scheme url harus http/https")
	}
	if u.Host == "" {
		return nil, "", fmt.Errorf("host url kosong")
	}
	if _, err := validateOutboundBaseURL(u.Scheme + "://" + u.Host); err != nil {
		return nil, "", fmt.Errorf("host url tidak diizinkan")
	}

	client := httpClient(25 * time.Second)
	req, err := http.NewRequest("GET", imageURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("request gagal")
	}
	req.Header.Set("User-Agent", "WahakuBot/1.0")
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("gagal fetch image")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("fetch image gagal (%d)", resp.StatusCode)
	}

	const maxSize = 5 * 1024 * 1024
	limited := io.LimitReader(resp.Body, maxSize+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, "", fmt.Errorf("gagal baca image")
	}
	if len(data) > maxSize {
		return nil, "", fmt.Errorf("image terlalu besar (maks 5MB)")
	}

	mime := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if !strings.HasPrefix(strings.ToLower(mime), "image/") {
		mime = http.DetectContentType(data)
	}
	if !strings.HasPrefix(strings.ToLower(mime), "image/") {
		return nil, "", fmt.Errorf("bukan file gambar")
	}
	return data, mime, nil
}

func sendImageWithCaption(cli *whatsmeow.Client, to types.JID, imageBytes []byte, mimeType, caption string) error {
	if cli == nil || !cli.IsConnected() {
		return fmt.Errorf("client tidak terkoneksi")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	up, err := cli.Upload(ctx, imageBytes, whatsmeow.MediaImage)
	if err != nil {
		return err
	}

	caption = strings.TrimSpace(caption)
	img := &waE2E.ImageMessage{
		Caption:       proto.String(caption),
		Mimetype:      proto.String(strings.TrimSpace(mimeType)),
		URL:           proto.String(up.URL),
		DirectPath:    proto.String(up.DirectPath),
		MediaKey:      up.MediaKey,
		FileEncSHA256: up.FileEncSHA256,
		FileSHA256:    up.FileSHA256,
		FileLength:    proto.Uint64(uint64(up.FileLength)),
	}
	_, err = cli.SendMessage(ctx, to, &waE2E.Message{ImageMessage: img})
	return err
}

func sendAIReply(cli *whatsmeow.Client, tenantID, userID int, to types.JID, reply string) error {
	imageURL, caption, text := parseAIMediaCommand(reply)
	if strings.TrimSpace(imageURL) != "" {
		b, mime, err := loadImageBytes(imageURL)
		if err != nil {
			return err
		}
		if err := sendImageWithCaption(cli, to, b, mime, caption); err != nil {
			return err
		}
		recordMessageEvent(tenantID, userID, to.String(), "out", time.Now())
		return nil
	}
	msg := strings.TrimSpace(text)
	if msg == "" {
		msg = strings.TrimSpace(reply)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := cli.SendMessage(ctx, to, &waE2E.Message{Conversation: &msg})
	if err == nil {
		recordMessageEvent(tenantID, userID, to.String(), "out", time.Now())
	}
	return err
}

func rebindQuery(query string) string {
	if authDialect != "postgres" {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 8)
	argIndex := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			b.WriteString(fmt.Sprintf("$%d", argIndex))
			argIndex++
			continue
		}
		b.WriteByte(query[i])
	}
	return b.String()
}

func authQueryRow(query string, args ...any) *sql.Row {
	return authDB.QueryRow(rebindQuery(query), args...)
}

func authQuery(query string, args ...any) (*sql.Rows, error) {
	return authDB.Query(rebindQuery(query), args...)
}

func authExec(query string, args ...any) (sql.Result, error) {
	return authDB.Exec(rebindQuery(query), args...)
}

func pendingNowSQL() string {
	if authDialect == "postgres" {
		return "NOW()"
	}
	return "datetime('now')"
}

func requireAdmin(c *fiber.Ctx) error {
	if isAdmin, ok := c.Locals("isAdmin").(bool); ok && isAdmin {
		return c.Next()
	}
	return c.Status(403).JSON(fiber.Map{"success": false, "message": "Requires Admin privileges"})
}

func requirePlatformAdmin(c *fiber.Ctx) error {
	if isAdmin, ok := c.Locals("isAdmin").(bool); !ok || !isAdmin {
		return c.Status(403).JSON(fiber.Map{"success": false, "message": "Requires Admin privileges"})
	}
	userID := c.Locals("userID").(int)
	if !isPlatformAdminUser(userID) {
		return c.Status(403).JSON(fiber.Map{"success": false, "message": "Requires Platform Admin privileges"})
	}
	return c.Next()
}

// Tenant identification middleware
func tenantMiddleware(c *fiber.Ctx) error {
	// 1. Check if user is logged in (session exists)
	sess, err := sessionStore.Get(c)
	if err == nil {
		if authenticated := sess.Get("authenticated"); authenticated == true {
			// Use tenantID from session for authenticated users
			if tenantID := sess.Get("tenantID"); tenantID != nil {
				c.Locals("tenantID", tenantID)
				return c.Next()
			}
		}
	}

	// 2. For unauthenticated requests (public endpoints), use header
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
	err = authQueryRow("SELECT EXISTS(SELECT 1 FROM tenants WHERE id = ?)", tenantID).Scan(&exists)
	if err != nil {
		log.Println("Tenant lookup database error:", err)
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

	if cfg.Database.Enabled && strings.ToLower(cfg.Database.Type) == "postgres" {
		authDialect = "postgres"
		dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", cfg.Database.Host, cfg.Database.Port, cfg.Database.User, cfg.Database.Password, cfg.Database.Name)
		pgDB, err := sql.Open("postgres", dsn)
		if err != nil {
			log.Fatal("Failed to open Postgres DB:", err)
		}
		pgDB.SetMaxOpenConns(10)
		pgDB.SetMaxIdleConns(10)
		pgDB.SetConnMaxLifetime(time.Hour)
		if err := pgDB.Ping(); err != nil {
			log.Fatal("Failed to ping Postgres DB:", err)
		}
		authDB = pgDB
		container = sqlstore.NewWithDB(pgDB, "postgres", dbLog)
	} else {
		authDialect = "sqlite"
		sharedDB, err := sql.Open("sqlite", "file:wahaku.db?_pragma=foreign_keys(1)&_busy_timeout=5000")
		if err != nil {
			log.Fatal("Failed to open SQLite DB:", err)
		}
		sharedDB.SetMaxOpenConns(5)
		sharedDB.SetMaxIdleConns(5)
		sharedDB.SetConnMaxLifetime(time.Hour)
		authDB = sharedDB
		container = sqlstore.NewWithDB(sharedDB, "sqlite", dbLog)
	}

	if err := container.Upgrade(context.Background()); err != nil {
		log.Fatal("Whatsmeow Store Upgrade Failed:", err)
	}

	if err := initAuthSchema(); err != nil {
		log.Fatal("Failed to init auth schema:", err)
	}
	log.Println("Auth storage dialect:", authDialect)

	ensureBillingSchema()

	ensureColumn(authDB, "users", "whatsapp_number", "ALTER TABLE users ADD COLUMN whatsapp_number TEXT")
	if authDialect == "postgres" {
		ensureColumn(authDB, "users", "timezone", "ALTER TABLE users ADD COLUMN timezone TEXT")
	} else {
		ensureColumn(authDB, "users", "timezone", "ALTER TABLE users ADD COLUMN timezone TEXT")
	}

	if authDialect == "sqlite" {
		ensureColumn(authDB, "users", "tenant_id", "ALTER TABLE users ADD COLUMN tenant_id INTEGER NOT NULL DEFAULT 1")
		ensureColumn(authDB, "user_devices", "tenant_id", "ALTER TABLE user_devices ADD COLUMN tenant_id INTEGER NOT NULL DEFAULT 1")
		ensureColumn(authDB, "followup_tasks", "tenant_id", "ALTER TABLE followup_tasks ADD COLUMN tenant_id INTEGER NOT NULL DEFAULT 1")
		ensureColumn(authDB, "tenant_knowledge_files", "tenant_id", "ALTER TABLE tenant_knowledge_files ADD COLUMN tenant_id INTEGER NOT NULL DEFAULT 1")

		var emailColCount int
		authQueryRow("SELECT COUNT(*) FROM pragma_table_info('users') WHERE name='email'").Scan(&emailColCount)
		if emailColCount == 0 {
			log.Println("Migrating DB: Adding email column to users table...")
			authExec("ALTER TABLE users ADD COLUMN email TEXT")
		}

		var isPrimaryColCount int
		authQueryRow("SELECT COUNT(*) FROM pragma_table_info('user_devices') WHERE name='is_primary'").Scan(&isPrimaryColCount)
		if isPrimaryColCount == 0 {
			log.Println("Migrating DB: Adding is_primary column to user_devices table...")
			authExec("ALTER TABLE user_devices ADD COLUMN is_primary BOOLEAN DEFAULT 0")
		}

		ensureColumn(authDB, "followup_tasks", "repeat_times", "ALTER TABLE followup_tasks ADD COLUMN repeat_times INTEGER DEFAULT 0")
		ensureColumn(authDB, "followup_tasks", "repeat_done", "ALTER TABLE followup_tasks ADD COLUMN repeat_done INTEGER DEFAULT 0")
		ensureColumn(authDB, "followup_tasks", "repeat_until", "ALTER TABLE followup_tasks ADD COLUMN repeat_until DATETIME")
		ensureColumn(authDB, "followup_tasks", "last_run", "ALTER TABLE followup_tasks ADD COLUMN last_run DATETIME")
		ensureColumn(authDB, "followup_tasks", "repeat_type", "ALTER TABLE followup_tasks ADD COLUMN repeat_type TEXT DEFAULT 'none'")
		ensureColumn(authDB, "followup_tasks", "repeat_interval", "ALTER TABLE followup_tasks ADD COLUMN repeat_interval INTEGER DEFAULT 1")
	}

	// Migration: Move device_jid from users to user_devices if exists
	var deviceJIDColCount int
	if authDialect == "sqlite" {
		authQueryRow("SELECT COUNT(*) FROM pragma_table_info('users') WHERE name='device_jid'").Scan(&deviceJIDColCount)
	}
	if authDialect == "sqlite" && deviceJIDColCount > 0 {
		log.Println("Migrating DB: Moving device_jid to user_devices table...")
		// Select existing
		type migrationData struct {
			UserID int
			JID    string
		}
		var dataToMigrate []migrationData

		// Get tenant ID from context (not available during migration, using default)
		var tenantID int = 1 // Default tenant ID

		rows, err := authQuery("SELECT id, device_jid FROM users WHERE device_jid IS NOT NULL AND device_jid != '' AND tenant_id = ?", tenantID)
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
			_, err := authExec("INSERT INTO user_devices (tenant_id, user_id, device_jid, alias, status) VALUES (?, ?, ?, 'Main Device', 'CONNECTED')", tenantID, d.UserID, d.JID)
			if err != nil {
				log.Println("Migration Insert Error:", err)
			}
		}
	}

	var tenantCount int
	authQueryRow("SELECT COUNT(*) FROM tenants WHERE name = ?", "default").Scan(&tenantCount)
	if tenantCount == 0 {
		if _, err := authExec("INSERT INTO tenants (name) VALUES (?)", "default"); err != nil {
			log.Println("Failed to create default tenant:", err)
		}
	}

	// Get default tenant ID
	var defaultTenantID int
	authQueryRow("SELECT id FROM tenants WHERE name = ?", "default").Scan(&defaultTenantID)

	// Create Admin if not exists in default tenant
	var count int
	authQueryRow("SELECT COUNT(*) FROM users WHERE username = ? AND tenant_id = ?", cfg.AdminUsername, defaultTenantID).Scan(&count)
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
			// Use default tenant ID already fetched above
			_, err := authExec("INSERT INTO users (username, password, tenant_id, is_admin, is_active) VALUES (?, ?, ?, ?, ?)", cfg.AdminUsername, adminPassword, defaultTenantID, true, true)
			if err != nil {
				log.Println("Failed to create initial admin user:", err)
			}
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
		BodyLimit: 10 * 1024 * 1024, // 10MB Limit
	})

	// Serve static files FIRST — before any middleware so CSS/JS always loads
	// Explicit handler for styles.css with query string support (e.g. ?v=4.6)
	app.Use(func(c *fiber.Ctx) error {
		p := c.Path()
		// Serve CSS directly — bypass all middleware
		if p == "/views/styles.css" || p == "/styles.css" {
			c.Set("Content-Type", "text/css; charset=utf-8")
			c.Set("Cache-Control", "public, max-age=86400")
			return c.SendFile("./views/styles.css")
		}
		// Serve other static assets under /views/
		if strings.HasPrefix(p, "/views/") {
			ext := strings.ToLower(filepath.Ext(p))
			switch ext {
			case ".css", ".js", ".png", ".jpg", ".jpeg", ".svg", ".ico", ".woff", ".woff2":
				return c.SendFile("." + p)
			}
		}
		return c.Next()
	})
	app.Static("/uploads", "./uploads")

	app.Use(func(c *fiber.Ctx) error {
		p := strings.ToLower(c.Path())
		if strings.Contains(p, "..") {
			return c.SendStatus(404)
		}
		switch p {
		case "/config.json", "/wahaku.db", "/wahaku.env", "/.env", "/.git", "/.git/config", "/.gitignore":
			return c.SendStatus(404)
		}
		return c.Next()
	})
	allowedOrigins := os.Getenv("CORS_ALLOW_ORIGINS")
	if allowedOrigins == "" {
		allowedOrigins = "http://localhost:" + cfg.AppPort + ",http://127.0.0.1:" + cfg.AppPort
	}
	app.Use(cors.New(cors.Config{
		AllowOrigins:     allowedOrigins,
		AllowCredentials: true,
	}))

	// CSRF Protection: cek Origin/Referer untuk POST/PUT/DELETE
	// Lebih reliable di balik reverse proxy dibanding cookie-based CSRF
	app.Use(func(c *fiber.Ctx) error {
		method := c.Method()
		if method != "POST" && method != "PUT" && method != "DELETE" && method != "PATCH" {
			return c.Next()
		}
		// Skip untuk auth endpoints (sudah dilindungi rate limit + lockout)
		p := c.Path()
		if strings.HasPrefix(p, "/api/auth/") || strings.HasPrefix(p, "/api/public/") {
			return c.Next()
		}
		// Hanya enforce untuk API endpoints
		if !strings.HasPrefix(p, "/api/") {
			return c.Next()
		}

		origin := strings.TrimSpace(c.Get("Origin"))
		referer := strings.TrimSpace(c.Get("Referer"))

		// Jika tidak ada Origin maupun Referer sama sekali — tolak
		// (browser selalu kirim salah satu untuk same-origin request)
		if origin == "" && referer == "" {
			return c.Status(403).JSON(fiber.Map{"success": false, "message": "Forbidden: missing origin"})
		}

		// Ambil nilai yang akan dicek
		check := origin
		if check == "" {
			check = referer
		}

		// Parse dan validasi scheme — harus http atau https
		// (bukan file://, chrome-extension://, dll)
		u, err := url.Parse(check)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
			return c.Status(403).JSON(fiber.Map{"success": false, "message": "Forbidden: invalid origin"})
		}

		// Hostname dari Origin/Referer tidak boleh kosong
		if u.Hostname() == "" {
			return c.Status(403).JSON(fiber.Map{"success": false, "message": "Forbidden: empty origin host"})
		}

		// Cek apakah origin host sama dengan Host header yang dikirim Nginx
		// Gunakan X-Forwarded-Host jika ada (lebih reliable di balik proxy)
		expectedHost := strings.TrimSpace(c.Get("X-Forwarded-Host"))
		if expectedHost == "" {
			expectedHost = c.Hostname()
		}
		// Strip port dari expectedHost jika ada
		if h, _, err2 := net.SplitHostPort(expectedHost); err2 == nil {
			expectedHost = h
		}

		if expectedHost != "" && u.Hostname() != expectedHost {
			return c.Status(403).JSON(fiber.Map{"success": false, "message": "Forbidden: origin mismatch"})
		}

		return c.Next()
	})
	app.Use(tenantMiddleware)

	// Middleware Auth
	app.Use(func(c *fiber.Ctx) error {
		// Whitelist Static Assets
		path := c.Path()
		if strings.HasSuffix(path, ".css") || strings.HasSuffix(path, ".js") || strings.HasSuffix(path, ".png") || strings.HasSuffix(path, ".jpg") || strings.HasSuffix(path, ".ico") || strings.HasSuffix(path, ".svg") {
			return c.Next()
		}

		// Whitelist Routes
		if path == "/" || path == "/landing" || path == "/landing.html" || path == "/login" || path == "/register" || strings.HasPrefix(path, "/api/auth") || strings.HasPrefix(path, "/api/public") {
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

	app.Use(func(c *fiber.Ctx) error {
		if !strings.HasPrefix(c.Path(), "/api") {
			return c.Next()
		}
		p := c.Path()
		if strings.HasPrefix(p, "/api/auth") ||
			strings.HasPrefix(p, "/api/public") ||
			strings.HasPrefix(p, "/api/billing") ||
			strings.HasPrefix(p, "/api/admin/billing") ||
			p == "/api/me" ||
			p == "/api/profile" ||
			strings.HasPrefix(p, "/api/profile/") {
			return c.Next()
		}
		isAdmin, _ := c.Locals("isAdmin").(bool)
		if isAdmin {
			userID := c.Locals("userID").(int)
			if isPlatformAdminUser(userID) {
				return c.Next()
			}
		}
		tenantID := c.Locals("tenantID").(int)
		ensureTenantSubscription(tenantID)
		sub, ok := getTenantSubscription(tenantID)
		if !ok || !isSubscriptionActive(sub) {
			return c.Status(402).JSON(fiber.Map{
				"success": false,
				"code":    "SUBSCRIPTION_REQUIRED",
				"message": "Langganan tidak aktif. Silakan perpanjang untuk menggunakan fitur ini.",
			})
		}
		return c.Next()
	})

	// Ensure uploads directory exists
	os.MkdirAll("uploads", 0755)

	// Serve HTML pages
	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendFile("./views/landing.html")
	})
	app.Get("/login", func(c *fiber.Ctx) error {
		return c.SendFile("./views/login.html")
	})
	app.Get("/register", func(c *fiber.Ctx) error {
		return c.SendFile("./views/register.html")
	})
	app.Get("/dashboard", func(c *fiber.Ctx) error {
		return c.SendFile("./views/index.html")
	})

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

		query := "SELECT id, username, COALESCE(whatsapp_number, ''), COALESCE(email, ''), COALESCE(password, ''), COALESCE(is_admin, FALSE), COALESCE(is_active, FALSE) FROM users WHERE username = ? AND tenant_id = ?"
		if isEmailLogin {
			query = "SELECT id, username, COALESCE(whatsapp_number, ''), COALESCE(email, ''), COALESCE(password, ''), COALESCE(is_admin, FALSE), COALESCE(is_active, FALSE) FROM users WHERE email = ? AND tenant_id = ?"
		}

		err = authQueryRow(query, req.Username, tenantID).Scan(&user.ID, &user.Username, &user.WhatsApp, &user.Email, &user.Password, &user.IsAdmin, &user.IsActive)

		if err == sql.ErrNoRows {
			return c.Status(401).JSON(fiber.Map{"success": false, "message": "User tidak ditemukan"})
		} else if err != nil {
			log.Println("Login database error:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Terjadi kesalahan, coba lagi"})
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

		// Check account lockout before verifying password
		if locked, remaining := checkAccountLocked(tenantID, user.Username); locked {
			return c.Status(429).JSON(fiber.Map{
				"success": false,
				"message": fmt.Sprintf("Akun dikunci sementara karena terlalu banyak percobaan login gagal. Coba lagi dalam %s.", remaining.Round(time.Second)),
			})
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
			recordLoginFailure(tenantID, user.Username)
			logAudit("LOGIN_FAILED", user.ID, tenantID, c.IP(), fmt.Sprintf("username=%s reason=wrong_password", user.Username))
			return c.Status(401).JSON(fiber.Map{"success": false, "message": "Password salah"})
		}

		// Password valid — reset failure counter
		resetLoginFailures(tenantID, user.Username)

		// Password is Valid -> Send OTP

		finalizeLogin := func(message string) error {
			sess, err := sessionStore.Get(c)
			if err != nil {
				return c.Status(500).JSON(fiber.Map{"success": false, "message": "Session Error"})
			}
			sess.Set("authenticated", true)
			sess.Set("userID", user.ID)
			sess.Set("isAdmin", user.IsAdmin)
			sess.Set("tenantID", tenantID)
			if err := sess.Save(); err != nil {
				return c.Status(500).JSON(fiber.Map{"success": false, "message": "Session Error"})
			}
			logAudit("LOGIN_SUCCESS", user.ID, tenantID, c.IP(), fmt.Sprintf("username=%s", user.Username))
			return c.JSON(fiber.Map{"success": true, "message": message})
		}

		if otpDisabled() {
			return finalizeLogin("Login berhasil (OTP dimatikan sementara).")
		}

		// GET SYSTEM BOT (Admin Bot) to send OTP
		sysClient := getSystemBot(tenantID)

		// Check if System Bot is connected
		if sysClient == nil || !sysClient.IsConnected() {
			if user.IsAdmin && envBool("ALLOW_ADMIN_LOGIN_WITHOUT_OTP") {
				return finalizeLogin("Login admin berhasil (OTP sementara dimatikan via env). Segera hubungkan Bot OTP.")
			}
			return c.Status(503).JSON(fiber.Map{"success": false, "message": "Sistem OTP sedang offline. Silakan coba lagi beberapa saat."})
		}
		otp, err := generateSecureOTP()
		if err != nil {
			log.Println("Failed to generate OTP:", err)
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

		// Send OTP asynchronously (non-blocking)
		go func() {
			targetNumber := normalizeWhatsAppNumber(user.WhatsApp)
			if targetNumber == "" {
				targetNumber = normalizeWhatsAppNumber(user.Username)
			}
			if targetNumber == "" || strings.Contains(targetNumber, "@") || len(targetNumber) < 10 {
				log.Printf("Failed to send async OTP: nomor WhatsApp belum valid (username=%s whatsapp_number=%s)", user.Username, user.WhatsApp)
				return
			}
			targetJID := types.NewJID(targetNumber, types.DefaultUserServer)
			if sysClient.Store.ID != nil && targetJID.User == sysClient.Store.ID.User {
				targetJID = *sysClient.Store.ID
				targetJID.Device = 0
			}

			msg := &waE2E.Message{
				Conversation: proto.String("🔐 Kode Login Wahaku Dashboard: *" + otp + "*\n\nJangan berikan kode ini kepada siapapun."),
			}

			send := func(timeout time.Duration) error {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				_, err := sysClient.SendMessage(ctx, targetJID, msg)
				return err
			}

			err := send(45 * time.Second)
			if err != nil && strings.Contains(strings.ToLower(err.Error()), "context deadline exceeded") {
				time.Sleep(600 * time.Millisecond)
				err = send(45 * time.Second)
			}
			if err != nil {
				log.Printf("Failed to send async OTP to %s: %v", targetNumber, err)
				return
			}
			log.Printf("OTP sent async to %s", targetNumber)
		}()

		return c.JSON(fiber.Map{"success": true, "require_otp": true, "message": "OTP dikirim ke WhatsApp"})
	})

	auth.Post("/verify", func(c *fiber.Ctx) error {
		if otpDisabled() {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "OTP sedang dimatikan sementara"})
		}
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

		var expiry int64
		switch v := expiryVal.(type) {
		case int64:
			expiry = v
		case int:
			expiry = int64(v)
		case float64:
			expiry = int64(v)
		case string:
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				expiry = parsed
			} else {
				return c.Status(400).JSON(fiber.Map{"success": false, "message": "OTP tidak valid, silakan login ulang"})
			}
		default:
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "OTP tidak valid, silakan login ulang"})
		}
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

			_, err := authExec("UPDATE users SET is_active = true WHERE id = ? AND tenant_id = ?", userID, tenantIDInt)
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
			logAudit("OTP_VERIFY_SUCCESS", userID, tenantIDInt, c.IP(), "")
			return c.JSON(fiber.Map{"success": true})
		}

		logAudit("OTP_VERIFY_FAILED", 0, 0, c.IP(), "wrong_otp")
		return c.Status(401).JSON(fiber.Map{"success": false, "message": "Kode OTP salah"})
	})

	// Resend OTP endpoint
	auth.Post("/resend-otp", func(c *fiber.Ctx) error {
		if otpDisabled() {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "OTP sedang dimatikan sementara"})
		}
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
		err = authQueryRow("SELECT id, username, COALESCE(whatsapp_number, ''), COALESCE(email, ''), COALESCE(password, ''), COALESCE(is_admin, FALSE), COALESCE(is_active, FALSE) FROM users WHERE id = ? AND tenant_id = ?", userID, tenantID).
			Scan(&user.ID, &user.Username, &user.WhatsApp, &user.Email, &user.Password, &user.IsAdmin, &user.IsActive)
		if err != nil {
			return c.Status(404).JSON(fiber.Map{"success": false, "message": "User tidak ditemukan"})
		}

		// Check if system bot is available
		sysClient := getSystemBot(tenantID)
		if sysClient == nil || !sysClient.IsConnected() {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Sistem Bot belum terhubung, tidak bisa kirim OTP"})
		}

		// Enforce per-user OTP resend cooldown
		if allowed, wait := checkOTPResendCooldown(userID); !allowed {
			return c.Status(429).JSON(fiber.Map{
				"success": false,
				"message": fmt.Sprintf("Tunggu %s sebelum meminta OTP baru.", wait.Round(time.Second)),
			})
		}

		// Generate new OTP
		otp, err := generateSecureOTP()
		if err != nil {
			log.Println("Failed to generate OTP for resend:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal generate OTP"})
		}

		// Record resend time
		recordOTPResend(userID)

		// Update session with new OTP
		sess.Set("otp", otp)
		sess.Set("otp_expiry", time.Now().Add(5*time.Minute).Unix())
		sess.Save()

		// Send OTP asynchronously (non-blocking)
		go func() {
			targetNumber := normalizeWhatsAppNumber(user.WhatsApp)
			if targetNumber == "" {
				targetNumber = normalizeWhatsAppNumber(user.Username)
			}
			if targetNumber == "" || strings.Contains(targetNumber, "@") || len(targetNumber) < 10 {
				log.Printf("Failed to send resend OTP: nomor WhatsApp belum valid (username=%s whatsapp_number=%s)", user.Username, user.WhatsApp)
				return
			}
			targetJID := types.NewJID(targetNumber, types.DefaultUserServer)
			if sysClient.Store.ID != nil && targetJID.User == sysClient.Store.ID.User {
				targetJID = *sysClient.Store.ID
				targetJID.Device = 0
			}

			msg := &waE2E.Message{
				Conversation: proto.String("🔐 Kode Verifikasi (Ulang): *" + otp + "*\n\nMasukkan kode ini untuk menyelesaikan verifikasi."),
			}

			send := func(timeout time.Duration) error {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				_, err := sysClient.SendMessage(ctx, targetJID, msg)
				return err
			}

			err := send(45 * time.Second)
			if err != nil && strings.Contains(strings.ToLower(err.Error()), "context deadline exceeded") {
				time.Sleep(600 * time.Millisecond)
				err = send(45 * time.Second)
			}
			if err != nil {
				log.Printf("Failed to send resend OTP to %s: %v", targetNumber, err)
				return
			}
			log.Printf("OTP resent to %s", targetNumber)
		}()

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
			if !validateEmail(req.Email) {
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
			authQueryRow("SELECT COUNT(*) FROM users WHERE email = ? AND tenant_id = ?", req.Email, tenantID).Scan(&count)
			if count > 0 {
				return c.Status(400).JSON(fiber.Map{"success": false, "message": "Email sudah terdaftar"})
			}
		}

		req.Username = normalizeWhatsAppNumber(req.Username)
		if len(req.Username) < 10 {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Nomor WhatsApp tidak valid (Wajib)"})
		}
		if errMsg := validatePasswordComplexity(req.Password); errMsg != "" {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": errMsg})
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

		_, err = authExec("INSERT INTO users (username, whatsapp_number, email, password, tenant_id, is_admin, is_active) VALUES (?, ?, ?, ?, ?, ?, ?)", req.Username, req.Username, req.Email, hashedPassword, tenantID, false, false)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Nomor WhatsApp atau Email sudah terdaftar"})
		}

		if otpDisabled() {
			_, _ = authExec("UPDATE users SET is_active = ? WHERE username = ? AND tenant_id = ?", true, req.Username, tenantID)
			return c.JSON(fiber.Map{"success": true, "require_otp": false, "message": "Pendaftaran berhasil (OTP dimatikan sementara). Silakan login."})
		}

		// SEND OTP
		sysClient := getSystemBot(tenantID)
		if sysClient == nil || !sysClient.IsConnected() {
			// Bot offline: akun tetap inactive, admin harus approve manual
			log.Printf("[REGISTER] Bot OTP offline, akun %s menunggu persetujuan admin (tenant %d)", req.Username, tenantID)
			return c.JSON(fiber.Map{
				"success":          true,
				"require_otp":      false,
				"pending_approval": true,
				"message":          "Pendaftaran berhasil. Bot OTP sedang offline, akun Anda menunggu persetujuan admin.",
			})
		}

		otp, err := generateSecureOTP()
		if err != nil {
			log.Println("Failed to generate OTP:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal generate OTP"})
		}

		// Get user ID for OTP session
		var userID int
		authQueryRow("SELECT id FROM users WHERE username = ? AND tenant_id = ?", req.Username, tenantID).Scan(&userID)

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

		// Record initial OTP send for cooldown tracking
		recordOTPResend(userID)

		// Send OTP asynchronously (non-blocking)
		go func() {
			targetNumber := normalizeWhatsAppNumber(req.Username)
			if targetNumber == "" || strings.Contains(targetNumber, "@") || len(targetNumber) < 10 {
				log.Printf("Failed to send OTP for registration: nomor WhatsApp belum valid (username=%s)", req.Username)
				return
			}
			targetJID := types.NewJID(targetNumber, types.DefaultUserServer)
			if sysClient.Store.ID != nil && targetJID.User == sysClient.Store.ID.User {
				targetJID = *sysClient.Store.ID
				targetJID.Device = 0
			}

			msg := &waE2E.Message{
				Conversation: proto.String("🔐 Kode Verifikasi Pendaftaran Wahaku: *" + otp + "*\n\nMasukkan kode ini untuk menyelesaikan pendaftaran."),
			}

			send := func(timeout time.Duration) error {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				_, err := sysClient.SendMessage(ctx, targetJID, msg)
				return err
			}

			err := send(45 * time.Second)
			if err != nil && strings.Contains(strings.ToLower(err.Error()), "context deadline exceeded") {
				time.Sleep(600 * time.Millisecond)
				err = send(45 * time.Second)
			}
			if err != nil {
				log.Printf("Failed to send OTP for registration to %s: %v", targetNumber, err)
				return
			}
			log.Printf("Registration OTP sent to %s", targetNumber)
		}()

		logAudit("REGISTER_SUCCESS", 0, tenantID, c.IP(), fmt.Sprintf("username=%s", req.Username))
		return c.JSON(fiber.Map{"success": true, "require_otp": true, "message": "Pendaftaran berhasil. Masukkan kode OTP yang dikirim ke WhatsApp."})
	})

	auth.Post("/logout", func(c *fiber.Ctx) error {
		sess, err := sessionStore.Get(c)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Session Error"})
		}

		// Capture user info before destroying session for audit log
		loggedUserID, _ := sess.Get("userID").(int)
		loggedTenantID, _ := sess.Get("tenantID").(int)

		if err := sess.Destroy(); err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Failed to destroy session"})
		}

		logAudit("LOGOUT", loggedUserID, loggedTenantID, c.IP(), "")
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
		rows, err := authQuery("SELECT filename FROM tenant_knowledge_files WHERE tenant_id = ?", tenantID)
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
		adminID, _ := c.Locals("userID").(int)
		tenantID, _ := c.Locals("tenantID").(int)
		logAudit("ADMIN_CONFIG_UPDATE", adminID, tenantID, c.IP(), "")
		return c.JSON(fiber.Map{"success": true})
	})

	api.Post("/branding/logo", requireAdmin, func(c *fiber.Ctx) error {
		file, err := c.FormFile("logo")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "File logo wajib diupload"})
		}
		if file.Size > 2*1024*1024 {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Ukuran file maksimal 2MB"})
		}

		ext := strings.ToLower(filepath.Ext(file.Filename))
		switch ext {
		case ".png", ".jpg", ".jpeg", ".webp":
		default:
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Format logo harus PNG/JPG/WEBP"})
		}

		filename := "brand-logo" + ext
		dstPath := filepath.Join("views", filename)
		if err := c.SaveFile(file, dstPath); err != nil {
			log.Println("Failed to save logo:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal menyimpan logo"})
		}

		cfg.BrandingLogo = "/" + filename
		saveConfig()

		return c.JSON(fiber.Map{"success": true, "logo": cfg.BrandingLogo})
	})

	// Follow-up Routes (unchanged)
	api.Post("/followup", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		var req struct {
			JID           string `json:"jid"`
			DelayMinutes  int    `json:"delay_minutes"`
			ScheduledTime string `json:"scheduled_time"`
			Instruction   string `json:"instruction"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid JSON"})
		}

		req.JID = strings.TrimSpace(req.JID)
		req.Instruction = strings.TrimSpace(req.Instruction)
		req.ScheduledTime = strings.TrimSpace(req.ScheduledTime)

		if req.JID == "" || req.Instruction == "" {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "jid dan instruction wajib diisi"})
		}
		if !strings.Contains(req.JID, "@") {
			req.JID = normalizeWhatsAppNumber(req.JID) + "@s.whatsapp.net"
		}

		nowUTC := time.Now().UTC()
		var scheduledTime time.Time
		if req.ScheduledTime != "" {
			if t, err := time.Parse(time.RFC3339Nano, req.ScheduledTime); err == nil {
				scheduledTime = t.UTC()
			} else if t, err := time.Parse(time.RFC3339, req.ScheduledTime); err == nil {
				scheduledTime = t.UTC()
			} else if t, err := time.ParseInLocation("2006-01-02T15:04", req.ScheduledTime, getUserTimeLocation(userID, tenantID)); err == nil {
				scheduledTime = t.UTC()
			} else {
				return c.Status(400).JSON(fiber.Map{"success": false, "message": "Format scheduled_time tidak valid"})
			}
		} else {
			if req.DelayMinutes <= 0 {
				req.DelayMinutes = 60
			}
			scheduledTime = nowUTC.Add(time.Duration(req.DelayMinutes) * time.Minute)
		}

		if scheduledTime.Before(nowUTC.Add(30 * time.Second)) {
			scheduledTime = nowUTC.Add(1 * time.Minute)
		}
		_, err := authExec("INSERT INTO followup_tasks (tenant_id, user_id, jid, scheduled_time, instruction, status) VALUES (?, ?, ?, ?, ?, 'pending')",
			tenantID, userID, req.JID, scheduledTime, req.Instruction)

		if err != nil {
			log.Println("Error creating followup:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database Error"})
		}

		return c.JSON(fiber.Map{"success": true, "message": "Follow-up scheduled", "scheduled_time": scheduledTime})
	})

	api.Get("/followup", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		rows, err := authQuery("SELECT id, jid, scheduled_time, instruction, status FROM followup_tasks WHERE user_id = ? AND tenant_id = ? AND status = 'pending' ORDER BY scheduled_time ASC", userID, tenantID)
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

		res, err := authExec("DELETE FROM followup_tasks WHERE id = ? AND user_id = ? AND tenant_id = ?", id, userID, tenantID)
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
		_, err = authExec("INSERT INTO tenant_knowledge_files (tenant_id, filename, original_name) VALUES (?, ?, ?)",
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
		err := authQueryRow("SELECT COUNT(*) FROM tenant_knowledge_files WHERE filename = ? AND tenant_id = ?", filename, tenantID).Scan(&count)
		if err != nil {
			log.Println("Error checking file ownership:", err)
			return c.Status(500).JSON(fiber.Map{"error": "Database error"})
		}
		if count == 0 {
			return c.Status(404).JSON(fiber.Map{"success": false, "message": "File tidak ditemukan di daftar."})
		}

		// Delete from DB
		authExec("DELETE FROM tenant_knowledge_files WHERE filename = ? AND tenant_id = ?", filename, tenantID)

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
				tenantID := c.Locals("tenantID").(int)
				authExec("UPDATE user_devices SET status = 'DISCONNECTED' WHERE user_id = ? AND tenant_id = ?", userID, tenantID)
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
		tenantID := c.Locals("tenantID").(int)
		isAdmin, _ := c.Locals("isAdmin").(bool)

		var username, whatsappNumber, email, tenantName, tz string
		var isActive bool
		authQueryRow("SELECT COALESCE(username, ''), COALESCE(whatsapp_number, ''), COALESCE(timezone, ''), COALESCE(email, ''), COALESCE(is_active, FALSE) FROM users WHERE id = ? AND tenant_id = ?", userID, tenantID).Scan(&username, &whatsappNumber, &tz, &email, &isActive)
		authQueryRow("SELECT COALESCE(name, '') FROM tenants WHERE id = ?", tenantID).Scan(&tenantName)
		if strings.TrimSpace(whatsappNumber) == "" {
			whatsappNumber = username
		}

		return c.JSON(fiber.Map{
			"id":            userID,
			"username":      username,
			"whatsapp_number": whatsappNumber,
			"timezone":      strings.TrimSpace(tz),
			"email":         email,
			"is_admin":      isAdmin,
			"is_platform_admin": isPlatformAdminUser(userID),
			"is_active":     isActive,
			"tenant_id":     tenantID,
			"tenant_name":   tenantName,
		})
	})

	api.Get("/profile", func(c *fiber.Ctx) error {
		return c.Redirect("/api/me")
	})

	api.Post("/profile", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		var req struct {
			Username     string `json:"username"`
			WhatsApp     string `json:"whatsapp_number"`
			Timezone     string `json:"timezone"`
			Email        string `json:"email"`
			Password     string `json:"password"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}
		req.Username = strings.TrimSpace(req.Username)
		req.WhatsApp = normalizeWhatsAppNumber(req.WhatsApp)
		req.Timezone = strings.TrimSpace(req.Timezone)
		req.Email = strings.TrimSpace(req.Email)
		if req.Username == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Username cannot be empty"})
		}
		if req.WhatsApp == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Nomor WhatsApp wajib diisi"})
		}
		if len(req.WhatsApp) < 10 || strings.Contains(req.WhatsApp, "@") {
			return c.Status(400).JSON(fiber.Map{"error": "Nomor WhatsApp tidak valid"})
		}
		if req.Email != "" && !validateEmail(req.Email) {
			return c.Status(400).JSON(fiber.Map{"error": "Email tidak valid"})
		}
		if req.Timezone != "" {
			if _, err := time.LoadLocation(req.Timezone); err != nil {
				return c.Status(400).JSON(fiber.Map{"error": "Timezone tidak valid"})
			}
		}
		var count int
		tenantID := c.Locals("tenantID").(int)
		if req.Timezone == "" {
			_ = authQueryRow("SELECT COALESCE(timezone, '') FROM users WHERE id = ? AND tenant_id = ?", userID, tenantID).Scan(&req.Timezone)
			req.Timezone = strings.TrimSpace(req.Timezone)
		}
		authQueryRow("SELECT COUNT(*) FROM users WHERE username = ? AND id != ? AND tenant_id = ?", req.Username, userID, tenantID).Scan(&count)
		if count > 0 {
			return c.Status(400).JSON(fiber.Map{"error": "Username already taken"})
		}

		authQueryRow("SELECT COUNT(*) FROM users WHERE whatsapp_number = ? AND id != ? AND tenant_id = ?", req.WhatsApp, userID, tenantID).Scan(&count)
		if count > 0 {
			return c.Status(400).JSON(fiber.Map{"error": "Nomor WhatsApp sudah terpakai"})
		}

		if req.Email != "" {
			authQueryRow("SELECT COUNT(*) FROM users WHERE email = ? AND id != ? AND tenant_id = ?", req.Email, userID, tenantID).Scan(&count)
			if count > 0 {
				return c.Status(400).JSON(fiber.Map{"error": "Email sudah terpakai"})
			}
		}

		if req.Password != "" {
			if errMsg := validatePasswordComplexity(req.Password); errMsg != "" {
				return c.Status(400).JSON(fiber.Map{"error": errMsg})
			}
			// Hash password before updating
			hashedPassword, err := hashPassword(req.Password)
			if err != nil {
				log.Println("Failed to hash password during profile update:", err)
				return c.Status(500).JSON(fiber.Map{"error": "Failed to process password"})
			}
			authExec("UPDATE users SET username = ?, whatsapp_number = ?, timezone = ?, email = ?, password = ? WHERE id = ? AND tenant_id = ?", req.Username, req.WhatsApp, req.Timezone, req.Email, hashedPassword, userID, tenantID)
		} else {
			authExec("UPDATE users SET username = ?, whatsapp_number = ?, timezone = ?, email = ? WHERE id = ? AND tenant_id = ?", req.Username, req.WhatsApp, req.Timezone, req.Email, userID, tenantID)
		}
		return c.JSON(fiber.Map{"success": true})
	})

	api.Post("/profile/timezone", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		var req struct {
			Timezone string `json:"timezone"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid JSON"})
		}
		req.Timezone = strings.TrimSpace(req.Timezone)
		if req.Timezone == "" {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "timezone wajib diisi"})
		}
		if _, err := time.LoadLocation(req.Timezone); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Timezone tidak valid"})
		}
		if _, err := authExec("UPDATE users SET timezone = ? WHERE id = ? AND tenant_id = ?", req.Timezone, userID, tenantID); err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database error"})
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

		rows, err := authQuery("SELECT device_jid, alias, status, is_primary FROM user_devices WHERE user_id = ? AND tenant_id = ?", userID, tenantID)
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
		authExec("DELETE FROM user_devices WHERE user_id = ? AND device_jid = ? AND tenant_id = ?", userID, jidStr, tenantID)

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
		_, err = authExec("UPDATE user_devices SET is_primary = false WHERE user_id = ? AND tenant_id = ?", userID, tenantID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": err.Error()})
		}

		// 2. Set new primary
		_, err = authExec("UPDATE user_devices SET is_primary = true WHERE user_id = ? AND device_jid = ? AND tenant_id = ?", userID, jidStr, tenantID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": err.Error()})
		}

		return c.JSON(fiber.Map{"success": true})
	})

	// Chat Contacts (filtered by tenant and user)
	api.Get("/chat-contacts", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		historyMutex.Lock()
		defer historyMutex.Unlock()
		contacts := []string{}
		for chatKey := range chatHistories {
			// chatKey format: "userID:jid" or just "jid" (legacy)
			parts := strings.SplitN(chatKey, ":", 2)
			if len(parts) == 2 {
				// New format: check both userID and tenant
				uidStr := parts[0]
				if uid, err := strconv.Atoi(uidStr); err == nil && uid == userID {
					// Verify user belongs to tenant
					var userTenantID int
					err := authQueryRow("SELECT tenant_id FROM users WHERE id = ?", userID).Scan(&userTenantID)
					if err == nil && userTenantID == tenantID {
						contacts = append(contacts, parts[1])
					}
				}
			}
			// Legacy format (without userID prefix) - skip for multi-tenancy
		}
		return c.JSON(fiber.Map{"success": true, "contacts": contacts})
	})

	api.Get("/wa/contacts", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		cli := getPrimaryUserClient(userID, tenantID)
		if cli == nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "WhatsApp belum terkoneksi"})
		}
		all, err := cli.Store.Contacts.GetAllContacts(context.Background())
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal mengambil kontak"})
		}
		contacts := make([]fiber.Map, 0, len(all))
		for jid, info := range all {
			name := strings.TrimSpace(info.FullName)
			if name == "" {
				name = strings.TrimSpace(info.PushName)
			}
			if name == "" {
				name = strings.TrimSpace(info.BusinessName)
			}
			if name == "" {
				name = strings.TrimSpace(info.FirstName)
			}
			contacts = append(contacts, fiber.Map{
				"jid":  jid.String(),
				"name": name,
			})
		}
		sort.Slice(contacts, func(i, j int) bool {
			ni, _ := contacts[i]["name"].(string)
			nj, _ := contacts[j]["name"].(string)
			ni = strings.ToLower(strings.TrimSpace(ni))
			nj = strings.ToLower(strings.TrimSpace(nj))
			if ni == "" && nj != "" {
				return false
			}
			if ni != "" && nj == "" {
				return true
			}
			if ni != nj {
				return ni < nj
			}
			ji, _ := contacts[i]["jid"].(string)
			jj, _ := contacts[j]["jid"].(string)
			return ji < jj
		})
		return c.JSON(fiber.Map{"success": true, "contacts": contacts})
	})

	api.Get("/wa/groups", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		cli := getPrimaryUserClient(userID, tenantID)
		if cli == nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "WhatsApp belum terkoneksi"})
		}
		groups, err := cli.GetJoinedGroups(context.Background())
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal mengambil grup"})
		}
		resp := make([]fiber.Map, 0, len(groups))
		for _, g := range groups {
			if g == nil {
				continue
			}
			participants := make([]fiber.Map, 0, len(g.Participants))
			for _, p := range g.Participants {
				display := strings.TrimSpace(p.DisplayName)
				participants = append(participants, fiber.Map{
					"jid":          p.JID.String(),
					"display_name": display,
					"is_admin":     p.IsAdmin,
					"is_superadmin": p.IsSuperAdmin,
				})
			}
			resp = append(resp, fiber.Map{
				"jid":          g.JID.String(),
				"name":         strings.TrimSpace(g.Name),
				"participants": participants,
			})
		}
		sort.Slice(resp, func(i, j int) bool {
			ni, _ := resp[i]["name"].(string)
			nj, _ := resp[j]["name"].(string)
			ni = strings.ToLower(strings.TrimSpace(ni))
			nj = strings.ToLower(strings.TrimSpace(nj))
			if ni == "" && nj != "" {
				return false
			}
			if ni != "" && nj == "" {
				return true
			}
			if ni != nj {
				return ni < nj
			}
			ji, _ := resp[i]["jid"].(string)
			jj, _ := resp[j]["jid"].(string)
			return ji < jj
		})
		return c.JSON(fiber.Map{"success": true, "groups": resp})
	})

	api.Get("/stats/messages", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		days, _ := strconv.Atoi(strings.TrimSpace(c.Query("days", "7")))
		if days <= 0 {
			days = 7
		}
		if days > 60 {
			days = 60
		}

		loc := getUserTimeLocation(userID, tenantID)
		nowLocal := time.Now().In(loc)
		endExclusiveLocal := time.Date(nowLocal.Year(), nowLocal.Month(), nowLocal.Day()+1, 0, 0, 0, 0, loc)
		startLocalBase := nowLocal.AddDate(0, 0, -(days - 1))
		startLocal := time.Date(startLocalBase.Year(), startLocalBase.Month(), startLocalBase.Day(), 0, 0, 0, 0, loc)

		startUTC := startLocal.UTC()
		endUTC := endExclusiveLocal.UTC()

		type counts struct {
			In  int
			Out int
		}
		byDay := make(map[string]*counts, days)
		for i := 0; i < days; i++ {
			d := startLocal.AddDate(0, 0, i)
			key := d.Format("2006-01-02")
			byDay[key] = &counts{}
		}

		rows, err := authQuery(
			"SELECT direction, created_at FROM message_events WHERE user_id = ? AND tenant_id = ? AND created_at >= ? AND created_at < ?",
			userID, tenantID, startUTC, endUTC,
		)
		if err == nil && rows != nil {
			for rows.Next() {
				var direction string
				var ts time.Time
				if scanErr := rows.Scan(&direction, &ts); scanErr != nil {
					continue
				}
				key := ts.In(loc).Format("2006-01-02")
				if cday, ok := byDay[key]; ok {
					switch strings.ToLower(strings.TrimSpace(direction)) {
					case "in":
						cday.In++
					case "out":
						cday.Out++
					}
				}
			}
			rows.Close()
		}

		categories := make([]string, 0, days)
		incoming := make([]int, 0, days)
		outgoing := make([]int, 0, days)
		for i := 0; i < days; i++ {
			d := startLocal.AddDate(0, 0, i)
			key := d.Format("2006-01-02")
			categories = append(categories, d.Format("02 Jan"))
			if cday := byDay[key]; cday != nil {
				incoming = append(incoming, cday.In)
				outgoing = append(outgoing, cday.Out)
			} else {
				incoming = append(incoming, 0)
				outgoing = append(outgoing, 0)
			}
		}

		return c.JSON(fiber.Map{
			"success":    true,
			"categories": categories,
			"incoming":  incoming,
			"outgoing":  outgoing,
		})
	})

	billing := api.Group("/billing")

	billing.Get("/bank", func(c *fiber.Ctx) error {
		mu.Lock()
		enabled := cfg.BillingEnabled
		bankEnabled := cfg.BillingBankEnabled
		mu.Unlock()
		if !enabled || !bankEnabled {
			return c.Status(404).JSON(fiber.Map{"success": false, "message": "Metode pembayaran tidak tersedia"})
		}
		mu.Lock()
		bankName := strings.TrimSpace(cfg.BillingBankName)
		bankAcc := strings.TrimSpace(cfg.BillingBankAccount)
		bankHolder := strings.TrimSpace(cfg.BillingBankHolder)
		notes := strings.TrimSpace(cfg.BillingNotes)
		mu.Unlock()
		return c.JSON(fiber.Map{
			"success": true,
			"bank": fiber.Map{
				"bank_name":    bankName,
				"account":      bankAcc,
				"holder":       bankHolder,
				"notes":        notes,
				"amount_idr":   65000,
				"billing_type": "manual_bank_transfer",
			},
		})
	})

	billing.Get("/plans", func(c *fiber.Ctx) error {
		mu.Lock()
		enabled := cfg.BillingEnabled
		mu.Unlock()
		if !enabled {
			return c.Status(404).JSON(fiber.Map{"success": false, "message": "Billing nonaktif"})
		}
		rows, err := authQuery("SELECT id, name, price_idr, limits_json, is_active FROM subscription_plans WHERE is_active = ? ORDER BY price_idr ASC", true)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database error"})
		}
		defer rows.Close()
		var plans []SubscriptionPlan
		for rows.Next() {
			var p SubscriptionPlan
			if scanErr := rows.Scan(&p.ID, &p.Name, &p.PriceIDR, &p.LimitsJSON, &p.IsActive); scanErr == nil {
				plans = append(plans, p)
			}
		}
		return c.JSON(fiber.Map{"success": true, "plans": plans})
	})

	billing.Get("/status", func(c *fiber.Ctx) error {
		mu.Lock()
		billingEnabled := cfg.BillingEnabled
		bankEnabled := cfg.BillingBankEnabled
		mu.Unlock()
		tenantID := c.Locals("tenantID").(int)
		ensureTenantSubscription(tenantID)
		sub, ok := getTenantSubscription(tenantID)
		if !ok {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database error"})
		}
		active := isSubscriptionActive(sub)
		return c.JSON(fiber.Map{
			"success": true,
			"active":  active,
			"billing_enabled": billingEnabled,
			"bank_enabled":    bankEnabled,
			"subscription": fiber.Map{
				"tenant_id":           sub.TenantID,
				"plan_id":             sub.PlanID,
				"status":              sub.Status,
				"current_period_end":  sub.CurrentPeriodEnd,
				"trial_end":           sub.TrialEnd,
				"grace_end":           sub.GraceEnd,
			},
		})
	})

	billing.Get("/invoices", func(c *fiber.Ctx) error {
		mu.Lock()
		enabled := cfg.BillingEnabled
		mu.Unlock()
		if !enabled {
			return c.Status(404).JSON(fiber.Map{"success": false, "message": "Billing nonaktif"})
		}
		tenantID := c.Locals("tenantID").(int)
		rows, err := authQuery("SELECT id, tenant_id, plan_id, amount_idr, status, period_start, period_end, COALESCE(proof_file, ''), COALESCE(note, ''), created_at, COALESCE(paid_at, ?) FROM subscription_invoices WHERE tenant_id = ? ORDER BY id DESC LIMIT 20", time.Time{}, tenantID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database error"})
		}
		defer rows.Close()
		var invoices []SubscriptionInvoice
		for rows.Next() {
			var inv SubscriptionInvoice
			if scanErr := rows.Scan(&inv.ID, &inv.TenantID, &inv.PlanID, &inv.AmountIDR, &inv.Status, &inv.PeriodStart, &inv.PeriodEnd, &inv.ProofFile, &inv.Note, &inv.CreatedAt, &inv.PaidAt); scanErr == nil {
				invoices = append(invoices, inv)
			}
		}
		return c.JSON(fiber.Map{"success": true, "invoices": invoices})
	})

	billing.Post("/invoice", func(c *fiber.Ctx) error {
		mu.Lock()
		enabled := cfg.BillingEnabled
		mu.Unlock()
		if !enabled {
			return c.Status(404).JSON(fiber.Map{"success": false, "message": "Billing nonaktif"})
		}
		tenantID := c.Locals("tenantID").(int)
		ensureTenantSubscription(tenantID)
		var req struct {
			PlanID int    `json:"plan_id"`
			Note   string `json:"note"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid JSON"})
		}
		if req.PlanID <= 0 {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "plan_id wajib"})
		}
		var planName string
		var amount int
		err := authQueryRow("SELECT name, price_idr FROM subscription_plans WHERE id = ? AND is_active = ?", req.PlanID, true).Scan(&planName, &amount)
		if err != nil || amount <= 0 {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Paket tidak valid"})
		}
		now := time.Now().UTC()
		periodStart := now
		periodEnd := now.AddDate(0, 1, 0)
		req.Note = strings.TrimSpace(req.Note)
		res, err := authExec("INSERT INTO subscription_invoices (tenant_id, plan_id, amount_idr, status, period_start, period_end, note) VALUES (?, ?, ?, ?, ?, ?, ?)", tenantID, req.PlanID, amount, "pending_proof", periodStart, periodEnd, req.Note)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database error"})
		}
		invoiceID64, _ := res.LastInsertId()
		invoiceID := int(invoiceID64)
		if authDialect == "postgres" {
			_ = authQueryRow("SELECT id FROM subscription_invoices WHERE tenant_id = ? ORDER BY id DESC LIMIT 1", tenantID).Scan(&invoiceID)
		}
		return c.JSON(fiber.Map{
			"success": true,
			"invoice": fiber.Map{
				"id":           invoiceID,
				"tenant_id":    tenantID,
				"plan_id":      req.PlanID,
				"plan_name":    planName,
				"amount_idr":   amount,
				"status":       "pending_proof",
				"period_start": periodStart,
				"period_end":   periodEnd,
			},
		})
	})

	billing.Post("/invoice/:id/proof", func(c *fiber.Ctx) error {
		mu.Lock()
		enabled := cfg.BillingEnabled
		bankEnabled := cfg.BillingBankEnabled
		mu.Unlock()
		if !enabled || !bankEnabled {
			return c.Status(404).JSON(fiber.Map{"success": false, "message": "Metode pembayaran tidak tersedia"})
		}
		tenantID := c.Locals("tenantID").(int)
		id, _ := strconv.Atoi(c.Params("id"))
		if id <= 0 {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "ID invoice tidak valid"})
		}
		var invTenant int
		var status string
		err := authQueryRow("SELECT tenant_id, COALESCE(status,'') FROM subscription_invoices WHERE id = ?", id).Scan(&invTenant, &status)
		if err != nil || invTenant != tenantID {
			return c.Status(404).JSON(fiber.Map{"success": false, "message": "Invoice tidak ditemukan"})
		}
		file, err := c.FormFile("file")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "File wajib diupload"})
		}
		if file.Size > 5*1024*1024 {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Ukuran file maksimal 5MB"})
		}
		ext := strings.ToLower(filepath.Ext(file.Filename))
		switch ext {
		case ".png", ".jpg", ".jpeg", ".pdf":
		default:
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Format file harus PNG/JPG/PDF"})
		}
		os.MkdirAll(filepath.Join("uploads", "billing"), 0755)
		suffix, _ := crand.Int(crand.Reader, big.NewInt(1000000))
		filename := fmt.Sprintf("inv_%d_%s_%06d%s", id, time.Now().Format("20060102_150405"), suffix.Int64(), ext)
		dstRel := filepath.Join("uploads", "billing", filename)
		if err := c.SaveFile(file, dstRel); err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal menyimpan file"})
		}
		_, err = authExec("UPDATE subscription_invoices SET proof_file = ?, status = ? WHERE id = ? AND tenant_id = ?", filename, "proof_submitted", id, tenantID)
		if err != nil {
			os.Remove(dstRel)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database error"})
		}
		return c.JSON(fiber.Map{"success": true})
	})

	billing.Get("/invoice/:id/proof", func(c *fiber.Ctx) error {
		mu.Lock()
		enabled := cfg.BillingEnabled
		bankEnabled := cfg.BillingBankEnabled
		mu.Unlock()
		if !enabled || !bankEnabled {
			return c.SendStatus(404)
		}
		tenantID := c.Locals("tenantID").(int)
		id, _ := strconv.Atoi(c.Params("id"))
		if id <= 0 {
			return c.SendStatus(404)
		}
		var invTenant int
		var filename string
		err := authQueryRow("SELECT tenant_id, COALESCE(proof_file,'') FROM subscription_invoices WHERE id = ?", id).Scan(&invTenant, &filename)
		if err != nil || invTenant != tenantID {
			return c.SendStatus(404)
		}
		filename = strings.TrimSpace(filename)
		if filename == "" {
			return c.SendStatus(404)
		}
		safe := filepath.Base(filename)
		return c.SendFile(filepath.Join("uploads", "billing", safe))
	})

	adminBilling := api.Group("/admin/billing", requirePlatformAdmin)
	adminBilling.Get("/settings", func(c *fiber.Ctx) error {
		mu.Lock()
		settings := fiber.Map{
			"billing_enabled":      cfg.BillingEnabled,
			"billing_bank_enabled": cfg.BillingBankEnabled,
			"billing_bank_name":    strings.TrimSpace(cfg.BillingBankName),
			"billing_bank_account": strings.TrimSpace(cfg.BillingBankAccount),
			"billing_bank_holder":  strings.TrimSpace(cfg.BillingBankHolder),
			"billing_notes":        strings.TrimSpace(cfg.BillingNotes),
		}
		mu.Unlock()
		return c.JSON(fiber.Map{"success": true, "settings": settings})
	})
	adminBilling.Post("/settings", func(c *fiber.Ctx) error {
		var req struct {
			BillingEnabled     *bool  `json:"billing_enabled"`
			BankEnabled        *bool  `json:"billing_bank_enabled"`
			BankName           string `json:"billing_bank_name"`
			BankAccount        string `json:"billing_bank_account"`
			BankHolder         string `json:"billing_bank_holder"`
			Notes              string `json:"billing_notes"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid JSON"})
		}
		mu.Lock()
		if req.BillingEnabled != nil {
			cfg.BillingEnabled = *req.BillingEnabled
		}
		if req.BankEnabled != nil {
			cfg.BillingBankEnabled = *req.BankEnabled
		}
		if req.BankName != "" || bytes.Contains(c.Body(), []byte(`"billing_bank_name"`)) {
			cfg.BillingBankName = strings.TrimSpace(req.BankName)
		}
		if req.BankAccount != "" || bytes.Contains(c.Body(), []byte(`"billing_bank_account"`)) {
			cfg.BillingBankAccount = strings.TrimSpace(req.BankAccount)
		}
		if req.BankHolder != "" || bytes.Contains(c.Body(), []byte(`"billing_bank_holder"`)) {
			cfg.BillingBankHolder = strings.TrimSpace(req.BankHolder)
		}
		if req.Notes != "" || bytes.Contains(c.Body(), []byte(`"billing_notes"`)) {
			cfg.BillingNotes = strings.TrimSpace(req.Notes)
		}
		mu.Unlock()
		saveConfig()
		return c.JSON(fiber.Map{"success": true})
	})
	adminBilling.Get("/invoices", func(c *fiber.Ctx) error {
		status := strings.TrimSpace(c.Query("status", ""))
		limit, _ := strconv.Atoi(c.Query("limit", "50"))
		if limit <= 0 {
			limit = 50
		}
		if limit > 200 {
			limit = 200
		}
		var rows *sql.Rows
		var err error
		if status != "" {
			rows, err = authQuery("SELECT id, tenant_id, plan_id, amount_idr, status, period_start, period_end, COALESCE(proof_file,''), COALESCE(note,''), created_at, COALESCE(paid_at, ?) FROM subscription_invoices WHERE status = ? ORDER BY id DESC LIMIT ?", time.Time{}, status, limit)
		} else {
			rows, err = authQuery("SELECT id, tenant_id, plan_id, amount_idr, status, period_start, period_end, COALESCE(proof_file,''), COALESCE(note,''), created_at, COALESCE(paid_at, ?) FROM subscription_invoices ORDER BY id DESC LIMIT ?", time.Time{}, limit)
		}
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database error"})
		}
		defer rows.Close()
		var invoices []SubscriptionInvoice
		for rows.Next() {
			var inv SubscriptionInvoice
			if scanErr := rows.Scan(&inv.ID, &inv.TenantID, &inv.PlanID, &inv.AmountIDR, &inv.Status, &inv.PeriodStart, &inv.PeriodEnd, &inv.ProofFile, &inv.Note, &inv.CreatedAt, &inv.PaidAt); scanErr == nil {
				invoices = append(invoices, inv)
			}
		}
		return c.JSON(fiber.Map{"success": true, "invoices": invoices})
	})

	adminBilling.Post("/invoices/:id/approve", func(c *fiber.Ctx) error {
		id, _ := strconv.Atoi(c.Params("id"))
		if id <= 0 {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "ID invoice tidak valid"})
		}
		var inv SubscriptionInvoice
		err := authQueryRow("SELECT id, tenant_id, plan_id, amount_idr, status, period_start, period_end, COALESCE(proof_file,''), COALESCE(note,''), created_at, COALESCE(paid_at, ?) FROM subscription_invoices WHERE id = ?", time.Time{}, id).
			Scan(&inv.ID, &inv.TenantID, &inv.PlanID, &inv.AmountIDR, &inv.Status, &inv.PeriodStart, &inv.PeriodEnd, &inv.ProofFile, &inv.Note, &inv.CreatedAt, &inv.PaidAt)
		if err != nil {
			return c.Status(404).JSON(fiber.Map{"success": false, "message": "Invoice tidak ditemukan"})
		}
		if strings.TrimSpace(inv.ProofFile) == "" {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Bukti transfer belum diupload"})
		}
		now := time.Now().UTC()
		_, _ = authExec("UPDATE subscription_invoices SET status = ?, paid_at = ? WHERE id = ?", "paid", now, id)
		upsertSQL := "INSERT INTO tenant_subscriptions (tenant_id, plan_id, status, current_period_end, trial_end, grace_end, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(tenant_id) DO UPDATE SET plan_id = excluded.plan_id, status = excluded.status, current_period_end = excluded.current_period_end, trial_end = excluded.trial_end, grace_end = excluded.grace_end, updated_at = excluded.updated_at"
		if authDialect == "postgres" {
			upsertSQL = "INSERT INTO tenant_subscriptions (tenant_id, plan_id, status, current_period_end, trial_end, grace_end, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (tenant_id) DO UPDATE SET plan_id = EXCLUDED.plan_id, status = EXCLUDED.status, current_period_end = EXCLUDED.current_period_end, trial_end = EXCLUDED.trial_end, grace_end = EXCLUDED.grace_end, updated_at = EXCLUDED.updated_at"
		}
		_, _ = authExec(upsertSQL, inv.TenantID, inv.PlanID, "active", inv.PeriodEnd, time.Time{}, time.Time{}, now)
		return c.JSON(fiber.Map{"success": true})
	})

	adminBilling.Post("/invoices/:id/reject", func(c *fiber.Ctx) error {
		id, _ := strconv.Atoi(c.Params("id"))
		if id <= 0 {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "ID invoice tidak valid"})
		}
		var req struct {
			Note string `json:"note"`
		}
		_ = c.BodyParser(&req)
		req.Note = strings.TrimSpace(req.Note)
		_, err := authExec("UPDATE subscription_invoices SET status = ?, note = ? WHERE id = ?", "rejected", req.Note, id)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database error"})
		}
		return c.JSON(fiber.Map{"success": true})
	})

	api.Get("/user/config", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		return c.JSON(fiber.Map{
			"success":       true,
			"system_prompt": getUserSystemPrompt(userID, tenantID),
		})
	})

	api.Post("/user/config", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		var req struct {
			SystemPrompt string `json:"system_prompt"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid JSON"})
		}
		if err := setUserSystemPrompt(userID, tenantID, req.SystemPrompt); err != nil {
			log.Println("Failed to save user system prompt:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal menyimpan prompt"})
		}
		return c.JSON(fiber.Map{"success": true})
	})

	api.Get("/user/ai-config", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		cfg, ok := getUserAIConfig(userID, tenantID)
		if !ok {
			cfg = UserAIConfig{ActiveProvider: "", Providers: map[string]ProviderConfig{}}
		}
		return c.JSON(fiber.Map{
			"success":         true,
			"active_provider": cfg.ActiveProvider,
			"providers":       cfg.Providers,
		})
	})

	api.Post("/user/ai-config", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		var req UserAIConfig
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid JSON"})
		}

		req.ActiveProvider = strings.TrimSpace(req.ActiveProvider)
		if req.Providers == nil {
			req.Providers = map[string]ProviderConfig{}
		}

		allowed := map[string]bool{
			"gemini":   true,
			"vertex":   true,
			"openai":   true,
			"sumopod":  true,
			"groq":     true,
			"qwen":     true,
			"byteplus": true,
			"deepseek": true,
		}
		for k := range req.Providers {
			if !allowed[k] {
				delete(req.Providers, k)
			}
		}

		for name, p := range req.Providers {
			if strings.TrimSpace(p.BaseURL) != "" && name != "vertex" {
				validated, err := validateOutboundBaseURL(p.BaseURL)
				if err != nil {
					log.Printf("[AI-CONFIG] base_url validation failed for provider=%s url=%s err=%v", name, p.BaseURL, err)
					return c.Status(400).JSON(fiber.Map{"success": false, "message": fmt.Sprintf("base_url provider '%s' tidak diizinkan: %s", name, err.Error())})
				}
				p.BaseURL = validated
				req.Providers[name] = p
			}
		}

		if req.ActiveProvider == "" {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Active provider wajib dipilih"})
		}
		if !allowed[req.ActiveProvider] {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Provider tidak valid: " + req.ActiveProvider})
		}
		p, ok := req.Providers[req.ActiveProvider]
		if !ok {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Konfigurasi provider '" + req.ActiveProvider + "' belum ada"})
		}
		if strings.TrimSpace(p.APIKey) == "" {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "API Key wajib diisi untuk provider " + req.ActiveProvider})
		}
		// Model boleh kosong — user bisa isi manual atau belum fetch models
		// Hanya warn di log, tidak reject

		if err := setUserAIConfig(userID, tenantID, req); err != nil {
			log.Println("Failed to save user ai config:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal menyimpan konfigurasi AI"})
		}
		return c.JSON(fiber.Map{"success": true})
	})

	type ChatLogEntry struct {
		Role      string `json:"role"`
		Message   string `json:"message"`
		Timestamp int64  `json:"timestamp"`
	}

	api.Get("/chat-logs", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		jid := c.Query("jid")
		if strings.TrimSpace(jid) == "" {
			return c.Status(400).JSON(fiber.Map{"success": false, "error": "jid wajib diisi"})
		}

		key := fmt.Sprintf("%d:%s", userID, jid)

		historyMutex.Lock()
		lines := append([]string(nil), chatHistories[key]...)
		historyMutex.Unlock()

		now := time.Now()
		logs := make([]ChatLogEntry, 0, len(lines))
		for i, line := range lines {
			raw := strings.TrimSpace(line)
			role := "assistant"
			msg := raw
			if strings.HasPrefix(raw, "User:") {
				role = "user"
				msg = strings.TrimSpace(strings.TrimPrefix(raw, "User:"))
			} else if strings.HasPrefix(raw, "Assistant:") {
				role = "assistant"
				msg = strings.TrimSpace(strings.TrimPrefix(raw, "Assistant:"))
			}
			ts := now.Add(-time.Duration(len(lines)-i) * time.Second).UnixMilli()
			logs = append(logs, ChatLogEntry{Role: role, Message: msg, Timestamp: ts})
		}

		return c.JSON(fiber.Map{"success": true, "logs": logs})
	})

	api.Delete("/chat-logs", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		jid := c.Query("jid")
		if strings.TrimSpace(jid) == "" {
			return c.Status(400).JSON(fiber.Map{"success": false, "error": "jid wajib diisi"})
		}

		key := fmt.Sprintf("%d:%s", userID, jid)

		historyMutex.Lock()
		delete(chatHistories, key)
		historyMutex.Unlock()

		return c.JSON(fiber.Map{"success": true})
	})

	api.Post("/reset-memory", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		var req struct {
			Target string `json:"target"`
			JID    string `json:"jid"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "error": "Invalid JSON"})
		}

		historyMutex.Lock()
		defer historyMutex.Unlock()

		if req.Target == "all_chats" || strings.TrimSpace(req.Target) == "" {
			prefix := fmt.Sprintf("%d:", userID)
			for k := range chatHistories {
				if strings.HasPrefix(k, prefix) {
					delete(chatHistories, k)
				}
			}
			return c.JSON(fiber.Map{"success": true})
		}

		if strings.TrimSpace(req.JID) != "" {
			key := fmt.Sprintf("%d:%s", userID, req.JID)
			delete(chatHistories, key)
			return c.JSON(fiber.Map{"success": true})
		}

		return c.Status(400).JSON(fiber.Map{"success": false, "error": "target tidak dikenal"})
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
		userID := c.Locals("userID").(int)
		tenantID := c.Locals("tenantID").(int)
		isAdmin, _ := c.Locals("isAdmin").(bool)
		providerName, _, cfgErr := getActiveProviderConfig(userID, tenantID, isAdmin)
		if cfgErr != "" {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": cfgErr, "provider": providerName})
		}
		reply := callAI(userID, tenantID, isAdmin, prompt)
		return c.JSON(fiber.Map{"success": true, "reply": reply, "provider": providerName})
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
		rows, err := authQuery("SELECT id, username, is_admin, is_active FROM users WHERE tenant_id = ? ORDER BY id DESC", tenantID)
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
		_, err = authExec("INSERT INTO users (username, password, tenant_id, is_admin, is_active) VALUES (?, ?, ?, ?, ?)",
			req.Username, hashedPassword, tenantID, req.IsAdmin, req.IsActive)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Username already exists or Database error"})
		}
		adminID, _ := c.Locals("userID").(int)
		logAudit("ADMIN_USER_CREATE", adminID, tenantID, c.IP(), fmt.Sprintf("new_username=%s", req.Username))
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
			authExec("UPDATE users SET username = ?, password = ?, is_admin = ?, is_active = ? WHERE id = ? AND tenant_id = ?",
				req.Username, hashedPassword, req.IsAdmin, req.IsActive, id, tenantID)
		} else {
			authExec("UPDATE users SET username = ?, is_admin = ?, is_active = ? WHERE id = ? AND tenant_id = ?",
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
		authExec("DELETE FROM users WHERE id = ? AND tenant_id = ?", id, tenantID)
		logAudit("ADMIN_USER_DELETE", myID, tenantID, c.IP(), fmt.Sprintf("deleted_user_id=%s", id))
		return c.JSON(fiber.Map{"success": true})
	})

	// --- TENANT MANAGEMENT ROUTES (Admin Only) ---
	tenantGroup := api.Group("/tenants")

	// List tenants
	tenantGroup.Get("/", func(c *fiber.Ctx) error {
		if isAdmin, ok := c.Locals("isAdmin").(bool); !ok || !isAdmin {
			return c.Status(403).JSON(fiber.Map{"error": "Requires Admin privileges"})
		}
		rows, err := authQuery("SELECT id, name, created_at FROM tenants ORDER BY id ASC")
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Database error"})
		}
		defer rows.Close()
		var tenants []fiber.Map
		for rows.Next() {
			var id int
			var name string
			var createdAt time.Time
			if err := rows.Scan(&id, &name, &createdAt); err == nil {
				tenants = append(tenants, fiber.Map{
					"id":         id,
					"name":       name,
					"created_at": createdAt.Format(time.RFC3339),
				})
			}
		}
		return c.JSON(fiber.Map{"tenants": tenants})
	})

	// Create tenant
	tenantGroup.Post("/", func(c *fiber.Ctx) error {
		if isAdmin, ok := c.Locals("isAdmin").(bool); !ok || !isAdmin {
			return c.Status(403).JSON(fiber.Map{"error": "Requires Admin privileges"})
		}
		var req struct {
			Name string `json:"name"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}
		if req.Name == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Tenant name required"})
		}

		// Insert tenant
		if authDialect == "postgres" {
			var tenantID int
			if err := authQueryRow("INSERT INTO tenants (name) VALUES (?) RETURNING id", req.Name).Scan(&tenantID); err != nil {
				return c.Status(500).JSON(fiber.Map{"error": "Tenant name already exists or database error"})
			}
			return c.JSON(fiber.Map{"success": true, "tenant_id": tenantID})
		}
		result, err := authExec("INSERT INTO tenants (name) VALUES (?)", req.Name)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Tenant name already exists or database error"})
		}
		tenantID, _ := result.LastInsertId()
		return c.JSON(fiber.Map{"success": true, "tenant_id": tenantID})
	})

	// Delete tenant
	tenantGroup.Delete("/:id", func(c *fiber.Ctx) error {
		if isAdmin, ok := c.Locals("isAdmin").(bool); !ok || !isAdmin {
			return c.Status(403).JSON(fiber.Map{"error": "Requires Admin privileges"})
		}
		id := c.Params("id")
		currentTenantID := c.Locals("tenantID").(int)
		idInt, err := strconv.Atoi(id)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid tenant id"})
		}

		// Prevent deleting current tenant
		if currentTenantID == idInt {
			return c.Status(400).JSON(fiber.Map{"error": "Cannot delete current tenant"})
		}

		// Delete users and devices for this tenant first (cascade should handle but explicit)
		authExec("DELETE FROM user_devices WHERE tenant_id = ?", idInt)
		authExec("DELETE FROM followup_tasks WHERE tenant_id = ?", idInt)
		authExec("DELETE FROM tenant_knowledge_files WHERE tenant_id = ?", idInt)
		authExec("DELETE FROM users WHERE tenant_id = ?", idInt)

		// Delete tenant
		authExec("DELETE FROM tenants WHERE id = ?", idInt)

		return c.JSON(fiber.Map{"success": true})
	})

	api.Post("/models", fetchModelsHandler)
	api.Get("/public/branding", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"success": true,
			"name":    cfg.BrandingName,
			"logo":    cfg.BrandingLogo,
			"version": cfg.BrandingVersion,
		})
	})
	api.Get("/version", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"success":         true,
			"build_version":   buildVersion,
			"display_version": cfg.BrandingVersion,
			"build_time":      buildTime,
			"features":        []string{"multi-device", "otp-login", "multi-tenancy", "tenant-management"},
		})
	})

	log.Println(cfg.BrandingName + " Service Starting...")
	log.Println("Build Version:", buildVersion, "Display Version:", cfg.BrandingVersion)
	log.Println("Server running on http://localhost:" + cfg.AppPort)
	log.Fatal(app.Listen(":" + cfg.AppPort))
}

// --- MULTI USER LOGIC ---

func initAdminClient() {
	// Initialize admin clients for all tenants
	rows, err := authQuery("SELECT id FROM tenants")
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
		err := authQueryRow("SELECT id FROM users WHERE is_admin = ? AND tenant_id = ? LIMIT 1", true, tenantID).Scan(&adminID)
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
	err := authQueryRow("SELECT id FROM users WHERE is_admin = ? AND tenant_id = ? LIMIT 1", true, tenantID).Scan(&adminID)
	if err != nil {
		return nil
	}

	// 1. Try Primary Device
	var primaryJID string
	err = authQueryRow("SELECT device_jid FROM user_devices WHERE user_id = ? AND is_primary = ? AND tenant_id = ?", adminID, true, tenantID).Scan(&primaryJID)
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

func getPrimaryUserClient(userID, tenantID int) *whatsmeow.Client {
	var primaryJID string
	if err := authQueryRow("SELECT device_jid FROM user_devices WHERE user_id = ? AND is_primary = ? AND tenant_id = ?", userID, true, tenantID).Scan(&primaryJID); err == nil {
		primaryJID = strings.TrimSpace(primaryJID)
		if primaryJID != "" {
			if cli := getUserDeviceClient(userID, primaryJID); cli != nil {
				return cli
			}
		}
	}
	return getUserClient(userID)
}

func recordMessageEvent(tenantID, userID int, chatJID, direction string, createdAt time.Time) {
	chatJID = strings.TrimSpace(chatJID)
	direction = strings.ToLower(strings.TrimSpace(direction))
	if direction != "in" && direction != "out" {
		return
	}
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	createdAt = createdAt.UTC()
	_, _ = authExec(
		"INSERT INTO message_events (tenant_id, user_id, chat_jid, direction, created_at) VALUES (?, ?, ?, ?, ?)",
		tenantID, userID, chatJID, direction, createdAt,
	)
}

type SubscriptionPlan struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	PriceIDR    int    `json:"price_idr"`
	LimitsJSON  string `json:"limits_json"`
	IsActive    bool   `json:"is_active"`
}

type TenantSubscription struct {
	TenantID         int       `json:"tenant_id"`
	PlanID           int       `json:"plan_id"`
	Status           string    `json:"status"`
	CurrentPeriodEnd time.Time `json:"current_period_end"`
	TrialEnd         time.Time `json:"trial_end"`
	GraceEnd         time.Time `json:"grace_end"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type SubscriptionInvoice struct {
	ID          int       `json:"id"`
	TenantID    int       `json:"tenant_id"`
	PlanID      int       `json:"plan_id"`
	AmountIDR   int       `json:"amount_idr"`
	Status      string    `json:"status"`
	PeriodStart time.Time `json:"period_start"`
	PeriodEnd   time.Time `json:"period_end"`
	ProofFile   string    `json:"proof_file"`
	Note        string    `json:"note"`
	CreatedAt   time.Time `json:"created_at"`
	PaidAt      time.Time `json:"paid_at"`
}

func ensureBillingSchema() {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS subscription_plans (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			price_idr INTEGER NOT NULL,
			limits_json TEXT NOT NULL DEFAULT '{}',
			is_active BOOLEAN NOT NULL DEFAULT 1,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS tenant_subscriptions (
			tenant_id INTEGER PRIMARY KEY,
			plan_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			current_period_end DATETIME,
			trial_end DATETIME,
			grace_end DATETIME,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY(tenant_id) REFERENCES tenants(id)
		)`,
		`CREATE TABLE IF NOT EXISTS subscription_invoices (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id INTEGER NOT NULL,
			plan_id INTEGER NOT NULL,
			amount_idr INTEGER NOT NULL,
			status TEXT NOT NULL,
			period_start DATETIME,
			period_end DATETIME,
			proof_file TEXT,
			note TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			paid_at DATETIME,
			FOREIGN KEY(tenant_id) REFERENCES tenants(id)
		)`,
		`CREATE INDEX IF NOT EXISTS subscription_invoices_tenant_status_idx ON subscription_invoices(tenant_id, status)`,
	}

	if authDialect == "postgres" {
		stmts = []string{
			`CREATE TABLE IF NOT EXISTS subscription_plans (
				id SERIAL PRIMARY KEY,
				name TEXT NOT NULL,
				price_idr INTEGER NOT NULL,
				limits_json TEXT NOT NULL DEFAULT '{}',
				is_active BOOLEAN NOT NULL DEFAULT TRUE,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)`,
			`CREATE TABLE IF NOT EXISTS tenant_subscriptions (
				tenant_id INTEGER PRIMARY KEY REFERENCES tenants(id),
				plan_id INTEGER NOT NULL REFERENCES subscription_plans(id),
				status TEXT NOT NULL,
				current_period_end TIMESTAMPTZ,
				trial_end TIMESTAMPTZ,
				grace_end TIMESTAMPTZ,
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			)`,
			`CREATE TABLE IF NOT EXISTS subscription_invoices (
				id SERIAL PRIMARY KEY,
				tenant_id INTEGER NOT NULL REFERENCES tenants(id),
				plan_id INTEGER NOT NULL REFERENCES subscription_plans(id),
				amount_idr INTEGER NOT NULL,
				status TEXT NOT NULL,
				period_start TIMESTAMPTZ,
				period_end TIMESTAMPTZ,
				proof_file TEXT,
				note TEXT,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				paid_at TIMESTAMPTZ
			)`,
			`CREATE INDEX IF NOT EXISTS subscription_invoices_tenant_status_idx ON subscription_invoices(tenant_id, status)`,
		}
	}

	for _, s := range stmts {
		_, _ = authExec(s)
	}

	var planCount int
	_ = authQueryRow("SELECT COUNT(*) FROM subscription_plans").Scan(&planCount)
	if planCount == 0 {
		limits := `{"max_users":3,"max_devices":1,"messages_per_month":2000}`
		_, _ = authExec("INSERT INTO subscription_plans (name, price_idr, limits_json, is_active) VALUES (?, ?, ?, ?)", "Basic", 65000, limits, true)
	}
}

func ensureTenantSubscription(tenantID int) {
	var count int
	_ = authQueryRow("SELECT COUNT(*) FROM tenant_subscriptions WHERE tenant_id = ?", tenantID).Scan(&count)
	if count > 0 {
		return
	}
	var planID int
	err := authQueryRow("SELECT id FROM subscription_plans WHERE is_active = ? ORDER BY price_idr ASC LIMIT 1", true).Scan(&planID)
	if err != nil || planID <= 0 {
		err = authQueryRow("SELECT id FROM subscription_plans ORDER BY id ASC LIMIT 1").Scan(&planID)
	}
	if planID <= 0 {
		return
	}
	now := time.Now().UTC()
	trialEnd := now.Add(7 * 24 * time.Hour)
	_, _ = authExec(
		"INSERT INTO tenant_subscriptions (tenant_id, plan_id, status, current_period_end, trial_end, grace_end, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		tenantID, planID, "trial", time.Time{}, trialEnd, time.Time{}, now,
	)
}

func getTenantSubscription(tenantID int) (TenantSubscription, bool) {
	var sub TenantSubscription
	sub.TenantID = tenantID
	err := authQueryRow(
		"SELECT plan_id, COALESCE(status, ''), COALESCE(current_period_end, ?), COALESCE(trial_end, ?), COALESCE(grace_end, ?), COALESCE(updated_at, ?) FROM tenant_subscriptions WHERE tenant_id = ?",
		time.Time{}, time.Time{}, time.Time{}, time.Time{}, tenantID,
	).Scan(&sub.PlanID, &sub.Status, &sub.CurrentPeriodEnd, &sub.TrialEnd, &sub.GraceEnd, &sub.UpdatedAt)
	if err != nil {
		return TenantSubscription{}, false
	}
	sub.Status = strings.TrimSpace(sub.Status)
	return sub, true
}

func isSubscriptionActive(sub TenantSubscription) bool {
	now := time.Now().UTC()
	if strings.ToLower(strings.TrimSpace(sub.Status)) == "active" && !sub.CurrentPeriodEnd.IsZero() && now.Before(sub.CurrentPeriodEnd) {
		return true
	}
	if strings.ToLower(strings.TrimSpace(sub.Status)) == "trial" && !sub.TrialEnd.IsZero() && now.Before(sub.TrialEnd) {
		return true
	}
	if strings.ToLower(strings.TrimSpace(sub.Status)) == "past_due" && !sub.GraceEnd.IsZero() && now.Before(sub.GraceEnd) {
		return true
	}
	return false
}

func isPlatformAdminUser(userID int) bool {
	mu.Lock()
	adminUsername := strings.TrimSpace(cfg.AdminUsername)
	mu.Unlock()
	if adminUsername == "" {
		return false
	}
	var count int
	_ = authQueryRow("SELECT COUNT(*) FROM users WHERE id = ? AND COALESCE(is_admin, FALSE) = ? AND username = ?", userID, true, adminUsername).Scan(&count)
	return count > 0
}

func startAllUserDevices(userID int, tenantID int) {
	rows, err := authQuery("SELECT device_jid FROM user_devices WHERE user_id = ? AND tenant_id = ?", userID, tenantID)
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

		log.Printf("[User %d][%s] Received message (len=%d)", userID, key, len(msg))

		var tenantID int
		_ = authQueryRow("SELECT tenant_id FROM users WHERE id = ?", userID).Scan(&tenantID)
		if tenantID <= 0 {
			tenantID = 1
		}
		recordMessageEvent(tenantID, userID, v.Info.Chat.String(), "in", v.Info.Timestamp)

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
			log.Printf("[User %d] Processing message (len=%d)", userID, len(msg))

			var isAdmin bool
			err := authQueryRow("SELECT tenant_id, COALESCE(is_admin, FALSE) FROM users WHERE id = ?", userID).Scan(&tenantID, &isAdmin)
			if err != nil {
				log.Println("Error getting tenant for user", userID, ":", err)
				tenantID = 1
				isAdmin = false
			}
			ensureTenantSubscription(tenantID)
			if sub, ok := getTenantSubscription(tenantID); !ok || !isSubscriptionActive(sub) {
				log.Printf("[User %d] Subscription inactive for tenant %d, skipping reply", userID, tenantID)
				return
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

			reply := callAI(userID, tenantID, isAdmin, msg)
			log.Printf("[User %d] AI Reply generated (len=%d)", userID, len(reply))

			imageURL, caption, text := parseAIMediaCommand(reply)
			historyMutex.Lock()
			if strings.TrimSpace(imageURL) != "" {
				if strings.TrimSpace(caption) != "" {
					chatHistories[userChatKey] = append(chatHistories[userChatKey], "Assistant: "+caption)
				} else {
					chatHistories[userChatKey] = append(chatHistories[userChatKey], "Assistant: [image]")
				}
			} else {
				if strings.TrimSpace(text) == "" {
					chatHistories[userChatKey] = append(chatHistories[userChatKey], "Assistant: "+reply)
				} else {
					chatHistories[userChatKey] = append(chatHistories[userChatKey], "Assistant: "+text)
				}
			}
			historyMutex.Unlock()

			sendErr := sendAIReply(cli, tenantID, userID, v.Info.Chat, reply)
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
		err := authQueryRow("SELECT tenant_id FROM users WHERE id = ?", userID).Scan(&tenantID)
		if err != nil {
			log.Println("Error getting tenant for user", userID, ":", err)
			tenantID = 1 // fallback
		}

		// 1. Check if device already exists for this user
		var exists int
		err = authQueryRow("SELECT COUNT(*) FROM user_devices WHERE user_id = ? AND device_jid = ? AND tenant_id = ?", userID, newJID, tenantID).Scan(&exists)
		if err != nil {
			log.Println("Error checking device existence:", err)
		}

		if exists > 0 {
			log.Printf("Device %s already exists for user %d. Updating status instead of inserting.", newJID, userID)
			_, err = authExec("UPDATE user_devices SET status = 'CONNECTED', alias = 'WhatsApp Device' WHERE user_id = ? AND device_jid = ? AND tenant_id = ?", userID, newJID, tenantID)
			if err != nil {
				log.Println("Error updating existing device:", err)
			}
		} else {
			// 2. Save to DB if not exists
			_, err = authExec("INSERT INTO user_devices (tenant_id, user_id, device_jid, alias, status) VALUES (?, ?, ?, ?, ?)",
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

func getActiveProviderConfig(userID, tenantID int, isAdmin bool) (string, ProviderConfig, string) {
	userCfg, ok := getUserAIConfig(userID, tenantID)
	if ok {
		providerName := strings.TrimSpace(userCfg.ActiveProvider)
		if providerName == "" {
			return "", ProviderConfig{}, "AI untuk akun ini belum dikonfigurasi (Active Provider kosong). Buka AI Brain dan pilih provider."
		}
		pConfig, exists := userCfg.Providers[providerName]
		if !exists {
			return providerName, ProviderConfig{}, "AI untuk akun ini belum dikonfigurasi untuk provider '" + providerName + "'."
		}
		if strings.TrimSpace(pConfig.APIKey) == "" {
			return providerName, ProviderConfig{}, "API Key untuk akun ini masih kosong. Isi API Key di AI Brain lalu Simpan."
		}
		if strings.TrimSpace(pConfig.Model) == "" {
			return providerName, ProviderConfig{}, "Model untuk akun ini masih kosong. Pilih Model di AI Brain lalu Simpan."
		}
		return providerName, pConfig, ""
	}

	if !isAdmin {
		return "", ProviderConfig{}, "AI untuk akun ini belum dikonfigurasi. Buka AI Brain → isi API Key dan Model → Simpan."
	}

	mu.Lock()
	providerName := cfg.ActiveProvider
	providers := cfg.Providers
	mu.Unlock()

	if providerName == "" {
		providerName = "gemini"
	}
	if providers == nil {
		return providerName, ProviderConfig{}, "AI belum dikonfigurasi. Buka Dashboard → AI Brain → isi API Key dan Model."
	}

	pConfig, ok2 := providers[providerName]
	if !ok2 {
		return providerName, ProviderConfig{}, "AI belum dikonfigurasi untuk provider '" + providerName + "'. Buka Dashboard → AI Brain dan pilih provider yang benar."
	}
	if strings.TrimSpace(pConfig.APIKey) == "" {
		return providerName, ProviderConfig{}, "API Key untuk provider '" + providerName + "' masih kosong. Buka Dashboard → AI Brain → isi API Key, lalu Simpan."
	}
	if strings.TrimSpace(pConfig.Model) == "" {
		return providerName, ProviderConfig{}, "Model untuk provider '" + providerName + "' masih kosong. Buka Dashboard → AI Brain → pilih Model, lalu Simpan."
	}

	return providerName, pConfig, ""
}

func callAI(userID, tenantID int, isAdmin bool, prompt string) string {
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

	providerName, pConfig, cfgErr := getActiveProviderConfig(userID, tenantID, isAdmin)
	if cfgErr != "" {
		return cfgErr
	}

	sysPrompt := ""
	if isAdmin {
		sysPrompt = getUserSystemPrompt(userID, tenantID)
		if sysPrompt == "" {
			mu.Lock()
			sysPrompt = cfg.SystemPrompt
			mu.Unlock()
		}
	} else {
		sysPrompt = defaultSystemPromptForProvider(providerName, pConfig.Model)
	}
	if sysPrompt == "" {
		sysPrompt = "You are a helpful assistant."
	}

	fullPrompt := fmt.Sprintf("%s\n\nContext:\n%s\n\nUser Question: %s", sysPrompt, contextText, prompt)

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

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient(30 * time.Second).Do(req)
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

	resp, err := httpClient(30 * time.Second).Do(req)
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

	resp, err := httpClient(30 * time.Second).Do(req)
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

	resp, err := httpClient(30 * time.Second).Do(req)
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
	raw, err := os.ReadFile(configFile)
	if err != nil {
		cfg = Config{
			AppPort:         "4500",
			AdminUsername:   "admin",
			BrandingName:    "Wahaku",
			BrandingLogo:    "",
			BrandingVersion: buildVersion,
			BillingEnabled:     true,
			BillingBankEnabled: true,
			OTPEnabled:      true,
			Providers:       make(map[string]ProviderConfig),
			SavedPrompts:    make(map[string]string),
		}
		saveConfig()
		return
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		cfg = Config{
			AppPort:         "4500",
			AdminUsername:   "admin",
			BrandingName:    "Wahaku",
			BrandingLogo:    "",
			BrandingVersion: buildVersion,
			OTPEnabled:      true,
			Providers:       make(map[string]ProviderConfig),
			SavedPrompts:    make(map[string]string),
		}
		saveConfig()
		return
	}

	// Ensure defaults
	if cfg.AppPort == "" {
		cfg.AppPort = "4500"
	}
	if !bytes.Contains(raw, []byte(`"otp_enabled"`)) {
		cfg.OTPEnabled = true
	}
	if !bytes.Contains(raw, []byte(`"billing_enabled"`)) {
		cfg.BillingEnabled = true
	}
	if !bytes.Contains(raw, []byte(`"billing_bank_enabled"`)) {
		cfg.BillingBankEnabled = true
	}
	if cfg.Providers == nil {
		cfg.Providers = make(map[string]ProviderConfig)
	}
	if strings.TrimSpace(cfg.BrandingName) == "" {
		cfg.BrandingName = "Wahaku"
	}
	if strings.TrimSpace(cfg.BrandingVersion) == "" {
		cfg.BrandingVersion = buildVersion
	}
	if strings.TrimSpace(cfg.AdminPassword) == "password" {
		cfg.AdminPassword = ""
		saveConfig()
	}
}

func saveConfig() {
	data, _ := json.MarshalIndent(cfg, "", "  ")
	if err := os.WriteFile(configFile, data, 0600); err != nil {
		log.Println("Failed to write config:", err)
	}
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
	rows, err := authQuery("SELECT filename FROM tenant_knowledge_files WHERE tenant_id = ?", tenantID)
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
	rows, err := authQuery("SELECT id FROM tenants")
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

		query := "SELECT id, user_id, tenant_id, jid, instruction FROM followup_tasks WHERE status = 'pending' AND scheduled_time <= " + pendingNowSQL()
		rows, err := authQuery(query)
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

			ensureTenantSubscription(t.TenantID)
			if sub, ok := getTenantSubscription(t.TenantID); !ok || !isSubscriptionActive(sub) {
				continue
			}

			// Generate Message
			var isAdmin bool
			_ = authQueryRow("SELECT COALESCE(is_admin, FALSE) FROM users WHERE id = ? AND tenant_id = ?", t.UserID, t.TenantID).Scan(&isAdmin)
			reply := callAI(t.UserID, t.TenantID, isAdmin, "Generate a follow-up message for: "+t.Instruction)

			// Send
			cli := getUserClient(t.UserID)
			if cli != nil && cli.IsConnected() {
				remoteJID, _ := types.ParseJID(t.JID)
				if sendErr := sendAIReply(cli, t.TenantID, t.UserID, remoteJID, reply); sendErr == nil {
					authExec("UPDATE followup_tasks SET status = 'completed' WHERE id = ? AND tenant_id = ?", t.ID, t.TenantID)
				} else {
					log.Println("Failed to send followup task:", t.ID, "err:", sendErr)
				}
			} else {
				log.Println("User client not connected for task:", t.ID)
				// Retry later? Or mark failed?
			}
		}
	}
}

func fetchModelsHandler(c *fiber.Ctx) error {
	isAdmin, _ := c.Locals("isAdmin").(bool)
	var req struct {
		Provider string `json:"provider"`
		APIKey   string `json:"api_key"`
		BaseURL  string `json:"base_url"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
	}
	provider := strings.ToLower(strings.TrimSpace(req.Provider))
	apiKey := strings.TrimSpace(req.APIKey)
	baseURL := strings.TrimSpace(req.BaseURL)

	if provider == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Provider required"})
	}
	if apiKey == "" {
		if !isAdmin {
			return c.Status(400).JSON(fiber.Map{"error": "API Key required"})
		}
		if p, ok := cfg.Providers[provider]; ok {
			apiKey = strings.TrimSpace(p.APIKey)
			if baseURL == "" {
				baseURL = strings.TrimSpace(p.BaseURL)
			}
		}
	}
	if apiKey == "" && provider != "ollama" && provider != "vertex" {
		return c.Status(400).JSON(fiber.Map{"error": "API Key required"})
	}
	if baseURL != "" && provider != "vertex" {
		validated, err := validateOutboundBaseURL(baseURL)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "base_url tidak diizinkan"})
		}
		baseURL = validated
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
		log.Printf("[MODELS] provider=%s baseURL=%s err=%v", provider, baseURL, err)
		return c.Status(500).JSON(fiber.Map{"error": "Gagal memuat model dari provider: " + err.Error()})
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

	req, _ := http.NewRequest("GET", url, nil)
	resp, err := httpClient(15 * time.Second).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("API Error: %s", resp.Status)
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
	resp, err := httpClient(15 * time.Second).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API Error: %s", resp.Status)
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

	resp, err := httpClient(15 * time.Second).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("API Error: %s", resp.Status)
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
