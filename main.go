package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"math/rand"

	"github.com/PuerkitoBio/goquery"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq" // PostgreSQL Driver
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/session"
	"github.com/ledongthuc/pdf"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"
	"google.golang.org/protobuf/proto"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// --- CONFIG ---
type ProviderConfig struct {
	APIKey  string `json:"api_key"`
	Model   string `json:"model"`
	BaseURL string `json:"base_url"`
}

type DBConfig struct {
	Type     string `json:"type"` // "mysql" or "postgres"
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
	client        *whatsmeow.Client
	qrCode        string
	status        string = "STARTING"
	cfg           Config
	configFile    = "config.json"
	connected     bool
	container     *sqlstore.Container
	mu            sync.Mutex // Global Mutex
	knowledgeText string     // Combined scraped & file text
	appDB         *sql.DB    // Application Database (MySQL)
	dbSchema      string     // Table schema for AI
	sheetsService *sheets.Service // Google Sheets Service
	sheetSchema   string          // Sheet names & headers for AI
	store         *session.Store  // Session Store
	authDB        *sql.DB         // SQLite for Users
	chatHistories = make(map[string][]string) // Chat History Memory
	historyMutex  sync.Mutex      // Mutex for Chat History
)

type User struct {
	ID       int    `json:"id"`
	Username string `json:"username"`
	Password string `json:"password"`
	IsAdmin  bool   `json:"is_admin"`
	IsActive bool   `json:"is_active"`
}

type FollowupTask struct {
	ID            int       `json:"id"`
	UserID        int       `json:"user_id"`
	JID           string    `json:"jid"`
	ScheduledTime time.Time `json:"scheduled_time"`
	Instruction   string    `json:"instruction"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}

func main() {
	// 1. Load Config
	loadConfig()

	// Connect to DB & Sheets in background
	go func() {
		connectAppDB()
		connectSheets()
	}()

	// 2. Setup Database
	dbLog := waLog.Stdout("Database", "ERROR", true)
	var err error

	// Open Shared SQLite Connection to prevent locking issues
	// We use the same connection for Whatsmeow and AuthDB
	// Removed WAL mode for better compatibility on some VPS
	sharedDB, err := sql.Open("sqlite", "file:wahaku.db?_pragma=foreign_keys(1)&_busy_timeout=5000")
	if err != nil {
		log.Fatal("Failed to open Shared DB:", err)
	}
	sharedDB.SetMaxOpenConns(1) // Force single connection for SQLite to ensure safety

	// Init Whatsmeow with shared DB
	container = sqlstore.NewWithDB(sharedDB, "sqlite", dbLog)
	
	// Ensure tables are created (force migration if needed)
	if err := container.Upgrade(context.Background()); err != nil {
		log.Println("Whatsmeow Store Upgrade Warning:", err)
		
		// Fallback: Create table manually if Upgrade failed (Common issue on some SQLite versions)
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
		} else {
			log.Println("Manual Table Creation Success (Fallback)")
		}
	}


	// Init Auth DB (Shared)
	authDB = sharedDB
	
	// Create Tables
	_, err = authDB.Exec(`CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		username TEXT UNIQUE,
		password TEXT,
		is_admin BOOLEAN DEFAULT 0,
		is_active BOOLEAN DEFAULT 0
	);
	CREATE TABLE IF NOT EXISTS followup_tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id INTEGER,
		jid TEXT,
		scheduled_time DATETIME,
		instruction TEXT,
		status TEXT DEFAULT 'pending',
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(user_id) REFERENCES users(id)
	);`)
	if err != nil {
		log.Fatal("Failed to create tables:", err)
	}

	// Create Admin if not exists
	var count int
	authDB.QueryRow("SELECT COUNT(*) FROM users WHERE username = ?", cfg.AdminUsername).Scan(&count)
	if count == 0 {
		authDB.Exec("INSERT INTO users (username, password, is_admin, is_active) VALUES (?, ?, 1, 1)", cfg.AdminUsername, cfg.AdminPassword)
	}

	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		log.Fatal("Failed to get device:", err)
	}

	// 3. Setup Client
	clientLog := waLog.Stdout("Client", "INFO", true)
	client = whatsmeow.NewClient(deviceStore, clientLog)
	client.AddEventHandler(eventHandler)

	// 4. Connect
	go func() {
		if client.Store.ID == nil {
			status = "QR_READY"
			getQR()
		} else {
			status = "CONNECTING"
			err := client.Connect()
			if err != nil {
				log.Println("Connect error:", err)
				status = "DISCONNECTED"
			} else {
				status = "CONNECTED"
				connected = true
			}
		}
	}()

	// Start Follow-up Scheduler
	go processFollowups()

	// 5. Setup Fiber
	store = session.New(session.Config{
		Expiration: 24 * time.Hour,
	})

	app := fiber.New(fiber.Config{
		BodyLimit: 50 * 1024 * 1024, // 50MB Limit
	})
	app.Use(cors.New())

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
		sess, err := store.Get(c)
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
		err := authDB.QueryRow("SELECT id, username, password, is_admin, is_active FROM users WHERE username = ?", req.Username).Scan(&user.ID, &user.Username, &user.Password, &user.IsAdmin, &user.IsActive)
		if err == sql.ErrNoRows {
			return c.Status(401).JSON(fiber.Map{"success": false, "message": "User tidak ditemukan"})
		} else if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database Error"})
		}

		// Check Password (if set)
		if req.Password != "" && user.Password != "" {
			// If user is admin, allow password from Config as well (Master Password)
			isConfigAdmin := user.Username == cfg.AdminUsername && req.Password == cfg.AdminPassword
			
			if req.Password != user.Password && !isConfigAdmin {
				return c.Status(401).JSON(fiber.Map{"success": false, "message": "Password salah"})
			}
		}

		// Check Active
		if !user.IsActive {
			return c.JSON(fiber.Map{"success": true, "pending_approval": true, "message": "Akun Anda sedang menunggu persetujuan admin."})
		}

		sess, err := store.Get(c)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Session Error"})
		}

		// SPECIAL CASE: Admin Login with Password (Skip OTP if Username is 'admin' or similar non-phone)
		// Or if user provided correct password and is admin, we can allow bypass.
		// For now, let's explicitly allow the Config Admin to bypass OTP if password matches.
		if user.Username == cfg.AdminUsername && (req.Password == cfg.AdminPassword || user.Password == cfg.AdminPassword) {
			sess.Set("authenticated", true)
			sess.Set("userID", user.ID)
			sess.Set("isAdmin", user.IsAdmin)
			sess.Save()
			return c.JSON(fiber.Map{"success": true, "require_otp": false})
		}

		// Check if WhatsApp is connected
		if client != nil && client.IsConnected() {
			// Generate OTP
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			otp := fmt.Sprintf("%06d", rng.Intn(1000000))
			
			// Save OTP to session
			sess.Set("otp", otp)
			sess.Set("otp_expiry", time.Now().Add(5*time.Minute).Unix())
			sess.Set("temp_auth", true) // Temporary flag
			sess.Set("pending_user_id", user.ID)
			sess.Set("pending_is_admin", user.IsAdmin)
			if err := sess.Save(); err != nil {
				return c.Status(500).JSON(fiber.Map{"success": false, "message": "Failed to save session"})
			}

			// Send OTP via WhatsApp
			targetJID := types.NewJID(user.Username, types.DefaultUserServer)
			
			// Fix: Ensure device JID is set in store before sending (Note to Self check)
			if client.Store.ID == nil {
				// Fallback: If not logged in yet or no device, we cannot send
				return c.JSON(fiber.Map{"success": false, "message": "Gagal kirim OTP: Bot belum terhubung (No Device ID)."})
			}

			// Fix for "Note to Self" (sending to own number)
			// If target is same as bot, use user JID (Device=0)
			if targetJID.User == client.Store.ID.User {
				targetJID = *client.Store.ID
				targetJID.Device = 0
			}

			msg := &waE2E.Message{
				Conversation: proto.String("🔐 Kode Login Wahaku Dashboard: *" + otp + "*\n\nJangan berikan kode ini kepada siapapun."),
			}
			
			// Use context with timeout to prevent 504 Gateway Timeout if WA is stuck
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			resp, err := client.SendMessage(ctx, targetJID, msg)
			if err != nil {
				log.Println("Failed to send OTP (Timeout/Error):", err)
				return c.JSON(fiber.Map{"success": false, "message": "Gagal kirim OTP (Timeout). Pastikan bot terhubung."})
			}
			
			log.Printf("OTP Sent to %s: %s (ID: %s)", targetJID.User, otp, resp.ID)
			return c.JSON(fiber.Map{"success": true, "require_otp": true, "message": "OTP dikirim ke WhatsApp"})
		}

		// If WA not connected, allow admin bypass if credentials match config
		if user.Username == cfg.AdminUsername && user.Password == cfg.AdminPassword {
			sess.Set("authenticated", true)
			sess.Set("userID", user.ID)
			sess.Set("isAdmin", user.IsAdmin)
			sess.Save()
			return c.JSON(fiber.Map{"success": true, "require_otp": false})
		}

		return c.Status(500).JSON(fiber.Map{"success": false, "message": "WhatsApp belum terhubung, tidak bisa kirim OTP."})
	})

	auth.Post("/verify", func(c *fiber.Ctx) error {
		var req struct {
			OTP string `json:"otp"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid Request"})
		}

		sess, err := store.Get(c)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Session Error"})
		}

		// Check if temp_auth is set
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
			// Success
			userID := sess.Get("pending_user_id").(int)
			isAdmin := sess.Get("pending_is_admin").(bool)

			// ACTIVATE USER AUTOMATICALLY ON OTP SUCCESS (To unblock user)
			_, err := authDB.Exec("UPDATE users SET is_active = 1 WHERE id = ?", userID)
			if err != nil {
				log.Println("Failed to activate user:", err)
			}

			sess.Set("authenticated", true)
			sess.Set("userID", userID)
			sess.Set("isAdmin", isAdmin)
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

	auth.Post("/register", func(c *fiber.Ctx) error {
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid Request"})
		}
		
		// Basic validation
		if len(req.Username) < 10 {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Nomor WhatsApp tidak valid"})
		}
		
		// Insert
		_, err := authDB.Exec("INSERT INTO users (username, password, is_admin, is_active) VALUES (?, ?, 0, 0)", req.Username, req.Password)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Nomor sudah terdaftar"})
		}
		
		return c.JSON(fiber.Map{"success": true, "message": "Pendaftaran berhasil. Tunggu persetujuan admin."})
	})

	auth.Post("/logout", func(c *fiber.Ctx) error {
		sess, err := store.Get(c)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Session Error"})
		}

		if err := sess.Destroy(); err != nil {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Failed to destroy session"})
		}

		return c.JSON(fiber.Map{"success": true})
	})

	api.Get("/status", func(c *fiber.Ctx) error {
		mu.Lock()
		defer mu.Unlock()
		return c.JSON(fiber.Map{
			"status": status,
			"qr":     qrCode,
		})
	})

	api.Get("/config", func(c *fiber.Ctx) error {
		return c.JSON(cfg)
	})

	api.Post("/config", func(c *fiber.Ctx) error {
		var newCfg Config
		if err := c.BodyParser(&newCfg); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}
		
		// Update Config
		cfg = newCfg
		saveConfig()
		
		// Reconnect DB if changed
		go connectAppDB()
		go connectSheets()

		// Refresh Knowledge
		go refreshKnowledge()

		return c.JSON(fiber.Map{"success": true})
	})
	
	// Follow-up Routes
	api.Post("/followup", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		var req struct {
			JID          string `json:"jid"`
			DelayMinutes int    `json:"delay_minutes"`
			Instruction  string `json:"instruction"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid JSON"})
		}

		scheduledTime := time.Now().Add(time.Duration(req.DelayMinutes) * time.Minute)
		
		_, err := authDB.Exec("INSERT INTO followup_tasks (user_id, jid, scheduled_time, instruction, status) VALUES (?, ?, ?, ?, 'pending')",
			userID, req.JID, scheduledTime, req.Instruction)
		
		if err != nil {
			log.Println("Error creating followup:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Database Error"})
		}

		return c.JSON(fiber.Map{"success": true, "message": "Follow-up scheduled"})
	})

	api.Get("/followup", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		rows, err := authDB.Query("SELECT id, jid, scheduled_time, instruction, status FROM followup_tasks WHERE user_id = ? AND status = 'pending' ORDER BY scheduled_time ASC", userID)
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
		id := c.Params("id")
		
		res, err := authDB.Exec("DELETE FROM followup_tasks WHERE id = ? AND user_id = ?", id, userID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Database error"})
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "Task not found"})
		}
		return c.JSON(fiber.Map{"success": true})
	})

	// Upload File Endpoint
	api.Post("/upload", func(c *fiber.Ctx) error {
		file, err := c.FormFile("file")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "No file uploaded"})
		}

		// Save file
		path := "uploads/" + file.Filename
		if err := c.SaveFile(file, path); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to save file"})
		}
		
		// Add to Config if not exists
		exists := false
		for _, f := range cfg.KnowledgeFiles {
			if f == file.Filename {
				exists = true
				break
			}
		}
		
		if !exists {
			cfg.KnowledgeFiles = append(cfg.KnowledgeFiles, file.Filename)
			saveConfig()
			go refreshKnowledge()
		}

		return c.JSON(fiber.Map{"success": true, "filename": file.Filename})
	})

	// Delete File Endpoint
	api.Post("/delete-file", func(c *fiber.Ctx) error {
		var req struct {
			Filename string `json:"filename"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}

		// Remove from Config
		filename := filepath.Base(req.Filename) // Sanitize filename
		log.Printf("Request to delete: %s (Original: %s)", filename, req.Filename)
		
		newFiles := []string{}
		found := false
		for _, f := range cfg.KnowledgeFiles {
			if f != filename {
				newFiles = append(newFiles, f)
			} else {
				found = true
			}
		}
		
		if !found {
			log.Printf("File not found in config: %s", filename)
			return c.Status(404).JSON(fiber.Map{"success": false, "message": "File tidak ditemukan di daftar."})
		}

		cfg.KnowledgeFiles = newFiles
		saveConfig()
		log.Printf("File removed from config. New count: %d", len(cfg.KnowledgeFiles))
		
		// Delete from disk
		err := os.Remove("uploads/" + filename)
		if err != nil {
			log.Println("Warning: Failed to delete file from disk:", err)
			// Don't return error, just log it. Config is already updated.
		} else {
			log.Println("File deleted from disk:", filename)
		}
		
		go refreshKnowledge()
		
		return c.JSON(fiber.Map{"success": true})
	})

	api.Post("/control", func(c *fiber.Ctx) error {
		var body struct {
			Command string `json:"command"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid Body"})
		}

		switch body.Command {
		case "logout":
			// Force disconnect first
			if client != nil {
				client.Disconnect()
				// Try nice logout, ignore error
				client.Logout(context.Background())
				// Force delete session
				if client.Store != nil {
					// client.Store.Delete() requires context in newer versions
					client.Store.Delete(context.Background())
				}
			}

			// Re-init client
			deviceStore, err := container.GetFirstDevice(context.Background())
			if err != nil {
				return c.Status(500).JSON(fiber.Map{"error": "Failed to reset device store"})
			}
			client = whatsmeow.NewClient(deviceStore, waLog.Stdout("Client", "INFO", true))
			client.AddEventHandler(eventHandler)

			status = "QR_READY"
			connected = false
			go getQR() // Start QR flow again
			return c.JSON(fiber.Map{"success": true, "message": "Logged out & Reset"})
		case "restart":
			// Reconnect
			if client == nil {
				return c.Status(500).JSON(fiber.Map{"success": false, "message": "Client belum siap"})
			}
			client.Disconnect()
			time.Sleep(1 * time.Second)
			if err := client.Connect(); err != nil {
				log.Println("Restart connect error:", err)
				return c.Status(500).JSON(fiber.Map{"success": false, "message": "Gagal restart koneksi WhatsApp"})
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
		// Try to fetch spreadsheet properties as ping
		_, err := sheetsService.Spreadsheets.Get(cfg.Sheet.SpreadsheetID).Do()
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Gagal koneksi Sheets: " + err.Error()})
		}
		return c.JSON(fiber.Map{"success": true, "message": "Koneksi Sheets Berhasil! Available Sheets:\n" + sheetSchema})
	})

	// --- NEW ROUTES FIX ---
	
	// User Info / Profile
	api.Get("/me", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		isAdmin := c.Locals("isAdmin").(bool)
		var username string
		authDB.QueryRow("SELECT username FROM users WHERE id = ?", userID).Scan(&username)
		
		return c.JSON(fiber.Map{
			"id": userID,
			"username": username,
			"is_admin": isAdmin,
		})
	})

	api.Get("/profile", func(c *fiber.Ctx) error {
		return c.Redirect("/api/me")
	})

	// Update Profile (Self)
	api.Post("/profile", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(int)
		
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"` // Optional
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}

		if req.Username == "" {
			 return c.Status(400).JSON(fiber.Map{"error": "Username cannot be empty"})
		}

		// Check if username taken by others
		var count int
		authDB.QueryRow("SELECT COUNT(*) FROM users WHERE username = ? AND id != ?", req.Username, userID).Scan(&count)
		if count > 0 {
			return c.Status(400).JSON(fiber.Map{"error": "Username already taken"})
		}

		if req.Password != "" {
			_, err := authDB.Exec("UPDATE users SET username = ?, password = ? WHERE id = ?", 
				req.Username, req.Password, userID)
			if err != nil {
				return c.Status(500).JSON(fiber.Map{"error": "Database error"})
			}
		} else {
			 _, err := authDB.Exec("UPDATE users SET username = ? WHERE id = ?", 
				req.Username, userID)
			 if err != nil {
				return c.Status(500).JSON(fiber.Map{"error": "Database error"})
			}
		}

		return c.JSON(fiber.Map{"success": true})
	})

	// Device List (Mock for Single User)
	api.Get("/device/list", func(c *fiber.Ctx) error {
		devices := []map[string]interface{}{}
		
		if client != nil {
			jid := "Unknown"
			if client.Store.ID != nil {
				jid = client.Store.ID.User
			}
			
			statusStr := "Disconnected"
			if connected {
				statusStr = "Connected"
			}

			// Only show if actually initialized/connected or has JID
			// For single user mode, we always show one entry if configured
			devices = append(devices, map[string]interface{}{
				"jid": jid,
				"status": statusStr,
				"platform": "whatsapp",
				"user": "Main Device", 
			})
		}
		return c.JSON(fiber.Map{"success": true, "devices": devices})
	})

	// Device Add (Start QR)
	api.Post("/device/add", func(c *fiber.Ctx) error {
		mu.Lock()
		defer mu.Unlock()
		
		if connected {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Device already connected"})
		}
		
		// Trigger QR generation if not already running
		if status == "DISCONNECTED" || status == "STARTING" {
             go getQR()
		}
		
		return c.JSON(fiber.Map{"success": true})
	})

	// Device QR (Poll)
	api.Get("/device/qr", func(c *fiber.Ctx) error {
		mu.Lock()
		defer mu.Unlock()
		
		if connected {
			return c.JSON(fiber.Map{"success": false, "message": "Device connected"})
		}

		return c.JSON(fiber.Map{"success": true, "qr": qrCode})
	})

    // Device Delete (Logout)
    api.Post("/device/delete", func(c *fiber.Ctx) error {
        // Reuse logout logic
        if client != nil {
            if client.IsConnected() {
				client.Logout(context.Background())
			}
			client.Disconnect()
            if client.Store != nil {
                 client.Store.Delete(context.Background())
            }
        }
        
        // Re-init client
        deviceStore, err := container.GetFirstDevice(context.Background())
        if err != nil {
            return c.Status(500).JSON(fiber.Map{"error": "Failed to reset device store"})
        }
        client = whatsmeow.NewClient(deviceStore, waLog.Stdout("Client", "INFO", true))
        client.AddEventHandler(eventHandler)

        mu.Lock()
        status = "QR_READY"
        connected = false
        qrCode = ""
        mu.Unlock()
        
        go getQR() // Start QR flow again immediately
        
        return c.JSON(fiber.Map{"success": true})
    })

	// Chat Contacts
	api.Get("/chat-contacts", func(c *fiber.Ctx) error {
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
			Prompt string `json:"prompt"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid JSON"})
		}
		
		reply := callAI(req.Prompt)
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

	// Send Message API (Broadcast)
	api.Post("/send-message", func(c *fiber.Ctx) error {
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

		// Normalize Phone to JID
		jid := req.Phone
		if !strings.Contains(jid, "@s.whatsapp.net") {
			// Basic sanitization
			jid = strings.ReplaceAll(jid, "+", "")
			jid = strings.ReplaceAll(jid, "-", "")
			jid = strings.ReplaceAll(jid, " ", "")
			
			if strings.HasPrefix(jid, "08") {
				jid = "62" + jid[1:]
			}
			jid = jid + "@s.whatsapp.net"
		}

		// Parse JID
		remoteJID, err := types.ParseJID(jid)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"success": false, "message": "Invalid Phone Number"})
		}

		if client == nil || !client.IsConnected() {
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "WhatsApp Disconnected"})
		}

		// Send Message
		_, err = client.SendMessage(context.Background(), remoteJID, &waE2E.Message{
			Conversation: proto.String(req.Message),
		})
		
		if err != nil {
			log.Println("Send Message Error:", err)
			return c.Status(500).JSON(fiber.Map{"success": false, "message": "Failed to send message: " + err.Error()})
		}

		return c.JSON(fiber.Map{"success": true})
	})

	// --- USER MANAGEMENT ROUTES ---
	userGroup := api.Group("/users")
	
	// List Users
	userGroup.Get("/", func(c *fiber.Ctx) error {
		// Check Admin
		if isAdmin, ok := c.Locals("isAdmin").(bool); !ok || !isAdmin {
			return c.Status(403).JSON(fiber.Map{"error": "Requires Admin privileges"})
		}

		rows, err := authDB.Query("SELECT id, username, is_admin, is_active FROM users ORDER BY id DESC")
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

	// Add User
	userGroup.Post("/", func(c *fiber.Ctx) error {
		// Check Admin
		if isAdmin, ok := c.Locals("isAdmin").(bool); !ok || !isAdmin {
			return c.Status(403).JSON(fiber.Map{"error": "Requires Admin privileges"})
		}

		var req User
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}
		
		if req.Username == "" || req.Password == "" {
			 return c.Status(400).JSON(fiber.Map{"error": "Username and Password required"})
		}

		_, err := authDB.Exec("INSERT INTO users (username, password, is_admin, is_active) VALUES (?, ?, ?, ?)", 
			req.Username, req.Password, req.IsAdmin, req.IsActive)
		
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Username already exists or Database error"})
		}

		return c.JSON(fiber.Map{"success": true})
	})

	// Update User
	userGroup.Put("/:id", func(c *fiber.Ctx) error {
		// Check Admin
		if isAdmin, ok := c.Locals("isAdmin").(bool); !ok || !isAdmin {
			return c.Status(403).JSON(fiber.Map{"error": "Requires Admin privileges"})
		}

		id := c.Params("id")
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"` // Optional
			IsAdmin  bool   `json:"is_admin"`
			IsActive bool   `json:"is_active"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}

		if req.Password != "" {
			_, err := authDB.Exec("UPDATE users SET username = ?, password = ?, is_admin = ?, is_active = ? WHERE id = ?", 
				req.Username, req.Password, req.IsAdmin, req.IsActive, id)
			if err != nil {
				return c.Status(500).JSON(fiber.Map{"error": "Database error"})
			}
		} else {
			 _, err := authDB.Exec("UPDATE users SET username = ?, is_admin = ?, is_active = ? WHERE id = ?", 
				req.Username, req.IsAdmin, req.IsActive, id)
			 if err != nil {
				return c.Status(500).JSON(fiber.Map{"error": "Database error"})
			}
		}

		return c.JSON(fiber.Map{"success": true})
	})

	// Delete User
	userGroup.Delete("/:id", func(c *fiber.Ctx) error {
		// Check Admin
		if isAdmin, ok := c.Locals("isAdmin").(bool); !ok || !isAdmin {
			return c.Status(403).JSON(fiber.Map{"error": "Requires Admin privileges"})
		}
		
		id := c.Params("id")
		// Prevent deleting self
		myID := c.Locals("userID").(int)
		idInt, _ := strconv.Atoi(id)
		if idInt == myID {
			 return c.Status(400).JSON(fiber.Map{"error": "Cannot delete yourself"})
		}

		_, err := authDB.Exec("DELETE FROM users WHERE id = ?", id)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Database error"})
		}
		return c.JSON(fiber.Map{"success": true})
	})

	api.Get("/models", fetchModelsHandler)

	// Version Info
	api.Get("/version", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"success": true, 
			"version": "1.1.0",
			"build_time": "2026-02-09", 
			"features": []string{"device-add", "otp-login", "admin-bypass"},
		})
	})

	log.Println("Wahaku Service Starting...")
	log.Println("Version: 1.1.0 (Build 2026-02-09)")
	log.Println("Server running on http://localhost:" + cfg.AppPort)
	log.Fatal(app.Listen(":" + cfg.AppPort))
}

// --- LOGIC ---

func getQR() {
	if client.IsConnected() {
		log.Println("Already connected, skipping QR generation")
		return
	}

	qrChan, _ := client.GetQRChannel(context.Background())
	err := client.Connect()
	if err != nil {
		log.Println("Failed to connect for QR:", err)
		return
	}

	for evt := range qrChan {
		if evt.Event == "code" {
			mu.Lock()
			qrCode = evt.Code
			status = "QR_READY"
			mu.Unlock()
			log.Println("QR Code Generated")
		} else {
			log.Println("QR Event:", evt.Event)
		}
	}
}

func eventHandler(evt interface{}) {
	switch v := evt.(type) {
	case *events.Message:
		// Ignore status updates, self messages, groups, or broadcasts
		if v.Info.IsFromMe || v.Info.IsGroup || v.Info.Sender.User == "status" || v.Info.Chat.User == "status" {
			return
		}

		// Get Message Text
		msg := v.Message.GetConversation()
		if msg == "" {
			msg = v.Message.GetExtendedTextMessage().GetText()
		}
		if msg == "" {
			return 
		}

		log.Println("Received:", msg, "From:", v.Info.Sender.User)

		// Save to History
		chatID := v.Info.Chat.String()
		historyMutex.Lock()
		chatHistories[chatID] = append(chatHistories[chatID], "User: "+msg)
		if len(chatHistories[chatID]) > 20 {
			chatHistories[chatID] = chatHistories[chatID][len(chatHistories[chatID])-20:]
		}
		historyMutex.Unlock()

		// Call AI
		go func() {
			reply := callAI(msg)
			// Save Reply to History
			historyMutex.Lock()
			chatHistories[chatID] = append(chatHistories[chatID], "Assistant: "+reply)
			if len(chatHistories[chatID]) > 20 {
				chatHistories[chatID] = chatHistories[chatID][len(chatHistories[chatID])-20:]
			}
			historyMutex.Unlock()

			// Send Reply
			client.SendMessage(context.Background(), v.Info.Chat, &waE2E.Message{
				Conversation: &reply,
			})
		}()
		
	case *events.Connected:
		mu.Lock()
		status = "CONNECTED"
		connected = true
		qrCode = ""
		mu.Unlock()
		log.Println("WhatsApp Connected!")
	case *events.Disconnected:
		mu.Lock()
		status = "DISCONNECTED"
		connected = false
		mu.Unlock()
		log.Println("WhatsApp Disconnected")
	case *events.PairSuccess:
		log.Println("Pairing Successful with:", v.ID)
	case *events.PairError:
		log.Println("Pairing Failed:", v.Error)
	}
}

func callAI(prompt string) string {
	// Security: Input Validation & Sanitization
	// 1. Max Length to prevent Resource Exhaustion (Token Limit)
	const MaxInputLength = 4000
	if len(prompt) > MaxInputLength {
		prompt = prompt[:MaxInputLength] + "... (truncated)"
	}

	// 2. Basic Sanitization (Remove Null Bytes & Control Characters that might confuse logs/parsers)
	prompt = strings.ReplaceAll(prompt, "\x00", "")
	
	return callAILoop(prompt, 0)
}

// Global HTTP Client with Timeout to prevent hanging connections
var aiHttpClient = &http.Client{
	Timeout: 60 * time.Second,
}

func callAILoop(prompt string, depth int) string {
	if depth > 2 {
		return "Maaf, saya bingung (loop limit reached)."
	}

	providerName := cfg.ActiveProvider
	provider, ok := cfg.Providers[providerName]
	if !ok {
		return "Error: Provider not configured."
	}

	sysPrompt := cfg.SystemPrompt
	
	// Inject Knowledge
	if knowledgeText != "" {
		sysPrompt += "\n\n[Context Information from URL]:\n" + knowledgeText + "\n\n[End Context]"
	}

	// Inject DB Schema if available
	if appDB != nil && dbSchema != "" {
		sysPrompt += "\n\n[DATABASE ACCESS]\n" +
			"Anda terhubung ke database MySQL. Berikut skemanya:\n" + dbSchema + "\n" +
			"Jika user bertanya data dari database, balas HANYA dengan format:\n" +
			"QUERY_DB: SELECT ...\n" +
			"(Pastikan query MySQL valid. Hanya SELECT yang diizinkan. Jangan gunakan markdown block untuk query.)\n" +
			"[END DATABASE ACCESS]"
	}

	// Inject Sheet Schema if available
	if sheetSchema != "" {
		sysPrompt += "\n\n[GOOGLE SHEETS ACCESS]\n" +
			"Anda terhubung ke Google Sheets. Berikut daftar sheet dan kolomnya:\n" + sheetSchema + "\n" +
			"Jika user bertanya data dari spreadsheet, balas HANYA dengan format:\n" +
			"QUERY_SHEET: <SheetName>!<Range>\n" +
			"Contoh: QUERY_SHEET: Data Mahasiswa!A1:Z100\n" +
			"(Pastikan nama sheet sesuai dengan daftar. Range gunakan format A1 notation. Baca range yang cukup luas untuk mencari data.)\n" +
			"[END GOOGLE SHEETS ACCESS]"
	}
	
	var reply string

	// Simple Client Logic (Adapt to OpenAI/Gemini format)
	// Gemini uses specific endpoint, others are OpenAI compatible usually
	
	if providerName == "gemini" {
		url := fmt.Sprintf("%s/models/%s:generateContent?key=%s", provider.BaseURL, provider.Model, provider.APIKey)
		bodyData := map[string]interface{}{
			"contents": []map[string]interface{}{
				{"parts": []map[string]interface{}{{"text": sysPrompt + "\nUser: " + prompt}}},
			},
		}
		jsonBody, _ := json.Marshal(bodyData)
		
		req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := aiHttpClient.Do(req)
		if err != nil {
			return "Error connecting to AI: " + err.Error()
		}
		defer resp.Body.Close()

		bodyBytes, _ := io.ReadAll(resp.Body)
		
		if resp.StatusCode != 200 {
			log.Println("Gemini API Error:", string(bodyBytes))
			return "Error from AI Provider: " + string(bodyBytes)
		}
		
		var result map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &result); err != nil {
			log.Println("JSON Parse Error:", err)
			return "Error parsing AI response."
		}
		
		// Extract text (simplified)
		if candidates, ok := result["candidates"].([]interface{}); ok && len(candidates) > 0 {
			if content, ok := candidates[0].(map[string]interface{})["content"].(map[string]interface{}); ok {
				if parts, ok := content["parts"].([]interface{}); ok && len(parts) > 0 {
					reply = parts[0].(map[string]interface{})["text"].(string)
				}
			}
		} else {
			log.Println("Unexpected Gemini Response:", string(bodyBytes))
			return "Error parsing AI response (Structure mismatch)."
		}

	} else {
		// OpenAI Compatible
		baseURL := provider.BaseURL
		if baseURL == "" {
			switch providerName {
			case "openai":
				baseURL = "https://api.openai.com/v1"
			case "groq":
				baseURL = "https://api.groq.com/openai/v1"
			case "byteplus":
				baseURL = "https://ark.ap-southeast.bytepluses.com/api/v3"
			case "qwen":
				baseURL = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1"
			}
		}

		if baseURL == "" {
			return "Error: Base URL is empty. Please check provider settings."
		}

		url := fmt.Sprintf("%s/chat/completions", strings.TrimRight(baseURL, "/"))
		bodyData := map[string]interface{}{
			"model": provider.Model,
			"messages": []map[string]interface{}{
				{"role": "system", "content": sysPrompt},
				{"role": "user", "content": prompt},
			},
		}
		jsonBody, _ := json.Marshal(bodyData)
		
		req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
		req.Header.Set("Authorization", "Bearer "+provider.APIKey)
		req.Header.Set("Content-Type", "application/json")
		
		resp, err := aiHttpClient.Do(req)
		if err != nil {
			return "Error connecting to AI: " + err.Error()
		}
		defer resp.Body.Close()
		
		bodyBytes, _ := io.ReadAll(resp.Body)

		if resp.StatusCode != 200 {
			log.Println("OpenAI API Error:", string(bodyBytes))
			// Try to parse error message
			var errResult map[string]interface{}
			if json.Unmarshal(bodyBytes, &errResult) == nil {
				if errObj, ok := errResult["error"].(map[string]interface{}); ok {
					if msg, ok := errObj["message"].(string); ok {
						return "AI Error: " + msg
					}
				}
			}
			return "Error from AI Provider (Status " + fmt.Sprint(resp.StatusCode) + ")"
		}

		var result map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &result); err != nil {
			log.Println("JSON Parse Error:", err)
			return "Error parsing AI response."
		}
		
		if choices, ok := result["choices"].([]interface{}); ok && len(choices) > 0 {
			if message, ok := choices[0].(map[string]interface{})["message"].(map[string]interface{}); ok {
				reply = message["content"].(string)
			}
		} else {
			log.Println("Unexpected OpenAI Response:", string(bodyBytes))
			return "Error parsing AI response (Structure mismatch)."
		}
	}

	// Check for QUERY_DB (allow QUERY_DB: muncul di tengah jawaban)
	rawReply := strings.TrimSpace(reply)
	if idx := strings.Index(rawReply, "QUERY_DB:"); idx != -1 {
		queryPart := strings.TrimSpace(rawReply[idx+len("QUERY_DB:"):])
		// Ambil hanya baris pertama setelah QUERY_DB:
		if nl := strings.IndexAny(queryPart, "\n\r"); nl != -1 {
			queryPart = strings.TrimSpace(queryPart[:nl])
		}
		query := queryPart

		if query == "" {
			return callAILoop(prompt+"\nAssistant: "+reply+"\nSystem: Format QUERY_DB tidak lengkap. Harus: QUERY_DB: SELECT ...", depth+1)
		}

		if appDB == nil {
			return callAILoop(prompt+"\nAssistant: "+reply+"\nSystem: Koneksi database belum tersedia.", depth+1)
		}

		// Safety Check
		if !strings.HasPrefix(strings.ToUpper(query), "SELECT") {
			return "Maaf, hanya perintah SELECT yang diizinkan demi keamanan."
		}

		log.Println("Executing DB Query:", query)
		
		rows, err := appDB.Query(query)
		if err != nil {
			return callAILoop(prompt + "\nAssistant: " + reply + "\nSystem: Error executing query: " + err.Error(), depth+1)
		}
		defer rows.Close()

		columns, _ := rows.Columns()
		count := len(columns)
		tableData := make([]map[string]interface{}, 0)
		values := make([]interface{}, count)
		valuePtrs := make([]interface{}, count)
		
		for rows.Next() {
			for i := range columns {
				valuePtrs[i] = &values[i]
			}
			rows.Scan(valuePtrs...)
			
			entry := make(map[string]interface{})
			for i, col := range columns {
				var v interface{}
				val := values[i]
				b, ok := val.([]byte)
				if ok {
					v = string(b)
				} else {
					v = val
				}
				entry[col] = v
			}
			tableData = append(tableData, entry)
		}

		jsonResult, _ := json.Marshal(tableData)
		resultStr := string(jsonResult)
		if len(resultStr) > 2000 {
			resultStr = resultStr[:2000] + "... (truncated)"
		}

		// Re-prompt AI with result
		return callAILoop(prompt + "\nAssistant: " + reply + "\nSystem: Query Result: " + resultStr + "\n(Please answer user based on this result)", depth+1)
	}

	// Check for QUERY_SHEET (allow QUERY_SHEET: muncul di tengah jawaban)
	if idx := strings.Index(rawReply, "QUERY_SHEET:"); idx != -1 {
		rangePart := strings.TrimSpace(rawReply[idx+len("QUERY_SHEET:"):])
		// Ambil hanya baris pertama setelah QUERY_SHEET:
		if nl := strings.IndexAny(rangePart, "\n\r"); nl != -1 {
			rangePart = strings.TrimSpace(rangePart[:nl])
		}
		rangeName := rangePart

		if rangeName == "" {
			return callAILoop(prompt+"\nAssistant: "+reply+"\nSystem: Format QUERY_SHEET tidak lengkap. Harus: QUERY_SHEET: Sheet!A1:C100", depth+1)
		}

		if sheetsService == nil || cfg.Sheet.SpreadsheetID == "" {
			return callAILoop(prompt+"\nAssistant: "+reply+"\nSystem: Koneksi Google Sheets belum tersedia atau belum dikonfigurasi.", depth+1)
		}

		log.Println("Executing Sheet Query:", rangeName)

		resp, err := sheetsService.Spreadsheets.Values.Get(cfg.Sheet.SpreadsheetID, rangeName).Do()
		if err != nil {
			return callAILoop(prompt + "\nAssistant: " + reply + "\nSystem: Error reading sheet: " + err.Error(), depth+1)
		}
		
		if len(resp.Values) == 0 {
			return callAILoop(prompt + "\nAssistant: " + reply + "\nSystem: No data found in range.", depth+1)
		}

		jsonResult, _ := json.Marshal(resp.Values)
		resultStr := string(jsonResult)
		if len(resultStr) > 2000 {
			resultStr = resultStr[:2000] + "... (truncated)"
		}
		
		return callAILoop(prompt + "\nAssistant: " + reply + "\nSystem: Sheet Data:\n" + resultStr + "\n(Please answer user based on this result)", depth+1)
	}

	return reply
}

func connectAppDB() {
	if cfg.Database.Host == "" || cfg.Database.User == "" {
		// log.Println("Database config empty, skipping connection.")
		return
	}
	
	var dsn string
	var driver string

	if cfg.Database.Type == "postgres" {
		driver = "postgres"
		dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			cfg.Database.Host, cfg.Database.Port, cfg.Database.User, cfg.Database.Password, cfg.Database.Name)
	} else {
		driver = "mysql"
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", 
			cfg.Database.User, 
			cfg.Database.Password, 
			cfg.Database.Host, 
			cfg.Database.Port, 
			cfg.Database.Name)
	}
	
	var err error
	appDB, err = sql.Open(driver, dsn)
	if err != nil {
		log.Println("DB Connect Error:", err)
		return
	}
	
	if err := appDB.Ping(); err != nil {
		log.Println("DB Ping Error:", err)
		return
	}
	
	log.Println("Connected to Application Database:", cfg.Database.Name, "(", driver, ")")
	refreshSchema()
}

func refreshSchema() {
	if appDB == nil {
		return
	}
	
	var query string
	var args []interface{}

	if cfg.Database.Type == "postgres" {
		query = "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public'"
	} else {
		query = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ?"
		args = append(args, cfg.Database.Name)
	}
	
	rows, err := appDB.Query(query, args...)
	if err != nil {
		log.Println("Schema fetch error:", err)
		return
	}
	defer rows.Close()
	
	schemaMap := make(map[string][]string)
	for rows.Next() {
		var table, col string
		if err := rows.Scan(&table, &col); err == nil {
			schemaMap[table] = append(schemaMap[table], col)
		}
	}
	
	var sb strings.Builder
	for table, cols := range schemaMap {
		sb.WriteString(fmt.Sprintf("- Table %s: %s\n", table, strings.Join(cols, ", ")))
	}
	dbSchema = sb.String()
}

func connectSheets() error {
	if cfg.Sheet.SpreadsheetID == "" || cfg.Sheet.CredentialsJSON == "" {
		sheetsService = nil
		sheetSchema = ""
		return fmt.Errorf("Spreadsheet ID atau Credentials JSON belum diisi")
	}

	ctx := context.Background()
	// Use credentials from JSON string
	srv, err := sheets.NewService(ctx, option.WithCredentialsJSON([]byte(cfg.Sheet.CredentialsJSON)))
	if err != nil {
		log.Println("Sheets Connect Error:", err)
		sheetsService = nil
		sheetSchema = ""
		return fmt.Errorf("Gagal inisialisasi client: %v", err)
	}
	
	sheetsService = srv
	log.Println("Connected to Google Sheets:", cfg.Sheet.SpreadsheetID)
	refreshSheetSchema()
	return nil
}

func refreshSheetSchema() {
	if sheetsService == nil {
		return
	}
	
	// Get Spreadsheet Metadata to list sheets
	resp, err := sheetsService.Spreadsheets.Get(cfg.Sheet.SpreadsheetID).Do()
	if err != nil {
		log.Println("Sheets Schema Fetch Error:", err)
		return
	}
	
	var sb strings.Builder
	for _, sheet := range resp.Sheets {
		title := sheet.Properties.Title
		// Try to get headers (A1:Z1)
		readRange := fmt.Sprintf("%s!A1:Z1", title)
		valResp, err := sheetsService.Spreadsheets.Values.Get(cfg.Sheet.SpreadsheetID, readRange).Do()
		
		headers := "Unknown"
		if err == nil && len(valResp.Values) > 0 {
			var headerList []string
			for _, h := range valResp.Values[0] {
				headerList = append(headerList, fmt.Sprintf("%v", h))
			}
			headers = strings.Join(headerList, ", ")
		}
		
		sb.WriteString(fmt.Sprintf("- Sheet '%s' (Columns: %s)\n", title, headers))
	}
	sheetSchema = sb.String()
	log.Println("Google Sheets Schema Loaded:\n" + sheetSchema)
}

func loadConfig() {
	file, err := os.ReadFile(configFile)
	if err != nil {
		// Default Config
		cfg = Config{
			ActiveProvider: "gemini",
			AppPort:        "3000",
			SystemPrompt:   "Kamu adalah asisten AI.",
			SavedPrompts: map[string]string{
				"Default": "Kamu adalah asisten AI.",
				"CS Ramah": "Kamu adalah Customer Service yang sangat ramah dan membantu.",
				"Formal":   "Mohon menjawab dengan bahasa Indonesia yang baku dan formal.",
			},
			Providers: map[string]ProviderConfig{
				"gemini": {Model: "gemini-1.5-flash", BaseURL: "https://generativelanguage.googleapis.com/v1beta"},
				"openai": {Model: "gpt-3.5-turbo", BaseURL: "https://api.openai.com/v1"},
				"groq":   {Model: "llama3-8b-8192", BaseURL: "https://api.groq.com/openai/v1"},
			},
		}
		saveConfig()
		return
	}
	json.Unmarshal(file, &cfg)
	if cfg.SavedPrompts == nil {
		cfg.SavedPrompts = make(map[string]string)
	}
	
	if cfg.AppPort == "" {
		cfg.AppPort = "3000"
	}

	// Default Credentials
	if cfg.AdminUsername == "" {
		cfg.AdminUsername = "admin"
	}
	if cfg.AdminPassword == "" {
		cfg.AdminPassword = "password123"
	}

	// Initial Refresh
	go refreshKnowledge()
}

func saveConfig() {
	data, _ := json.MarshalIndent(cfg, "", "  ")
	os.WriteFile(configFile, data, 0644)
}

func refreshKnowledge() {
	var sb strings.Builder
	log.Printf("Refreshing Knowledge. URLs: %d, Files: %d", len(cfg.KnowledgeURLs), len(cfg.KnowledgeFiles))
	
	// 1. Scrape URLs
	for _, url := range cfg.KnowledgeURLs {
		if url == "" { continue }
		sb.WriteString("\n\n[Source: " + url + "]\n")
		sb.WriteString(scrapeURL(url))
	}

	// 2. Read Files
	for _, filename := range cfg.KnowledgeFiles {
		content := readFile("uploads/" + filename)
		if content != "" {
			sb.WriteString("\n\n[Source File: " + filename + "]\n")
			sb.WriteString(content)
		}
	}

	knowledgeText = sb.String()
	log.Println("Knowledge Base Updated. Total chars:", len(knowledgeText))
}

func scrapeURL(url string) string {
	log.Println("Scraping content from:", url)
	res, err := http.Get(url)
	if err != nil {
		log.Println("Scrape Error:", err)
		return "[Error fetching URL]"
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return "[Error: URL unreachable]"
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return "[Error parsing content]"
	}

	// Remove scripts, styles
	doc.Find("script, style, nav, footer, header").Remove()
	
	// Get text
	text := doc.Find("body").Text()
	
	// Clean text
	lines := strings.Split(text, "\n")
	var cleanLines []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			cleanLines = append(cleanLines, trimmed)
		}
	}
	
	return strings.Join(cleanLines, "\n")
}

func readFile(path string) string {
	lowerPath := strings.ToLower(path)
	if strings.HasSuffix(lowerPath, ".txt") {
		content, err := os.ReadFile(path)
		if err != nil {
			return ""
		}
		return string(content)
	} else if strings.HasSuffix(lowerPath, ".pdf") {
		f, r, err := pdf.Open(path)

		if err != nil {
			return ""
		}
		defer f.Close()
		
		var textBuilder strings.Builder
		totalPage := r.NumPage()

		for pageIndex := 1; pageIndex <= totalPage; pageIndex++ {
			p := r.Page(pageIndex)
			if p.V.IsNull() {
				continue
			}
			s, _ := p.GetPlainText(nil)
			textBuilder.WriteString(s)
		}
		return textBuilder.String()
	}
	return ""
}

func fetchModelsHandler(c *fiber.Ctx) error {
	provider := c.Query("provider")
	apiKey := strings.TrimSpace(c.Query("api_key"))
	baseURL := strings.TrimSpace(c.Query("base_url"))

	if apiKey == "" {
		return c.Status(400).JSON(fiber.Map{"error": "API Key Required"})
	}

	var url string
	var method = "GET"
	var headers = map[string]string{}

	if provider == "gemini" {
		url = "https://generativelanguage.googleapis.com/v1beta/models?key=" + apiKey
	} else {
		// Default to OpenAI compatible
		if baseURL == "" {
			switch provider {
			case "openai":
				baseURL = "https://api.openai.com/v1"
			case "groq":
				baseURL = "https://api.groq.com/openai/v1"
			case "byteplus":
				baseURL = "https://ark.ap-southeast.bytepluses.com/api/v3"
			case "qwen":
				baseURL = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1"
			}
		}
		
		if baseURL != "" {
			baseURL = strings.TrimRight(baseURL, "/")
			url = baseURL + "/models"
			headers["Authorization"] = "Bearer " + apiKey
		} else {
			// Fallback if no base_url known
			return c.Status(400).JSON(fiber.Map{"error": "Base URL unknown for this provider"})
		}
	}

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to create request"})
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch models: " + err.Error()})
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
    
    if resp.StatusCode != 200 {
        return c.Status(resp.StatusCode).JSON(fiber.Map{"error": "Provider Error: " + string(bodyBytes)})
    }

	// Parse Response
	var result map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Invalid JSON response"})
	}

	var models []string

	if provider == "gemini" {
		if items, ok := result["models"].([]interface{}); ok {
			for _, item := range items {
				if m, ok := item.(map[string]interface{}); ok {
                    // name is like "models/gemini-pro"
					if name, ok := m["name"].(string); ok {
                        // strip "models/" prefix
                        name = strings.TrimPrefix(name, "models/")
						models = append(models, name)
					}
				}
			}
		}
	} else {
		// OpenAI compatible
		if data, ok := result["data"].([]interface{}); ok {
			for _, item := range data {
				if m, ok := item.(map[string]interface{}); ok {
					if id, ok := m["id"].(string); ok {
						models = append(models, id)
					}
				}
			}
		}
	}

	return c.JSON(fiber.Map{"models": models})
}

func processFollowups() {
	ticker := time.NewTicker(1 * time.Minute)
	log.Println("Follow-up Scheduler Started")
	for range ticker.C {
		// Fetch pending tasks
		rows, err := authDB.Query("SELECT id, user_id, jid, instruction FROM followup_tasks WHERE status = 'pending' AND scheduled_time <= ?", time.Now())
		if err != nil {
			log.Println("Scheduler Error:", err)
			continue
		}
		
		var tasks []FollowupTask
		for rows.Next() {
			var t FollowupTask
			rows.Scan(&t.ID, &t.UserID, &t.JID, &t.Instruction)
			tasks = append(tasks, t)
		}
		rows.Close()

		for _, t := range tasks {
			// Mark as processing
			authDB.Exec("UPDATE followup_tasks SET status = 'processing' WHERE id = ?", t.ID)
			
			go func(task FollowupTask) {
				// 1. Check Client (Single User Mode)
				if client == nil || !client.IsConnected() {
					log.Printf("Followup Task %d Failed: Client not connected", task.ID)
					authDB.Exec("UPDATE followup_tasks SET status = 'failed_no_client' WHERE id = ?", task.ID)
					return
				}
				
				// 2. Get Config (Global)
				// Use global cfg
				
				// Get Chat History
				historyMutex.Lock()
				history := chatHistories[task.JID]
				historyMutex.Unlock()

				contextText := ""
				// Take last 10 messages
				start := 0
				if len(history) > 10 {
					start = len(history) - 10
				}
				for _, msg := range history[start:] {
					contextText += msg + "\n"
				}
				
				// 3. Generate Content
				provName := cfg.ActiveProvider
				prov := cfg.Providers[provName]
				
				systemPrompt := fmt.Sprintf("You are a helpful assistant. Context:\n%s\n\nTask: %s\n\nGenerate a single WhatsApp message based on the instruction. Do not include 'Subject:' or explanations. Just the message body.", contextText, task.Instruction)
				
				// Use helper function
				reply, err := generateContent(provName, prov.APIKey, prov.Model, prov.BaseURL, systemPrompt, "")
				
				if err != nil {
					log.Printf("Followup Task %d AI Error: %v", task.ID, err)
					authDB.Exec("UPDATE followup_tasks SET status = 'failed_ai' WHERE id = ?", task.ID)
					return
				}

				// 4. Send Message
				targetJID, _ := types.ParseJID(task.JID)
				
				resp := &waE2E.Message{Conversation: &reply}
				
				// Send using the global client
				_, err = client.SendMessage(context.Background(), targetJID, resp)
				if err != nil {
					log.Printf("Followup Task %d Send Error: %v", task.ID, err)
					authDB.Exec("UPDATE followup_tasks SET status = 'failed_send' WHERE id = ?", task.ID)
					return
				}

				// Success
				authDB.Exec("UPDATE followup_tasks SET status = 'sent' WHERE id = ?", task.ID)
				log.Printf("Followup Task %d Sent to %s", task.ID, task.JID)
				
			}(t)
		}
	}
}

func generateContent(providerName, apiKey, modelName, baseURL, sysPrompt, userPrompt string) (string, error) {
	if providerName == "gemini" {
		if baseURL == "" {
			baseURL = "https://generativelanguage.googleapis.com/v1beta"
		}
		if modelName == "gemini-pro" {
			modelName = "gemini-1.5-flash"
		}

		url := fmt.Sprintf("%s/models/%s:generateContent?key=%s", baseURL, modelName, apiKey)
		bodyData := map[string]interface{}{
			"contents": []map[string]interface{}{
				{"parts": []map[string]interface{}{{"text": sysPrompt + "\n" + userPrompt}}},
			},
		}
		jsonBody, _ := json.Marshal(bodyData)

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonBody))
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()

		bodyBytes, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			return "", fmt.Errorf("gemini error: %s", string(bodyBytes))
		}

		var result map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &result); err != nil {
			return "", err
		}

		if candidates, ok := result["candidates"].([]interface{}); ok && len(candidates) > 0 {
			if content, ok := candidates[0].(map[string]interface{})["content"].(map[string]interface{}); ok {
				if parts, ok := content["parts"].([]interface{}); ok && len(parts) > 0 {
					return parts[0].(map[string]interface{})["text"].(string), nil
				}
			}
		}
		return "", fmt.Errorf("unexpected gemini response structure")

	} else if providerName == "vertex" {
		// Vertex AI (via Service Account)
		if apiKey == "" {
			return "", fmt.Errorf("service account json required")
		}
		
		var credsMap map[string]interface{}
		if err := json.Unmarshal([]byte(apiKey), &credsMap); err != nil {
			return "", fmt.Errorf("invalid service account json")
		}
		projectID, _ := credsMap["project_id"].(string)
		if projectID == "" {
			return "", fmt.Errorf("project_id not found in json")
		}

		location := baseURL
		if location == "" {
			location = "us-central1"
		}

		ctx := context.Background()
		creds, err := google.CredentialsFromJSON(ctx, []byte(apiKey), "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			return "", fmt.Errorf("auth error: %v", err)
		}
		token, err := creds.TokenSource.Token()
		if err != nil {
			return "", fmt.Errorf("token error: %v", err)
		}

		url := fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:generateContent", 
			location, projectID, location, modelName)

		bodyData := map[string]interface{}{
			"contents": []map[string]interface{}{
				{"parts": []map[string]interface{}{{"text": sysPrompt + "\n" + userPrompt}}},
			},
		}
		jsonBody, _ := json.Marshal(bodyData)

		req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
		req.Header.Set("Authorization", "Bearer "+token.AccessToken)
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()

		bodyBytes, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			return "", fmt.Errorf("vertex error: %s", string(bodyBytes))
		}

		var result map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &result); err != nil {
			return "", err
		}

		if candidates, ok := result["candidates"].([]interface{}); ok && len(candidates) > 0 {
			if content, ok := candidates[0].(map[string]interface{})["content"].(map[string]interface{}); ok {
				if parts, ok := content["parts"].([]interface{}); ok && len(parts) > 0 {
					return parts[0].(map[string]interface{})["text"].(string), nil
				}
			}
		}
		return "", fmt.Errorf("unexpected vertex response")

	} else {
		// OpenAI Compatible
		if baseURL == "" {
			switch providerName {
			case "openai":
				baseURL = "https://api.openai.com/v1"
			case "groq":
				baseURL = "https://api.groq.com/openai/v1"
			case "byteplus":
				baseURL = "https://ark.ap-southeast.bytepluses.com/api/v3"
			case "qwen":
				baseURL = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1"
			}
		}
		if baseURL == "" {
			return "", fmt.Errorf("base url empty")
		}

		url := fmt.Sprintf("%s/chat/completions", strings.TrimRight(baseURL, "/"))
		
		messages := []map[string]interface{}{
			{"role": "system", "content": sysPrompt},
		}
		if userPrompt != "" {
			messages = append(messages, map[string]interface{}{"role": "user", "content": userPrompt})
		}

		bodyData := map[string]interface{}{
			"model": modelName,
			"messages": messages,
		}
		jsonBody, _ := json.Marshal(bodyData)

		req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()

		bodyBytes, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			return "", fmt.Errorf("openai error: %s", string(bodyBytes))
		}

		var result map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &result); err != nil {
			return "", err
		}

		if choices, ok := result["choices"].([]interface{}); ok && len(choices) > 0 {
			if message, ok := choices[0].(map[string]interface{})["message"].(map[string]interface{}); ok {
				return message["content"].(string), nil
			}
		}
		return "", fmt.Errorf("unexpected openai response structure")
	}
}
