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
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/ledongthuc/pdf"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
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

type DBConfig struct {
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
)

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
	container, err = sqlstore.New(context.Background(), "sqlite", "file:wahaku.db?_pragma=foreign_keys(1)&_busy_timeout=5000&_journal_mode=WAL", dbLog)
	if err != nil {
		log.Fatal("Failed to connect to DB:", err)
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

	// 5. Setup Fiber
	app := fiber.New(fiber.Config{
		BodyLimit: 50 * 1024 * 1024, // 50MB Limit
	})
	app.Use(cors.New())

	// Ensure uploads directory exists
	os.MkdirAll("uploads", 0755)

	// Serve Static Files
	app.Static("/", "./views")

	// API Routes
	api := app.Group("/api")
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

	api.Get("/models", fetchModelsHandler)

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
		// Ignore status updates or self messages
		if v.Info.IsFromMe || v.Info.IsGroup || v.Info.Sender.User == "status" {
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

		// Call AI
		go func() {
			reply := callAI(msg)
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
	return callAILoop(prompt, 0)
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
		
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonBody))
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
		
		client := &http.Client{}
		resp, err := client.Do(req)
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
		return
	}
	
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", 
		cfg.Database.User, 
		cfg.Database.Password, 
		cfg.Database.Host, 
		cfg.Database.Port, 
		cfg.Database.Name)
	
	var err error
	appDB, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Println("DB Connect Error:", err)
		return
	}
	
	if err := appDB.Ping(); err != nil {
		log.Println("DB Ping Error:", err)
		return
	}
	
	log.Println("Connected to Application Database:", cfg.Database.Name)
	refreshSchema()
}

func refreshSchema() {
	if appDB == nil {
		return
	}
	
	rows, err := appDB.Query("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ?", cfg.Database.Name)
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
