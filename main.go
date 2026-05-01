package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/time/rate"
)

type User struct {
	ID          string `json:"_id"` // MongoDB ID, not used as key
	UserID      string `json:"userId"`
	DisplayName string `json:"displayName"`
	Avatar      string `json:"avatar"`
	IsOnline    bool   `json:"isOnline"`
}

type ApiResponse struct {
	Users               []User `json:"users"`
	Total               int    `json:"total"`
	Page                int    `json:"page"`
	HasMore             bool   `json:"hasMore"`
	CanSeeFollowersList bool   `json:"canSeeFollowersList"`
}

var (
	db          *sql.DB
	httpClient  = &http.Client{Timeout: 15 * time.Second}
	apiBase     = "https://api.vasl.fun/api"
	authToken   string
	rateLimiter *rate.Limiter
	reportTick  = 10 * time.Second
	workerCount = 5
	maxRPS      = 2.0 // requests per second
)

func main() {
	dbPath := flag.String("db", "vasl_crawler.db", "SQLite database file path")
	token := flag.String("token", "", "Bearer token (or set VASL_TOKEN env)")
	seedUsers := flag.String("seeds", "2882540,8939454", "Comma‑separated seed user IDs")
	reportInterval := flag.Int("report", 10, "Status report interval in seconds (0 disables)")
	workers := flag.Int("workers", 5, "Number of concurrent workers")
	rps := flag.Float64("rps", 2.0, "Maximum API requests per second (global)")
	flag.Parse()

	if *token == "" {
		*token = os.Getenv("VASL_TOKEN")
	}
	if *token == "" {
		log.Fatal("No API token provided. Use -token flag or VASL_TOKEN environment variable.")
	}
	authToken = *token
	reportTick = time.Duration(*reportInterval) * time.Second
	workerCount = *workers

	rateLimiter = rate.NewLimiter(rate.Limit(*rps), 1)

	var err error
	db, err = sql.Open("sqlite3", *dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := initDB(); err != nil {
		log.Fatalf("Failed to initialise database: %v", err)
	}

	seeds := strings.Split(*seedUsers, ",")
	for _, s := range seeds {
		s = strings.TrimSpace(s)
		if s != "" {
			ensureUserExists(s)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt)
		<-sigCh
		log.Println("Interrupt received, shutting down...")
		cancel()
	}()

	stopReporter := make(chan struct{})
	if reportTick > 0 {
		go func() {
			ticker := time.NewTicker(reportTick)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					printStatus()
				case <-stopReporter:
					return
				}
			}
		}()
	}

	log.Printf("Crawler started. %d workers, rate limit %.1f req/s. Press Ctrl+C to stop.", workerCount, *rps)
	runConcurrentCrawler(ctx)

	close(stopReporter)
	printStatus()
	log.Println("Crawler shut down cleanly.")
}

func initDB() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			user_id TEXT PRIMARY KEY,
			display_name TEXT,
			avatar TEXT,
			followers_next_page INTEGER NOT NULL DEFAULT 1,
			followers_done INTEGER NOT NULL DEFAULT 0,
			following_next_page INTEGER NOT NULL DEFAULT 1,
			following_done INTEGER NOT NULL DEFAULT 0,
			last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
			crawling INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS edges (
			follower_id TEXT NOT NULL,
			followee_id TEXT NOT NULL,
			PRIMARY KEY (follower_id, followee_id)
		);
		CREATE INDEX IF NOT EXISTS idx_edges_follower ON edges(follower_id);
		CREATE INDEX IF NOT EXISTS idx_edges_followee ON edges(followee_id);
	`)
	if err != nil {
		return err
	}

	rows, err := db.Query("PRAGMA table_info(users)")
	if err != nil {
		return err
	}
	defer rows.Close()
	hasCrawling := false
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == "crawling" {
			hasCrawling = true
			break
		}
	}
	if !hasCrawling {
		log.Println("Adding 'crawling' column to users table...")
		_, err = db.Exec("ALTER TABLE users ADD COLUMN crawling INTEGER NOT NULL DEFAULT 0")
		if err != nil {
			return err
		}
	}

	return nil
}

func ensureUserExists(userID string) {
	_, err := db.Exec(`INSERT OR IGNORE INTO users (user_id, display_name) VALUES (?, ?)`, userID, "")
	if err != nil {
		log.Printf("Error ensuring user %s exists: %v", userID, err)
	}
}

func runConcurrentCrawler(ctx context.Context) {
	workCh := make(chan string, workerCount*2)

	for i := 0; i < workerCount; i++ {
		go worker(ctx, i+1, workCh)
	}

	dispatcher(ctx, workCh)

	close(workCh)
}

func dispatcher(ctx context.Context, workCh chan<- string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		tx, err := db.Begin()
		if err != nil {
			log.Printf("Dispatcher: cannot begin tx: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		var uid string
		err = tx.QueryRow(`
			SELECT user_id FROM users
			WHERE (followers_done = 0 OR following_done = 0) AND crawling = 0
			ORDER BY last_updated ASC
			LIMIT 1
		`).Scan(&uid)
		if err == sql.ErrNoRows {
			tx.Rollback()

			var inProgress int
			db.QueryRow("SELECT COUNT(*) FROM users WHERE crawling = 1").Scan(&inProgress)
			if inProgress == 0 {

				return
			}

			time.Sleep(1 * time.Second)
			continue
		}
		if err != nil {
			tx.Rollback()
			log.Printf("Dispatcher query error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		res, err := tx.Exec(`UPDATE users SET crawling = 1 WHERE user_id = ? AND crawling = 0`, uid)
		if err != nil || rowsAffected(res) == 0 {
			tx.Rollback()
			continue
		}

		if err := tx.Commit(); err != nil {
			log.Printf("Dispatcher commit error: %v", err)
			continue
		}

		select {
		case workCh <- uid:
		case <-ctx.Done():
			return
		}
	}
}

func rowsAffected(r sql.Result) int64 {
	n, _ := r.RowsAffected()
	return n
}

func worker(ctx context.Context, id int, workCh <-chan string) {
	log.Printf("Worker %d started", id)
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d shutting down", id)
			return
		case userID, ok := <-workCh:
			if !ok {
				log.Printf("Worker %d: no more work", id)
				return
			}
			log.Printf("[Worker %d] processing user %s", id, userID)

			if err := crawlEndpoint(ctx, userID, "vaslers", "follower"); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[Worker %d] error on followers of %s: %v", id, userID, err)
			}

			if err := crawlEndpoint(ctx, userID, "vasling", "following"); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[Worker %d] error on following of %s: %v", id, userID, err)
			}

			if err := finaliseUser(userID); err != nil {
				log.Printf("[Worker %d] error finalising user %s: %v", id, userID, err)
			}
		}
	}
}

func finaliseUser(userID string) error {
	_, err := db.Exec(`UPDATE users SET crawling = 0 WHERE user_id = ?`, userID)
	return err
}

func printStatus() {
	var totalUsers, usersFullyCrawled, usersPartially, totalEdges int

	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&totalUsers)
	db.QueryRow("SELECT COUNT(*) FROM users WHERE followers_done = 1 AND following_done = 1").Scan(&usersFullyCrawled)
	usersPartially = totalUsers - usersFullyCrawled
	db.QueryRow("SELECT COUNT(*) FROM edges").Scan(&totalEdges)

	log.Printf("📊 Status: users total=%d | fully crawled=%d | in queue=%d | edges=%d",
		totalUsers, usersFullyCrawled, usersPartially, totalEdges)
}

func crawlEndpoint(ctx context.Context, userID, endpoint, direction string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		shouldFetch, nextPage, err := getNextPage(userID, endpoint)
		if err != nil {
			return err
		}
		if !shouldFetch {
			return nil
		}

		if err := rateLimiter.Wait(ctx); err != nil {
			return err
		}

		log.Printf("[%s] Fetching %s page %d", userID, endpoint, nextPage)
		url := fmt.Sprintf("%s/user/%s/%s?page=%d", apiBase, userID, endpoint, nextPage)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("creating request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+authToken)
		req.Header.Set("Accept", "application/json, text/plain, */*")
		req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36")
		req.Header.Set("Referer", "https://vasl.fun/")
		req.Header.Set("DNT", "1")

		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
		}

		var data ApiResponse
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			return fmt.Errorf("json decode error: %w", err)
		}

		tx, err := db.Begin()
		if err != nil {
			return err
		}
		stmtUser, err := tx.Prepare(`INSERT OR IGNORE INTO users (user_id, display_name, avatar) VALUES (?, ?, ?)`)
		if err != nil {
			tx.Rollback()
			return err
		}
		defer stmtUser.Close()

		stmtEdge, err := tx.Prepare(`INSERT OR IGNORE INTO edges (follower_id, followee_id) VALUES (?, ?)`)
		if err != nil {
			tx.Rollback()
			return err
		}
		defer stmtEdge.Close()

		for _, u := range data.Users {
			if _, err := stmtUser.Exec(u.UserID, u.DisplayName, u.Avatar); err != nil {
				tx.Rollback()
				return err
			}
			if direction == "follower" {
				if _, err := stmtEdge.Exec(u.UserID, userID); err != nil {
					tx.Rollback()
					return err
				}
			} else {
				if _, err := stmtEdge.Exec(userID, u.UserID); err != nil {
					tx.Rollback()
					return err
				}
			}
		}

		hasMoreInt := 0
		if data.HasMore {
			hasMoreInt = 1
		}
		var updateSQL string
		if endpoint == "vaslers" {
			updateSQL = `UPDATE users SET followers_next_page = ?, followers_done = CASE WHEN ? = 0 THEN 1 ELSE 0 END, last_updated = CURRENT_TIMESTAMP WHERE user_id = ?`
		} else {
			updateSQL = `UPDATE users SET following_next_page = ?, following_done = CASE WHEN ? = 0 THEN 1 ELSE 0 END, last_updated = CURRENT_TIMESTAMP WHERE user_id = ?`
		}
		nextPageToSet := nextPage + 1
		if _, err := tx.Exec(updateSQL, nextPageToSet, hasMoreInt, userID); err != nil {
			tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}

		log.Printf("[%s] %s page %d done: %d users, hasMore=%v", userID, endpoint, nextPage, len(data.Users), data.HasMore)

	}
}

func getNextPage(userID, endpoint string) (bool, int, error) {
	var nextPage int
	var done int
	if endpoint == "vaslers" {
		err := db.QueryRow(
			`SELECT followers_next_page, followers_done FROM users WHERE user_id = ?`,
			userID,
		).Scan(&nextPage, &done)
		if err != nil {
			return false, 0, err
		}
	} else {
		err := db.QueryRow(
			`SELECT following_next_page, following_done FROM users WHERE user_id = ?`,
			userID,
		).Scan(&nextPage, &done)
		if err != nil {
			return false, 0, err
		}
	}
	return done == 0, nextPage, nil
}
