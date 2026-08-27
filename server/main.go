// Command server is one shard's auction authority. It owns state only
// for the item range it's told to handle (via flags), never sees bids
// for other ranges, and persists that state to disk two ways:
//
//   - server_<id>_audit.log    append-only, one line per bid decision
//   - server_<id>_snapshot.txt current state, rewritten after every
//     accepted bid and reloaded automatically on startup
//
// It also claims server_<id>.lock on startup (containing its own PID)
// so that a second process accidentally started for the same shard ID
// fails immediately with a clear error, instead of both processes
// silently racing to overwrite the same audit log and snapshot files.
// If the previous holder crashed without cleaning up, the lock is
// detected as stale (its PID is no longer running) and reclaimed
// automatically.
//
// Run:
//
//	go run ./server -id 1 -port 6001 -low 1   -high 100
//	go run ./server -id 2 -port 6002 -low 101 -high 200
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chrisfoong/web-socket-programming/protocol"
)

type entry struct {
	Amount float64
	Bidder string
}

var (
	auctions = make(map[int]entry)
	mu       sync.Mutex

	shardID      int
	auditLogPath string
	snapshotPath string
)

// --- Startup lock: guarantees at most one server process per shard ID ---

// isProcessAlive reports whether pid is a currently-running process.
// Uses signal 0, which checks existence without actually signaling.
func isProcessAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return process.Signal(syscall.Signal(0)) == nil
}

// acquireLock claims lockPath exclusively, writing this process's PID
// into it. If the lock already exists and its PID is still alive, it
// refuses and returns an error. If the PID is no longer running (the
// previous holder crashed), it reclaims the stale lock automatically.
func acquireLock(lockPath string) (*os.File, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err == nil {
		fmt.Fprintf(f, "%d\n", os.Getpid())
		return f, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, err
	}

	// Lock file already exists — is its owner still alive?
	if data, readErr := os.ReadFile(lockPath); readErr == nil {
		if pid, convErr := strconv.Atoi(strings.TrimSpace(string(data))); convErr == nil {
			if isProcessAlive(pid) {
				return nil, fmt.Errorf("shard already running (pid %d holds %s) — refusing to start a duplicate", pid, lockPath)
			}
		}
	}

	// Stale lock: previous holder is gone. Reclaim it.
	if err := os.Remove(lockPath); err != nil {
		return nil, fmt.Errorf("could not remove stale lock %s: %w", lockPath, err)
	}
	f, err = os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(f, "%d\n", os.Getpid())
	log.Printf("[Server %d] Reclaimed stale lock %s (previous owner not running)", shardID, lockPath)
	return f, nil
}

func releaseLock(f *os.File, lockPath string) {
	f.Close()
	os.Remove(lockPath)
}

// --- Persistence: audit log + snapshot ---

// loadSnapshot restores state from snapshotPath, if it exists. Runs once
// at startup before the listener opens, so no locking needed.
func loadSnapshot() {
	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		return // no snapshot yet — fine, start empty
	}

	restored := 0
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, " | ")
		if len(parts) != 3 {
			continue
		}
		itemStr := strings.TrimPrefix(parts[0], "item=")
		bidStr := strings.TrimPrefix(parts[1], "highest_bid=")
		bidderStr := strings.TrimPrefix(parts[2], "bidder=")

		itemID, err1 := strconv.Atoi(itemStr)
		amount, err2 := strconv.ParseFloat(bidStr, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		auctions[itemID] = entry{Amount: amount, Bidder: bidderStr}
		restored++
	}
	if restored > 0 {
		log.Printf("[Server %d] Restored %d item(s) from %s", shardID, restored, snapshotPath)
	}
}

// writeSnapshotLocked rewrites the snapshot file from current state.
// Caller must already hold mu.
func writeSnapshotLocked() {
	var sb strings.Builder
	for itemID, e := range auctions {
		sb.WriteString(fmt.Sprintf("item=%d | highest_bid=%.2f | bidder=%s\n", itemID, e.Amount, e.Bidder))
	}
	if err := os.WriteFile(snapshotPath, []byte(sb.String()), 0644); err != nil {
		log.Printf("[Server %d] snapshot write error: %v", shardID, err)
	}
}

func appendAuditLog(line string) {
	f, err := os.OpenFile(auditLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[Server %d] audit log error: %v", shardID, err)
		return
	}
	defer f.Close()
	f.WriteString(line)
}

// --- Bid handling ---

func handleProxy(conn net.Conn) {
	defer conn.Close()
	addr := conn.RemoteAddr()
	label := fmt.Sprintf("Server %d", shardID)
	log.Printf("[%s] Proxy connected: %s", label, addr)
	reader := bufio.NewReader(conn)

	for {
		var req protocol.BidRequest
		if err := protocol.RecvMsg(reader, &req); err != nil {
			break
		}
		protocol.PrintReceived(label, req)

		var resp protocol.BidResponse
		ts := time.Now().Format("2006-01-02 15:04:05")

		mu.Lock()
		current, exists := auctions[req.ItemID]
		if !exists || req.BidAmount > current.Amount {
			auctions[req.ItemID] = entry{Amount: req.BidAmount, Bidder: req.ClientName}
			fmt.Printf("[%s][%s] Item %3d | HIGHEST BID: $%.2f  by '%s'\n",
				label, time.Now().Format("15:04:05"), req.ItemID, req.BidAmount, req.ClientName)

			resp = protocol.NewBidResponse(protocol.StatusBidAccepted, req.ItemID, req.BidAmount, req.ClientName, "")
			appendAuditLog(fmt.Sprintf("%s | %d %s | item=%d | bidder=%s | amount=%.2f\n",
				ts, resp.StatusCode, "ACCEPTED", req.ItemID, req.ClientName, req.BidAmount))
			writeSnapshotLocked()
		} else {
			resp = protocol.NewBidResponse(protocol.StatusBidRejected, req.ItemID, current.Amount, current.Bidder, "bid too low")
			appendAuditLog(fmt.Sprintf("%s | %d %s | item=%d | bidder=%s | amount=%.2f | current_highest=%.2f by %s\n",
				ts, resp.StatusCode, "REJECTED", req.ItemID, req.ClientName, req.BidAmount, current.Amount, current.Bidder))
		}
		mu.Unlock()

		protocol.PrintSent(label, resp)
		if err := protocol.SendMsg(conn, resp); err != nil {
			break
		}
	}
	log.Printf("[%s] Proxy disconnected: %s", label, addr)
}

func main() {
	id := flag.Int("id", 0, "Shard ID (unique, matches its paired proxy)")
	port := flag.String("port", "", "Port this server listens on")
	low := flag.Int("low", 0, "Lowest item ID this shard owns (for logging only)")
	high := flag.Int("high", 0, "Highest item ID this shard owns (for logging only)")
	flag.Parse()

	if *id == 0 || *port == "" {
		log.Fatal("Usage: server -id <n> -port <n> [-low <n> -high <n>]")
	}
	shardID = *id
	dataDir := fmt.Sprintf("data/server_%d", shardID)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("[Server %d] could not create data directory %s: %v", shardID, dataDir, err)
	}
	auditLogPath = filepath.Join(dataDir, fmt.Sprintf("server_%d_audit.log", shardID))
	snapshotPath = filepath.Join(dataDir, fmt.Sprintf("server_%d_snapshot.txt", shardID))
	lockPath := filepath.Join(dataDir, fmt.Sprintf("server_%d.lock", shardID))

	lockFile, err := acquireLock(lockPath)
	if err != nil {
		log.Fatalf("[Server %d] %v", shardID, err)
	}

	// Release the lock on graceful shutdown (Ctrl+C / SIGTERM). Without
	// this, deferred cleanup wouldn't run on a plain signal-based exit.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("[Server %d] Shutting down, releasing lock...", shardID)
		releaseLock(lockFile, lockPath)
		os.Exit(0)
	}()

	loadSnapshot()

	ln, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		releaseLock(lockFile, lockPath)
		log.Fatal(err)
	}
	defer ln.Close()
	defer releaseLock(lockFile, lockPath)
	log.Printf("Server %d listening on 0.0.0.0:%s (owns items %d-%d) | audit=%s snapshot=%s lock=%s",
		shardID, *port, *low, *high, auditLogPath, snapshotPath, lockPath)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("accept error:", err)
			continue
		}
		go handleProxy(conn)
	}
}