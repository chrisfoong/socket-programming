// Command registry is the discovery service that makes proxies dynamic.
// Instead of a hardcoded, compiled-in table of proxy addresses, each
// proxy registers itself here on startup with the item range it owns.
// Clients ask the registry which proxy handles a given item ID instead
// of computing it locally. This means proxies can be started, stopped,
// or given new ranges without touching any other process's code or
// config — the registry is the only thing that needs to know about them.
//
// Every registration is also appended to registry_audit.log as a plain
// text record, so you have a durable trail of which proxies came and
// went and when.
//
// Run:
//
//	go run ./registry
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/chrisfoong/web-socket-programming/protocol"
)

type registeredProxy struct {
	ProxyID      int
	Low, High    int
	Host, Port   string
	RegisteredAt time.Time
}

var (
	proxies []registeredProxy
	mu      sync.Mutex
)

// dataDir is where the registry keeps its own audit log, kept separate
// from every server's data directory.
const dataDir = "data/registry"

var auditLogPath = filepath.Join(dataDir, "registry_audit.log")

// ensureDataDir creates dataDir (and any missing parents) if it doesn't
// already exist. Safe to call even if it's already there.
func ensureDataDir() error {
	return os.MkdirAll(dataDir, 0755)
}

func appendAuditLog(line string) {
	f, err := os.OpenFile(auditLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[Registry] audit log error: %v", err)
		return
	}
	defer f.Close()
	f.WriteString(line)
}

// findProxyForItem assumes the caller already holds mu.
func findProxyForItem(itemID int) (registeredProxy, bool) {
	for _, p := range proxies {
		if itemID >= p.Low && itemID <= p.High {
			return p, true
		}
	}
	return registeredProxy{}, false
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		var msg protocol.RegistryMessage
		if err := protocol.RecvMsg(reader, &msg); err != nil {
			break
		}

		switch msg.Type {
		case "register":
			entry := registeredProxy{
				ProxyID:      msg.ProxyID,
				Low:          msg.Low,
				High:         msg.High,
				Host:         msg.ProxyHost,
				Port:         msg.ProxyPort,
				RegisteredAt: time.Now(),
			}

			mu.Lock()
			replaced := false
			for i, p := range proxies {
				if p.ProxyID == entry.ProxyID {
					proxies[i] = entry
					replaced = true
					break
				}
			}
			if !replaced {
				proxies = append(proxies, entry)
			}
			mu.Unlock()

			appendAuditLog(fmt.Sprintf("%s | REGISTER | proxy=%d | range=%d-%d | addr=%s:%s\n",
				entry.RegisteredAt.Format("2006-01-02 15:04:05"), entry.ProxyID, entry.Low, entry.High, entry.Host, entry.Port))
			log.Printf("[Registry] Proxy %d registered: items %d-%d at %s:%s",
				entry.ProxyID, entry.Low, entry.High, entry.Host, entry.Port)

			protocol.SendMsg(conn, protocol.RegistryResponse{Status: "ok"})

		case "lookup":
			mu.Lock()
			p, found := findProxyForItem(msg.ItemID)
			mu.Unlock()

			if !found {
				protocol.SendMsg(conn, protocol.RegistryResponse{
					Status: "not_found",
					Reason: fmt.Sprintf("no proxy registered for item %d", msg.ItemID),
				})
				continue
			}
			protocol.SendMsg(conn, protocol.RegistryResponse{
				Status:    "ok",
				ProxyID:   p.ProxyID,
				ProxyHost: p.Host,
				ProxyPort: p.Port,
			})

		default:
			protocol.SendMsg(conn, protocol.RegistryResponse{Status: "error", Reason: "unknown message type"})
		}
	}
}

func main() {
	if err := ensureDataDir(); err != nil {
		log.Fatalf("[Registry] could not create data directory %s: %v", dataDir, err)
	}

	ln, err := net.Listen("tcp", ":"+protocol.RegistryPort)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	log.Printf("Registry listening on 0.0.0.0:%s (audit log: %s)", protocol.RegistryPort, auditLogPath)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("accept error:", err)
			continue
		}
		go handleConn(conn)
	}
}