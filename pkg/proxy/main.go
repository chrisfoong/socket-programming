// Command proxy owns a range of item IDs given via flags and registers
// that range with the registry on startup — nothing about it is
// hardcoded or shared with other processes at compile time. It accepts
// bids from clients, rejects any item ID outside its own range, forwards
// valid bids to its paired server, and relays the server's response back
// to the client.
//
// Because registration happens dynamically, you can bring up a brand new
// proxy (a new range, or another instance of an existing range behind a
// load balancer) at any time without editing any other file.
//
// Run:
//
//	go run ./pkg/proxy -id 1 -low 1   -high 100 -port 7001 -server-port 6001
//	go run ./pkg/proxy -id 2 -low 101 -high 200 -port 7002 -server-port 6002
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"

	"github.com/chrisfoong/web-socket-programming/protocol"
)

func registerWithRegistry(proxyID, low, high int, proxyHost, proxyPort string) error {
	conn, err := net.Dial("tcp", protocol.RegistryHost+":"+protocol.RegistryPort)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := protocol.RegistryMessage{
		Type:      "register",
		ProxyID:   proxyID,
		Low:       low,
		High:      high,
		ProxyHost: proxyHost,
		ProxyPort: proxyPort,
	}
	if err := protocol.SendMsg(conn, msg); err != nil {
		return err
	}

	reader := bufio.NewReader(conn)
	var resp protocol.RegistryResponse
	if err := protocol.RecvMsg(reader, &resp); err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("registry rejected registration: %s", resp.Reason)
	}
	return nil
}

func forwardToServer(serverHost, serverPort string, req protocol.BidRequest) (protocol.BidResponse, error) {
	var resp protocol.BidResponse

	conn, err := net.Dial("tcp", serverHost+":"+serverPort)
	if err != nil {
		return resp, err
	}
	defer conn.Close()

	if err := protocol.SendMsg(conn, req); err != nil {
		return resp, err
	}
	reader := bufio.NewReader(conn)
	if err := protocol.RecvMsg(reader, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

func handleClient(conn net.Conn, low, high int, serverHost, serverPort string, proxyID int) {
	defer conn.Close()
	addr := conn.RemoteAddr()
	log.Printf("[Proxy %d] Client connected: %s", proxyID, addr)
	reader := bufio.NewReader(conn)

	for {
		var req protocol.BidRequest
		if err := protocol.RecvMsg(reader, &req); err != nil {
			break
		}

		if req.ItemID < low || req.ItemID > high {
			resp := protocol.BidResponse{
				Status: "error",
				Reason: "Proxy " + strconv.Itoa(proxyID) + " only manages items " +
					strconv.Itoa(low) + "-" + strconv.Itoa(high),
			}
			if err := protocol.SendMsg(conn, resp); err != nil {
				break
			}
			continue
		}

		log.Printf("[Proxy %d] Forwarding bid -> item=%d client=%s amount=%.2f",
			proxyID, req.ItemID, req.ClientName, req.BidAmount)

		resp, err := forwardToServer(serverHost, serverPort, req)
		if err != nil {
			resp = protocol.BidResponse{Status: "error", Reason: "Auction server unreachable"}
		}

		if err := protocol.SendMsg(conn, resp); err != nil {
			break
		}
	}
	log.Printf("[Proxy %d] Client disconnected: %s", proxyID, addr)
}

func main() {
	id := flag.Int("id", 0, "Proxy ID (unique)")
	low := flag.Int("low", 0, "Lowest item ID this proxy manages")
	high := flag.Int("high", 0, "Highest item ID this proxy manages")
	proxyHost := flag.String("host", "127.0.0.1", "Host this proxy listens/advertises on")
	proxyPort := flag.String("port", "", "Port this proxy listens on")
	serverHost := flag.String("server-host", "127.0.0.1", "Host of the paired server")
	serverPort := flag.String("server-port", "", "Port of the paired server")
	flag.Parse()

	if *id == 0 || *low == 0 || *high == 0 || *proxyPort == "" || *serverPort == "" {
		log.Fatal("Usage: proxy -id <n> -low <n> -high <n> -port <n> -server-port <n> [-host h] [-server-host h]")
	}

	if err := registerWithRegistry(*id, *low, *high, *proxyHost, *proxyPort); err != nil {
		log.Fatalf("Could not register with registry: %v", err)
	}
	log.Printf("Proxy %d registered with registry (items %d-%d)", *id, *low, *high)

	ln, err := net.Listen("tcp", *proxyHost+":"+*proxyPort)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	log.Printf("Proxy %d listening on %s:%s (manages items %d-%d, forwards to %s:%s)",
		*id, *proxyHost, *proxyPort, *low, *high, *serverHost, *serverPort)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("accept error:", err)
			continue
		}
		go handleClient(conn, *low, *high, *serverHost, *serverPort, *id)
	}
}