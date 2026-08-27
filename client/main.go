// Command client places a bid on an auction item. It asks the registry
// which proxy currently owns that item's ID range, then connects
// directly to that proxy — it never needs to know the topology itself.
//
// Every message the client sends or receives is printed to the
// terminal in the BAP/1.0 wire format, including the numeric status
// code and phrase, so a demo shows exactly what went over the network.
//
// Non-interactive:
//
//	go run ./client -name Alice -item 42 -amount 150.0
//
// Interactive (omit any flag to be prompted for it):
//
//	go run ./client
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/chrisfoong/web-socket-programming/protocol"
)

func lookupProxy(itemID int) (protocol.RegistryResponse, error) {
	conn, err := net.Dial("tcp", protocol.RegistryHost+":"+protocol.RegistryPort)
	if err != nil {
		return protocol.RegistryResponse{}, err
	}
	defer conn.Close()

	msg := protocol.NewLookupMessage(itemID)
	protocol.PrintSent("Client", msg)
	if err := protocol.SendMsg(conn, msg); err != nil {
		return protocol.RegistryResponse{}, err
	}

	reader := bufio.NewReader(conn)
	var resp protocol.RegistryResponse
	if err := protocol.RecvMsg(reader, &resp); err != nil {
		return protocol.RegistryResponse{}, err
	}
	protocol.PrintReceived("Client", resp)
	return resp, nil
}

func placeBid(name string, itemID int, amount float64) {
	lookup, err := lookupProxy(itemID)
	if err != nil {
		fmt.Printf("Could not reach registry at %s:%s. Is it running?\n", protocol.RegistryHost, protocol.RegistryPort)
		return
	}
	if lookup.StatusCode != protocol.RegStatusOK {
		reason := lookup.Reason
		if reason == "" {
			reason = lookup.StatusPhrase
		}
		fmt.Printf("No proxy manages item %d (%d %s).\n", itemID, lookup.StatusCode, reason)
		return
	}

	conn, err := net.Dial("tcp", lookup.ProxyHost+":"+lookup.ProxyPort)
	if err != nil {
		fmt.Printf("Could not reach Proxy %d at %s:%s. Is it running?\n", lookup.ProxyID, lookup.ProxyHost, lookup.ProxyPort)
		return
	}
	defer conn.Close()

	req := protocol.NewBidRequest(name, itemID, amount)
	protocol.PrintSent("Client", req)
	if err := protocol.SendMsg(conn, req); err != nil {
		fmt.Println("Failed to send bid:", err)
		return
	}

	reader := bufio.NewReader(conn)
	var resp protocol.BidResponse
	if err := protocol.RecvMsg(reader, &resp); err != nil {
		fmt.Println("No response from proxy.")
		return
	}
	protocol.PrintReceived("Client", resp)

	switch resp.StatusCode {
	case protocol.StatusBidAccepted:
		fmt.Printf("Bid accepted! [%d %s] You are the highest bidder on item %d at $%.2f.\n",
			resp.StatusCode, resp.StatusPhrase, itemID, resp.HighestBid)
	case protocol.StatusBidRejected:
		fmt.Printf("Bid rejected [%d %s]: item %d already has a higher bid of $%.2f by '%s'.\n",
			resp.StatusCode, resp.StatusPhrase, itemID, resp.HighestBid, resp.HighestBidder)
	default:
		fmt.Printf("Error [%d %s]: %s\n", resp.StatusCode, resp.StatusPhrase, resp.Reason)
	}
}

func prompt(label string) string {
	fmt.Print(label)
	reader := bufio.NewReader(os.Stdin)
	text, _ := reader.ReadString('\n')
	return strings.TrimSpace(text)
}

func main() {
	name := flag.String("name", "", "Your name")
	item := flag.Int("item", 0, "Auction item ID")
	amount := flag.Float64("amount", 0, "Bid amount")
	flag.Parse()

	n := *name
	if n == "" {
		n = prompt("Your name: ")
	}

	it := *item
	if it == 0 {
		v := prompt("Item ID: ")
		parsed, err := strconv.Atoi(v)
		if err != nil {
			log.Fatal("invalid item id")
		}
		it = parsed
	}

	amt := *amount
	if amt == 0 {
		v := prompt("Bid amount: $")
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			log.Fatal("invalid amount")
		}
		amt = parsed
	}

	placeBid(n, it, amt)
}