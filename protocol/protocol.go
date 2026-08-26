// Package protocol defines the Bid Auction Protocol (BAP/1.0): the
// application-layer protocol shared by the client, proxies, servers, and
// registry. Messages are newline-delimited JSON over TCP. Every message
// this package constructs carries a "protocol" field (e.g. "BAP/1.0"),
// the same way an HTTP request line carries "HTTP/1.1" — so the protocol
// identity is visible in every printed log line and packet capture.
package protocol

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
)

const (
	ProtocolName    = "BAP" // Bid Auction Protocol
	ProtocolVersion = "1.0"
)

// ProtocolID returns the wire-visible identifier, e.g. "BAP/1.0".
func ProtocolID() string {
	return fmt.Sprintf("%s/%s", ProtocolName, ProtocolVersion)
}

// ---------------------------------------------------------------------
// Bid message family: Client -> Proxy -> Server
// ---------------------------------------------------------------------

// BidRequest is sent Client -> Proxy -> Server.
type BidRequest struct {
	Protocol   string  `json:"protocol"`
	ClientName string  `json:"client_name"`
	ItemID     int     `json:"item_id"`
	BidAmount  float64 `json:"bid_amount"`
}

// NewBidRequest builds a BidRequest with the protocol identifier set.
func NewBidRequest(clientName string, itemID int, bidAmount float64) BidRequest {
	return BidRequest{
		Protocol:   ProtocolID(),
		ClientName: clientName,
		ItemID:     itemID,
		BidAmount:  bidAmount,
	}
}

// Bid status codes, modeled on HTTP: 2xx success, 4xx client-side
// rejection, 5xx server-side failure.
const (
	StatusBidAccepted    = 200
	StatusBidRejected    = 409 // Conflict: a higher bid already exists
	StatusItemOutOfRange = 404 // This proxy doesn't manage this item ID
	StatusServerDown     = 503 // Paired server unreachable
)

var bidStatusPhrases = map[int]string{
	StatusBidAccepted:    "OK - Bid Accepted",
	StatusBidRejected:    "Conflict - Bid Too Low",
	StatusItemOutOfRange: "Not Found - Item Out Of Range",
	StatusServerDown:     "Service Unavailable - Server Unreachable",
}

// BidResponse is sent Server -> Proxy -> Client.
type BidResponse struct {
	Protocol      string  `json:"protocol"`
	StatusCode    int     `json:"status_code"`
	StatusPhrase  string  `json:"status_phrase"`
	Reason        string  `json:"reason,omitempty"`
	ItemID        int     `json:"item_id,omitempty"`
	HighestBid    float64 `json:"highest_bid,omitempty"`
	HighestBidder string  `json:"highest_bidder,omitempty"`
}

// NewBidResponse builds a BidResponse, filling in the phrase for code
// and the protocol identifier automatically so they can never drift
// out of sync with each other.
func NewBidResponse(code, itemID int, highestBid float64, highestBidder, reason string) BidResponse {
	return BidResponse{
		Protocol:      ProtocolID(),
		StatusCode:    code,
		StatusPhrase:  bidStatusPhrases[code],
		Reason:        reason,
		ItemID:        itemID,
		HighestBid:    highestBid,
		HighestBidder: highestBidder,
	}
}

// ---------------------------------------------------------------------
// Registry message family: Proxy/Client -> Registry
// ---------------------------------------------------------------------

const (
	RegStatusOK         = 200
	RegStatusNotFound   = 404
	RegStatusBadRequest = 400
)

var registryStatusPhrases = map[int]string{
	RegStatusOK:         "OK",
	RegStatusNotFound:   "Not Found - No Proxy Registered For Item",
	RegStatusBadRequest: "Bad Request - Unknown Message Type",
}

// RegistryMessage is sent Proxy -> Registry (Type "register") or
// Client -> Registry (Type "lookup").
type RegistryMessage struct {
	Protocol string `json:"protocol"`
	Type     string `json:"type"`

	// register fields
	ProxyID   int    `json:"proxy_id,omitempty"`
	Low       int    `json:"low,omitempty"`
	High      int    `json:"high,omitempty"`
	ProxyHost string `json:"proxy_host,omitempty"`
	ProxyPort string `json:"proxy_port,omitempty"`

	// lookup fields
	ItemID int `json:"item_id,omitempty"`
}

func NewRegisterMessage(proxyID, low, high int, proxyHost, proxyPort string) RegistryMessage {
	return RegistryMessage{
		Protocol:  ProtocolID(),
		Type:      "register",
		ProxyID:   proxyID,
		Low:       low,
		High:      high,
		ProxyHost: proxyHost,
		ProxyPort: proxyPort,
	}
}

func NewLookupMessage(itemID int) RegistryMessage {
	return RegistryMessage{
		Protocol: ProtocolID(),
		Type:     "lookup",
		ItemID:   itemID,
	}
}

// RegistryResponse answers a RegistryMessage.
type RegistryResponse struct {
	Protocol     string `json:"protocol"`
	StatusCode   int    `json:"status_code"`
	StatusPhrase string `json:"status_phrase"`
	Reason       string `json:"reason,omitempty"`
	ProxyID      int    `json:"proxy_id,omitempty"`
	ProxyHost    string `json:"proxy_host,omitempty"`
	ProxyPort    string `json:"proxy_port,omitempty"`
}

func NewRegistryResponse(code, proxyID int, proxyHost, proxyPort, reason string) RegistryResponse {
	return RegistryResponse{
		Protocol:     ProtocolID(),
		StatusCode:   code,
		StatusPhrase: registryStatusPhrases[code],
		Reason:       reason,
		ProxyID:      proxyID,
		ProxyHost:    proxyHost,
		ProxyPort:    proxyPort,
	}
}

const (
	RegistryHost = "127.0.0.1"
	RegistryPort = "8000"
)

// ---------------------------------------------------------------------
// Wire framing: newline-delimited JSON
// ---------------------------------------------------------------------

func SendMsg(conn net.Conn, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = conn.Write(data)
	return err
}

func RecvMsg(reader *bufio.Reader, v interface{}) error {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	return json.Unmarshal(line, v)
}

// ---------------------------------------------------------------------
// Logging helpers: print the literal wire message. Used by every
// component so a demo/video shows exactly what BAP/1.0 sends and
// receives at each hop.
// ---------------------------------------------------------------------

func PrintSent(component string, v interface{}) {
	data, _ := json.Marshal(v)
	fmt.Printf("[%s] SENT     -> %s\n", component, string(data))
}

func PrintReceived(component string, v interface{}) {
	data, _ := json.Marshal(v)
	fmt.Printf("[%s] RECEIVED <- %s\n", component, string(data))
}