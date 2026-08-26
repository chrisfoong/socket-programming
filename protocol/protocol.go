// Package protocol defines the wire format and static topology shared by
// the client, proxies, and server: newline-delimited JSON over TCP.
package protocol

import (
	"bufio"
	"encoding/json"
	"net"
)

// BidRequest is sent Client -> Proxy -> Server.
type BidRequest struct {
	ClientName string  `json:"client_name"`
	ItemID     int     `json:"item_id"`
	BidAmount  float64 `json:"bid_amount"`
}

// BidResponse is sent Server -> Proxy -> Client.
// Status is one of "accepted", "rejected", "error".
type BidResponse struct {
	Status        string  `json:"status"`
	Reason        string  `json:"reason,omitempty"`
	ItemID        int     `json:"item_id,omitempty"`
	HighestBid    float64 `json:"highest_bid,omitempty"`
	HighestBidder string  `json:"highest_bidder,omitempty"`
}

const (
	RegistryHost = "127.0.0.1"
	RegistryPort = "8000"
)

// RegistryMessage is sent Proxy -> Registry (Type "register") or
// Client -> Registry (Type "lookup"). Fields are grouped by which
// message type uses them; irrelevant fields are omitted on the wire.
type RegistryMessage struct {
	Type string `json:"type"` // "register" or "lookup"

	// register fields
	ProxyID   int    `json:"proxy_id,omitempty"`
	Low       int    `json:"low,omitempty"`
	High      int    `json:"high,omitempty"`
	ProxyHost string `json:"proxy_host,omitempty"`
	ProxyPort string `json:"proxy_port,omitempty"`

	// lookup fields
	ItemID int `json:"item_id,omitempty"`
}

// RegistryResponse answers a RegistryMessage. Status is "ok", "not_found",
// or "error". For a successful lookup, ProxyID/ProxyHost/ProxyPort name
// the proxy the client should connect to next.
type RegistryResponse struct {
	Status    string `json:"status"`
	Reason    string `json:"reason,omitempty"`
	ProxyID   int    `json:"proxy_id,omitempty"`
	ProxyHost string `json:"proxy_host,omitempty"`
	ProxyPort string `json:"proxy_port,omitempty"`
}

// SendMsg encodes v as JSON and writes it newline-terminated to conn.
func SendMsg(conn net.Conn, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = conn.Write(data)
	return err
}

// RecvMsg reads one newline-delimited JSON message from reader into v.
func RecvMsg(reader *bufio.Reader, v interface{}) error {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	return json.Unmarshal(line, v)
}