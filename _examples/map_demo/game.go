package main

import (
	"time"

	"github.com/centrifugal/centrifuge"
)

type GameInfo struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	CreatedBy  string    `json:"createdBy"`
	CreatedAt  time.Time `json:"createdAt"`
	MaxPlayers int       `json:"maxPlayers"`
}

type GamePlayer struct {
	UserID   string `json:"userId"`
	Name     string `json:"name"`
	ClientID string `json:"clientId"`
	Slot     int    `json:"slot"`
}

func handleGameCreate(client *centrifuge.Client, node *centrifuge.Node, data []byte, cb centrifuge.RPCCallback) {
	_ = "STUB: not implemented"
	return
}

// Generate unique game ID using timestamp + random suffix.

// Publish game to games list channel (single source of truth).

func handleGameJoin(client *centrifuge.Client, node *centrifuge.Node, data []byte, cb centrifuge.RPCCallback) {
	_ = "STUB: not implemented"
	return
}

// Read game info from map broker (single source of truth).

// Use KeyModeIfNew to prevent slot stealing.

// Check if game is full.

func handleGameLeave(_ *centrifuge.Client, node *centrifuge.Node, data []byte, cb centrifuge.RPCCallback) {
	_ = "STUB: not implemented"
	return
}

func checkGameFull(node *centrifuge.Node, gameID string, maxPlayers int) {
	_ = "STUB: not implemented"
	return
}

// Read state - by default reads fresh data from backend (safe for CAS operations).

// Count players (slots).

// Game is full - publish game start event.

// Remove game from games list and clear game channel after delay.

// Remove from games list.

// Clear all slots and event.
