package main

import (
	"github.com/centrifugal/centrifuge"
)

// Inventory items (in-memory for demo).
var inventoryItems = map[string]*InventoryItem{
	"golden_ticket": {ID: "golden_ticket", Name: "Golden Ticket", Price: 100, Stock: 3, Emoji: "🎫"},
	"rare_potion":   {ID: "rare_potion", Name: "Rare Potion", Price: 50, Stock: 5, Emoji: "🧪"},
	"dragon_egg":    {ID: "dragon_egg", Name: "Dragon Egg", Price: 500, Stock: 1, Emoji: "🥚"},
}

type InventoryItem struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Price int    `json:"price"`
	Stock int    `json:"stock"`
	Emoji string `json:"emoji"`
}

type InventoryTransaction struct {
	Action   string `json:"action"` // "purchase" or "restock"
	ItemID   string `json:"itemId"`
	Quantity int    `json:"quantity"`
	BuyerID  string `json:"buyerId,omitempty"`
	Message  string `json:"message"`
}

type InventoryPayload struct {
	Item        InventoryItem         `json:"item"`
	Transaction *InventoryTransaction `json:"transaction,omitempty"`
}

// initInventory initializes inventory items in the map broker on startup.
func initInventory(node *centrifuge.Node) { _ = "STUB: not implemented"; return }

// Note: SyncMode/RetentionMode are configured via GetMapChannelOptions in node config.

// Only set if item doesn't exist yet.

// handleInventoryBuy handles purchase requests with CAS to prevent overselling.
func handleInventoryBuy(client *centrifuge.Client, node *centrifuge.Node, data []byte, cb centrifuge.RPCCallback) {
	_ = "STUB: not implemented"
	return
}

// Add delay to make it easier to test concurrent purchases from UI.

// CAS retry loop - keeps trying until success or terminal failure.

// Step 1: Read current state using Key filter (single key lookup).
// By default reads fresh data from backend (safe for CAS operations).

// Parse current item state.

// Check stock.

// Not enough stock - return error with current state.

// Step 2: Prepare new state (decrement stock).

// Combined payload with item state and transaction info.

// Step 3: CAS write - only succeeds if position matches what we read.

// Note: SyncMode/RetentionMode are configured via GetMapChannelOptions in node config.

// Check if CAS succeeded.

// Success! Purchase completed.

// CAS failed - someone else modified the item.

// Loop re-reads state at the top of the next iteration.

// Unknown suppression reason.

// Exhausted retries.

// handleInventoryRestock adds stock to an item (admin action).
func handleInventoryRestock(_ *centrifuge.Client, node *centrifuge.Node, data []byte, cb centrifuge.RPCCallback) {
	_ = "STUB: not implemented"
	return
}

// CAS retry loop for restock.

// By default reads fresh data from backend (safe for CAS operations).

// Prepare new state.

// Note: SyncMode/RetentionMode are configured via GetMapChannelOptions in node config.

// Loop re-reads state at the top of the next iteration.
