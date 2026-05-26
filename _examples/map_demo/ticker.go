package main

import (
	"github.com/centrifugal/centrifuge"
)

// Ticker sector assignments.
var tickerSectors = map[string]string{
	"AAPL": "tech", "GOOG": "tech", "MSFT": "tech", "META": "tech", "NVDA": "tech",
	"AMZN": "ecommerce",
	"TSLA": "auto",
	"NFLX": "media",
	"ORCL": "enterprise", "CRM": "enterprise",
}

func roundToTwoDecimals(f float64) float64 { _ = "STUB: not implemented"; return 0 }

// publishTickerData publishes random stock prices to the "tickers" map channel.
// Each tick updates 2-4 randomly chosen tickers (not all at once), which is more
// realistic and produces visually distinct highlights on the client.
func publishTickerData(node *centrifuge.Node) { _ = "STUB: not implemented"; return }

// Pick 2-4 random tickers to update this tick.
