package main

import (
	"context"

	"github.com/centrifugal/centrifuge"
)

type MatchEvent struct {
	Minute int    `json:"minute"`
	Type   string `json:"type"`
	Team   string `json:"team"`
	Player string `json:"player"`
}

type MatchData struct {
	MatchID     string       `json:"match_id"`
	HomeTeam    string       `json:"home_team"`
	AwayTeam    string       `json:"away_team"`
	HomeScore   int          `json:"home_score"`
	AwayScore   int          `json:"away_score"`
	Minute      int          `json:"minute"`
	Status      string       `json:"status"`
	HomePoss    int          `json:"home_poss"`
	HomeShots   int          `json:"home_shots"`
	AwayShots   int          `json:"away_shots"`
	HomePasses  int          `json:"home_passes"`
	AwayPasses  int          `json:"away_passes"`
	HomeCorners int          `json:"home_corners"`
	AwayCorners int          `json:"away_corners"`
	HomeFouls   int          `json:"home_fouls"`
	AwayFouls   int          `json:"away_fouls"`
	HomeYellow  int          `json:"home_yellow"`
	AwayYellow  int          `json:"away_yellow"`
	HomeRed     int          `json:"home_red"`
	AwayRed     int          `json:"away_red"`
	Events      []MatchEvent `json:"events"`
	UpdatedAt   int64        `json:"updated_at"`
}

type matchSim struct {
	id       string
	homeTeam string
	awayTeam string
	data     MatchData
}

var matchConfigs = []struct {
	id   string
	home string
	away string
}{
	{"match1", "Barcelona", "Real Madrid"},
	{"match2", "Liverpool", "Man City"},
	{"match3", "Bayern Munich", "Dortmund"},
	{"match4", "PSG", "Marseille"},
	{"match5", "Juventus", "AC Milan"},
	{"match6", "Ajax", "Feyenoord"},
}

var homePlayers = map[string][]string{
	"Barcelona":     {"Yamal", "Pedri", "Lewandowski", "Gavi", "de Jong"},
	"Liverpool":     {"Salah", "Nunez", "Diaz", "Szoboszlai", "Mac Allister"},
	"Bayern Munich": {"Musiala", "Kane", "Sane", "Kimmich", "Muller"},
	"PSG":           {"Dembele", "Barcola", "Asensio", "Vitinha", "Zaïre-Emery"},
	"Juventus":      {"Vlahovic", "Yildiz", "Chiesa", "Locatelli", "Rabiot"},
	"Ajax":          {"Bergwijn", "Brobbey", "Taylor", "Berghuis", "Blind"},
}

var awayPlayers = map[string][]string{
	"Real Madrid": {"Vinicius", "Bellingham", "Mbappe", "Rodrygo", "Valverde"},
	"Man City":    {"Haaland", "De Bruyne", "Foden", "Grealish", "Silva"},
	"Dortmund":    {"Adeyemi", "Brandt", "Reus", "Sabitzer", "Malen"},
	"Marseille":   {"Aubameyang", "Sanchez", "Harit", "Guendouzi", "Ounahi"},
	"AC Milan":    {"Leao", "Pulisic", "Giroud", "Reijnders", "Theo"},
	"Feyenoord":   {"Gimenez", "Stengs", "Timber", "Kokcu", "Dilrosun"},
}

func newMatch(id, home, away string) *matchSim { _ = "STUB: not implemented"; return nil }

func (m *matchSim) reset() { _ = "STUB: not implemented"; return }

func (m *matchSim) randomPlayer(team string) string { _ = "STUB: not implemented"; return "" }

func (m *matchSim) addEvent(evt MatchEvent) { _ = "STUB: not implemented"; return }

// Keep only last 8 events.

// tick advances the match by ~2 minutes of match time.
func (m *matchSim) tick() { _ = "STUB: not implemented"; return }

// Transition to half-time or full-time.

// Pick which team gets action this tick.

// Possession drift: small random walk.

// Passes (always increment both sides).

// Shots (~20% chance per tick).

// Goal (~3% chance per tick — roughly 2-3 goals per match).

// Corner (~8% chance).

// Foul (~10% chance, fouling team is the opponent).

// Yellow card on ~40% of fouls.

// Red card (~0.5% chance — rare.

func publishScoreboardData(ctx context.Context, node *centrifuge.Node) {
	_ = "STUB: not implemented"
	return
}

// Stagger initial minutes so matches aren't all in sync.

// Track pause timers for HT/FT per match.

// Handle paused states.

// Resume into second half after 5s pause.

// Restart match after 10s pause.
