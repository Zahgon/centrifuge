package main

import (
	"log"
	"net/http"
	"os"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// SessionName is the key used to access the session store.
const SessionName = "_session"

// Store can/should be set by applications. The default is a cookie store.
var Store sessions.Store

func init() {
	if os.Getenv("SESSION_SECRET") == "" {
		log.Fatal("SESSION_SECRET environment variable required")
	}
	if os.Getenv("GOOGLE_CLIENT_ID") == "" {
		log.Fatal("GOOGLE_CLIENT_ID environment variable required")
	}
	if os.Getenv("GOOGLE_CLIENT_SECRET") == "" {
		log.Fatal("GOOGLE_CLIENT_SECRET environment variable required")
	}
	key := []byte(os.Getenv("SESSION_SECRET"))
	cookieStore := sessions.NewCookieStore(key)
	cookieStore.Options.HttpOnly = true
	cookieStore.Options.MaxAge = 3600
	Store = cookieStore
}

var googleOauthConfig = &oauth2.Config{
	RedirectURL:  "http://localhost:3000/callback",
	ClientID:     os.Getenv("GOOGLE_CLIENT_ID"),
	ClientSecret: os.Getenv("GOOGLE_CLIENT_SECRET"),
	Scopes: []string{
		"https://www.googleapis.com/auth/userinfo.profile",
		"https://www.googleapis.com/auth/userinfo.email"},
	Endpoint: google.Endpoint,
}

// GoogleUser ...
type GoogleUser struct {
	ID            string `json:"id"`
	Email         string `json:"email"`
	VerifiedEmail bool   `json:"verified_email"`
	Name          string `json:"name"`
	GivenName     string `json:"given_name"`
	FamilyName    string `json:"family_name"`
	Link          string `json:"link"`
	Picture       string `json:"picture"`
	Gender        string `json:"gender"`
	Locale        string `json:"locale"`
}

func authMiddleware(h http.Handler) http.Handler {
	_ = "STUB: not implemented"
	return *new(http.Handler)
}

func createCentrifugeNode() (*centrifuge.Node, error) { _ = "STUB: not implemented"; return nil, nil }

func main() {
	node, err := createCentrifugeNode()
	if err != nil {
		log.Fatal(err)
	}

	router := mux.NewRouter().StrictSlash(true)

	// Chat handlers.
	router.Handle("/", authMiddleware(http.FileServer(http.Dir("./"))))
	router.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))

	// Auth handlers.
	router.HandleFunc("/account", accountHandler)
	router.HandleFunc("/login", loginHandler)
	router.HandleFunc("/logout", logoutHandler)
	router.HandleFunc("/callback", callbackHandler)

	if err := http.ListenAndServe(":3000", router); err != nil {
		log.Fatalln(err)
	}
}

func accountHandler(w http.ResponseWriter, r *http.Request) { _ = "STUB: not implemented"; return }

func loginHandler(w http.ResponseWriter, r *http.Request) { _ = "STUB: not implemented"; return }

func logoutHandler(w http.ResponseWriter, r *http.Request) { _ = "STUB: not implemented"; return }

func callbackHandler(w http.ResponseWriter, r *http.Request) { _ = "STUB: not implemented"; return }

// StoreInSession stores a specified key/value pair in the session.
func StoreInSession(key string, value string, req *http.Request, res http.ResponseWriter) error {
	_ = "STUB: not implemented"
	return nil
}

// GetFromSession retrieves a previously-stored value from the session.
// If no value has previously been stored at the specified key, it will return an error.
func GetFromSession(key string, req *http.Request) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func getSessionValue(session *sessions.Session, key string) (string, error) {
	_ = "STUB: not implemented"
	return "", nil
}

func updateSessionValue(session *sessions.Session, key, value string) error {
	_ = "STUB: not implemented"
	return nil
}

// Logout invalidates a user session.
func Logout(res http.ResponseWriter, req *http.Request) error {
	_ = "STUB: not implemented"
	return nil
}
