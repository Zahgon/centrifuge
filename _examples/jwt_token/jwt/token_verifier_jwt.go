package jwt

import (
	"crypto/rsa"
	"encoding/json"
	"errors"
	"sync"

	"github.com/cristalhq/jwt/v5"
)

type ConnectToken struct {
	// UserID tells library an ID of connecting user.
	UserID string
	// ExpireAt allows setting time in future when connection must be validated.
	// Validation can be server-side or client-side using Refresh handler.
	ExpireAt int64
	// Info contains additional information about connection. It will be
	// included into Join/Leave messages, into Presence information, also
	// info becomes a part of published message if it was published from
	// client directly. In some cases having additional info can be an
	// overhead – but you are simply free to not use it.
	Info []byte
	// Channels slice contains channels to subscribe connection to on server-side.
	Channels []string
}

type SubscribeToken struct {
	// Client is a unique client ID string set to each connection on server.
	// Will be compared with actual client ID.
	Client string
	// Channel client wants to subscribe. Will be compared with channel in
	// subscribe command.
	Channel string
	// ExpireAt allows setting time in future when connection must be validated.
	// Validation can be server-side or client-side using SubRefresh handler.
	ExpireAt int64
	// Info contains additional information about connection in channel.
	// It will be included into Join/Leave messages, into Presence information,
	// also channel info becomes a part of published message if it was published
	// from subscribed client directly.
	Info []byte
	// ExpireTokenOnly used to indicate that library must only check token
	// expiration but not turn on Subscription expiration checks on server side.
	// This allows to implement one-time subscription tokens.
	ExpireTokenOnly bool
}

type TokenVerifierConfig struct {
	// HMACSecretKey is a secret key used to validate connection and subscription
	// tokens generated using HMAC. Zero value means that HMAC tokens won't be allowed.
	HMACSecretKey string
	// RSAPublicKey is a public key used to validate connection and subscription
	// tokens generated using RSA. Zero value means that RSA tokens won't be allowed.
	RSAPublicKey *rsa.PublicKey
}

func NewTokenVerifier(config TokenVerifierConfig) *TokenVerifier {
	_ = "STUB: not implemented"
	return nil
}

type TokenVerifier struct {
	mu         sync.RWMutex
	algorithms *algorithms
}

var (
	ErrTokenExpired         = errors.New("token expired")
	errUnsupportedAlgorithm = errors.New("unsupported JWT algorithm")
	errDisabledAlgorithm    = errors.New("disabled JWT algorithm")
)

type connectTokenClaims struct {
	Info       json.RawMessage `json:"info,omitempty"`
	Base64Info string          `json:"b64info,omitempty"`
	Channels   []string        `json:"channels,omitempty"`
	jwt.RegisteredClaims
}

type subscribeTokenClaims struct {
	Client          string          `json:"client,omitempty"`
	Channel         string          `json:"channel,omitempty"`
	Info            json.RawMessage `json:"info,omitempty"`
	Base64Info      string          `json:"b64info,omitempty"`
	ExpireTokenOnly bool            `json:"eto,omitempty"`
	jwt.RegisteredClaims
}

type algorithms struct {
	HS256 jwt.Verifier
	HS384 jwt.Verifier
	HS512 jwt.Verifier
	RS256 jwt.Verifier
	RS384 jwt.Verifier
	RS512 jwt.Verifier
}

func newAlgorithms(tokenHMACSecretKey string, pubKey *rsa.PublicKey) (*algorithms, error) {
	_ = "STUB: not implemented"
	return nil,

		// HMAC SHA.
		nil
}

// RSA.

func (verifier *TokenVerifier) verifySignature(token *jwt.Token) error {
	_ = "STUB: not implemented"
	return nil
}

func (verifier *TokenVerifier) VerifyConnectToken(t string) (ConnectToken, error) {
	_ = "STUB: not implemented"
	return *new(ConnectToken), nil
}

func (verifier *TokenVerifier) selectVerifier(alg jwt.Algorithm) jwt.Verifier {
	_ = "STUB: not implemented"
	return *new(jwt.Verifier)
}

func (verifier *TokenVerifier) VerifySubscribeToken(t string) (SubscribeToken, error) {
	_ = "STUB: not implemented"
	return *new(SubscribeToken), nil
}

// Decode the Info field if it's present

// If Info is not present, but Base64Info is, decode it

func (verifier *TokenVerifier) Reload(config TokenVerifierConfig) error {
	_ = "STUB: not implemented"
	return nil
}
