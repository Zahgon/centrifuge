package centrifuge

import (
	"time"

	"github.com/centrifugal/protocol"
)

// PublishOption is a type to represent various Publish options.
type PublishOption func(*PublishOptions)

// WithHistory tells Broker to save message to history stream with provided size and ttl.
func WithHistory(size int, ttl time.Duration, metaTTL ...time.Duration) PublishOption {
	_ = "STUB: not implemented"
	return *new(PublishOption)
}

// WithIdempotencyKey tells Broker the idempotency key for the publication.
// See PublishOptions.IdempotencyKey.
func WithIdempotencyKey(key string) PublishOption {
	_ = "STUB: not implemented"
	return *new(PublishOption)
}

// WithKey sets a key for the publication. When set, the publication is associated
// with a specific key within the channel. This may enable per-key debouncing or
// channel level per-key batching. The key is delivered to subscribers in the Publication.
func WithKey(key string) PublishOption { _ = "STUB: not implemented"; return *new(PublishOption) }

// WithDelta tells Broker to use delta streaming.
func WithDelta(enabled bool) PublishOption { _ = "STUB: not implemented"; return *new(PublishOption) }

// WithIdempotentResultTTL sets the time of expiration for results of idempotent publications.
// See PublishOptions.IdempotentResultTTL for more description and defaults.
func WithIdempotentResultTTL(ttl time.Duration) PublishOption {
	_ = "STUB: not implemented"
	return *new(PublishOption)
}

// WithClientInfo adds ClientInfo to Publication.
func WithClientInfo(info *ClientInfo) PublishOption {
	_ = "STUB: not implemented"
	return *new(PublishOption)
}

// WithTags allows setting Publication.Tags.
func WithTags(tags map[string]string) PublishOption {
	_ = "STUB: not implemented"
	return *new(PublishOption)
}

// WithVersion allows application to provide a tip for Centrifuge about
// internal application version of Publication. This is helpful to drop
// non-actual publications on Centrifuge Broker level. Publications may be
// non-actual in case of unordered publish calls from application side.
// In some cases application may want Centrifuge to avoid delivery of these
// publications.
// This is mostly useful for scenarios when channel publications contain the
// entire state, so skipping intermediary messages is safe and beneficial.
// This also means that Centrifuge history will contain the most recent Publication,
// so the recovery will return the proper state (and state should be eventually
// consistent in case of at least one delivery).
// This option must be used only with channels that have history enabled. Note,
// these version and versionEpoch are not used as Centrifuge channel stream position.
// Centrifuge still generates its own independent StreamPosition for each Publication
// in channel streams with history, but it additionally starts keeping version and
// versionEpoch provided here.
// If versionEpoch is an empty string, then Centrifuge does not look at it when comparing
// versions.
func WithVersion(version uint64, versionEpoch string) PublishOption {
	_ = "STUB: not implemented"
	return *new(PublishOption)
}

// SubscriptionType defines the type of subscription.
type SubscriptionType int32

const (
	// SubscriptionTypeStream is a regular PUB/SUB subscription (default).
	SubscriptionTypeStream SubscriptionType = 0
	// SubscriptionTypeMap is a map subscription with keyed state.
	SubscriptionTypeMap SubscriptionType = 1
	// SubscriptionTypeMapClients is a client presence subscription on a map channel.
	SubscriptionTypeMapClients SubscriptionType = 2
	// SubscriptionTypeMapUsers is a user presence subscription on a map channel.
	SubscriptionTypeMapUsers SubscriptionType = 3
	// SubscriptionTypeSharedPoll is a shared poll subscription.
	SubscriptionTypeSharedPoll SubscriptionType = 4
)

// IsMapPresence reports whether t is a map presence subscription type
// (SubscriptionTypeMapClients or SubscriptionTypeMapUsers).
func (t SubscriptionType) IsMapPresence() bool { _ = "STUB: not implemented"; return false }

func (t SubscriptionType) String() string { _ = "STUB: not implemented"; return "" }

// FilterNode is a filter expression tree for matching against key-value tags.
// Used for server-side publication filtering (ServerTagsFilter in SubscribeOptions).
type FilterNode = protocol.FilterNode

// SubscribeOptions define per-subscription options.
type SubscribeOptions struct {
	// clientID to subscribe.
	clientID string
	// sessionID to subscribe.
	sessionID string

	// ExpireAt defines time in future when subscription should expire,
	// zero value means no expiration.
	ExpireAt int64
	// ChannelInfo defines custom channel information, zero value means no channel information.
	ChannelInfo []byte
	// EmitPresence turns on participating in channel presence - i.e. client
	// subscription will emit presence updates to PresenceManager and will be visible
	// in a channel presence result.
	EmitPresence bool
	// EmitJoinLeave turns on emitting Join and Leave events from the subscribing client.
	// See also PushJoinLeave if you want current client to receive join/leave messages.
	EmitJoinLeave bool
	// PushJoinLeave turns on receiving channel Join and Leave events by the client.
	// Subscriptions which emit join/leave events should have EmitJoinLeave on.
	PushJoinLeave bool
	// When position is on client will additionally sync its position inside a stream
	// to prevent publication loss. The loss can happen due to at most once guarantees
	// of PUB/SUB model. Make sure you are enabling EnablePositioning in channels that
	// maintain Publication history stream. When EnablePositioning is on Centrifuge will
	// include StreamPosition information to subscribe response - for a client to be
	// able to manually track its position inside a stream.
	EnablePositioning bool
	// EnableRecovery turns on automatic recovery for a channel. In this case
	// client will try to recover missed messages upon resubscribe to a channel
	// after reconnect to a server. This option also enables client position
	// tracking inside a stream (i.e. enabling EnableRecovery will automatically
	// enable EnablePositioning option) to prevent occasional publication loss.
	// Make sure you are using EnableRecovery in channels that maintain Publication
	// history stream.
	EnableRecovery bool
	// RecoveryMode is by default RecoveryModeStream, but can be also RecoveryModeCache.
	RecoveryMode RecoveryMode
	// Data to send to a client with Subscribe Push.
	Data []byte
	// RecoverSince will try to subscribe a client and recover from a certain StreamPosition.
	RecoverSince *StreamPosition

	// HistoryMetaTTL allows to override default (set in Config.HistoryMetaTTL) history
	// meta information expiration time.
	HistoryMetaTTL time.Duration

	// AllowedDeltaTypes is a whitelist of DeltaType subscribers can negotiate. At this point Centrifuge
	// only supports DeltaTypeFossil. If zero value – clients won't be able to negotiate delta encoding
	// within a channel and will receive full data in publications.
	// Delta encoding is an EXPERIMENTAL feature and may be changed.
	AllowedDeltaTypes []DeltaType
	// Source is a way to mark the source of Subscription - i.e. where it comes from. May be useful
	// for inspection of a connection during its lifetime.
	Source uint8
	// AllowChannelCompaction if true allows client to negotiate channel ID compaction –
	// Centrifuge will replace channel names with shorter IDs in subscription pushes.
	// If disabled, clients receive the full channel name in all pushes. Requires support
	// in client SDK.
	AllowChannelCompaction bool
	// AllowTagsFilter if set to true allows client to use publication filter by tags. If not allowed
	// and client provided a filter – the BadRequest error will be returned.
	// Important note here, since channel permissions are managed on channel level, tags filtering
	// must be used as a bandwidth optimization, not an access control mechanism.
	AllowTagsFilter bool
	// ServerTagsFilter is a server-controlled tags filter applied to publications before delivery.
	// Unlike AllowTagsFilter (which enables client-side filtering), this filter is set by the server
	// (via subscribe proxy or JWT) and cannot be overridden by the client. When both server and
	// client filters are set, they are applied independently (AND semantics). ServerTagsFilter
	// can not be used together with Delta Compression in subscription.
	ServerTagsFilter *FilterNode

	// Type defines the subscription type. Use SubscriptionTypeMap for map subscriptions.
	// For regular subscriptions this can be left as zero value (SubscriptionTypeStream).
	Type SubscriptionType
	// MapClientPresenceChannel is the full channel name for client presence.
	// When set, client presence will be published to this channel on subscribe.
	// Empty string means no client presence publishing.
	MapClientPresenceChannel string
	// MapUserPresenceChannel is the full channel name for user presence.
	// When set, user presence will be published to this channel on subscribe.
	// Empty string means no user presence publishing.
	MapUserPresenceChannel string
	// MapRemoveClientOnUnsubscribe enables automatic cleanup of map state when the
	// subscription ends – the key matching current client ID will be removed.
	// This is useful for ephemeral state like cursor positions or temporary resources
	// that should not persist after the client leaves.
	MapRemoveClientOnUnsubscribe bool
	// ClientPublishDebounceInterval when > 0, included in the subscribe result
	// as publish_debounce (milliseconds). The SDK debounces client-initiated publishes
	// to this channel locally.
	ClientPublishDebounceInterval time.Duration
	// LabelFilter narrows the subscribe to a subset of the user's connections by
	// matching against Client.Labels (the full map set via ConnectReply.Labels —
	// NOT only the keys whitelisted in MetricsConfig.ClientLabels). A user with
	// multiple connections may end up partially subscribed; non-matching
	// connections are left untouched. Subscribe never removes an existing
	// subscription from a non-matching connection. Combined with clientID and
	// sessionID using AND semantics. A malformed filter causes Node.Subscribe
	// to return an error. Nil means no label filtering.
	LabelFilter *FilterNode
}

// SubscribeOption is a type to represent various Subscribe options.
type SubscribeOption func(*SubscribeOptions)

// WithExpireAt allows setting ExpireAt field.
func WithExpireAt(expireAt int64) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithChannelInfo ...
func WithChannelInfo(chanInfo []byte) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithEmitPresence ...
func WithEmitPresence(enabled bool) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithEmitJoinLeave ...
func WithEmitJoinLeave(enabled bool) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithPushJoinLeave ...
func WithPushJoinLeave(enabled bool) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithPositioning ...
func WithPositioning(enabled bool) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithRecovery ...
func WithRecovery(enabled bool) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

type RecoveryMode uint8

const (
	RecoveryModeStream RecoveryMode = 0
	RecoveryModeCache  RecoveryMode = 1
)

// WithRecoveryMode ...
func WithRecoveryMode(mode RecoveryMode) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithSubscribeClient allows setting client ID that should be subscribed.
// This option not used when Client.Subscribe called.
func WithSubscribeClient(clientID string) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithSubscribeSession allows setting session ID that should be subscribed.
// This option not used when Client.Subscribe called.
func WithSubscribeSession(sessionID string) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithSubscribeData allows setting custom data to send with subscribe push.
func WithSubscribeData(data []byte) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithRecoverSince allows setting SubscribeOptions.RecoverFrom.
func WithRecoverSince(since *StreamPosition) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithSubscribeSource allows setting SubscribeOptions.Source.
func WithSubscribeSource(source uint8) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithSubscribeHistoryMetaTTL allows setting SubscribeOptions.HistoryMetaTTL.
func WithSubscribeHistoryMetaTTL(metaTTL time.Duration) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// WithSubscribeLabelFilter restricts the subscribe to connections whose
// Client.Labels match the filter. See SubscribeOptions.LabelFilter for the
// detailed contract.
func WithSubscribeLabelFilter(f *FilterNode) SubscribeOption {
	_ = "STUB: not implemented"
	return *new(SubscribeOption)
}

// RefreshOptions ...
type RefreshOptions struct {
	// Expired can close connection with expired reason.
	Expired bool
	// ExpireAt defines time in future when subscription should expire,
	// zero value means no expiration.
	ExpireAt int64
	// Info defines custom channel information, zero value means no channel information.
	Info []byte
	// clientID to refresh.
	clientID string
	// sessionID to refresh.
	sessionID string
	// LabelFilter narrows the refresh to a subset of the user's connections by
	// matching against Client.Labels (the full map set via ConnectReply.Labels —
	// NOT only the keys whitelisted in MetricsConfig.ClientLabels). A user with
	// multiple connections may end up partially refreshed. Combined with clientID
	// and sessionID using AND semantics. A malformed filter causes Node.Refresh
	// to return an error. Nil means no label filtering.
	LabelFilter *FilterNode
}

// RefreshOption is a type to represent various Refresh options.
type RefreshOption func(options *RefreshOptions)

// WithRefreshClient to limit refresh only for specified client ID.
func WithRefreshClient(clientID string) RefreshOption {
	_ = "STUB: not implemented"
	return *new(RefreshOption)
}

// WithRefreshSession to limit refresh only for specified session ID.
func WithRefreshSession(sessionID string) RefreshOption {
	_ = "STUB: not implemented"
	return *new(RefreshOption)
}

// WithRefreshExpired to set expired flag - connection will be closed with DisconnectExpired.
func WithRefreshExpired(expired bool) RefreshOption {
	_ = "STUB: not implemented"
	return *new(RefreshOption)
}

// WithRefreshExpireAt to set unix seconds in the future when connection should expire.
// Zero value means no expiration.
func WithRefreshExpireAt(expireAt int64) RefreshOption {
	_ = "STUB: not implemented"
	return *new(RefreshOption)
}

// WithRefreshInfo to override connection info.
func WithRefreshInfo(info []byte) RefreshOption {
	_ = "STUB: not implemented"
	return *new(RefreshOption)
}

// WithRefreshLabelFilter restricts the refresh to connections whose
// Client.Labels match the filter. See RefreshOptions.LabelFilter for the
// detailed contract.
func WithRefreshLabelFilter(f *FilterNode) RefreshOption {
	_ = "STUB: not implemented"
	return *new(RefreshOption)
}

// UnsubscribeOptions ...
type UnsubscribeOptions struct {
	// clientID to unsubscribe.
	clientID string
	// sessionID to unsubscribe.
	sessionID string
	// custom unsubscribe object.
	unsubscribe *Unsubscribe
	// LabelFilter narrows the unsubscribe to a subset of the user's connections
	// by matching against Client.Labels (the full map set via ConnectReply.Labels
	// — NOT only the keys whitelisted in MetricsConfig.ClientLabels). When the
	// channel argument is empty (unsubscribe from all channels), the filter still
	// applies and narrows which connections are affected. Combined with clientID
	// and sessionID using AND semantics. A malformed filter causes
	// Node.Unsubscribe to return an error. Nil means no label filtering.
	LabelFilter *FilterNode
}

// UnsubscribeOption is a type to represent various Unsubscribe options.
type UnsubscribeOption func(options *UnsubscribeOptions)

// WithUnsubscribeClient allows setting client ID that should be unsubscribed.
// This option not used when Client.Unsubscribe called.
func WithUnsubscribeClient(clientID string) UnsubscribeOption {
	_ = "STUB: not implemented"
	return *new(UnsubscribeOption)
}

// WithUnsubscribeSession allows setting session ID that should be unsubscribed.
// This option not used when Client.Unsubscribe called.
func WithUnsubscribeSession(sessionID string) UnsubscribeOption {
	_ = "STUB: not implemented"
	return *new(UnsubscribeOption)
}

// WithCustomUnsubscribe allows setting custom Unsubscribe.
func WithCustomUnsubscribe(unsubscribe Unsubscribe) UnsubscribeOption {
	_ = "STUB: not implemented"
	return *new(UnsubscribeOption)
}

// WithUnsubscribeLabelFilter restricts the unsubscribe to connections whose
// Client.Labels match the filter. See UnsubscribeOptions.LabelFilter for the
// detailed contract.
func WithUnsubscribeLabelFilter(f *FilterNode) UnsubscribeOption {
	_ = "STUB: not implemented"
	return *new(UnsubscribeOption)
}

// DisconnectOptions define some fields to alter behaviour of Disconnect operation.
type DisconnectOptions struct {
	// Disconnect represents custom disconnect to use.
	// By default, DisconnectForceNoReconnect will be used.
	Disconnect *Disconnect
	// ClientWhitelist contains client IDs to keep.
	ClientWhitelist []string
	// clientID to disconnect.
	clientID string
	// sessionID to disconnect.
	sessionID string
	// LabelFilter narrows the disconnect to a subset of the user's connections by
	// matching against Client.Labels (the full map set via ConnectReply.Labels —
	// NOT only the keys whitelisted in MetricsConfig.ClientLabels). Combined with
	// ClientWhitelist, clientID and sessionID using AND semantics — a connection
	// must clear every set check to be disconnected. A malformed filter causes
	// Node.Disconnect to return an error. Nil means no label filtering.
	LabelFilter *FilterNode
}

// DisconnectOption is a type to represent various Disconnect options.
type DisconnectOption func(options *DisconnectOptions)

// WithCustomDisconnect allows setting custom Disconnect.
func WithCustomDisconnect(disconnect Disconnect) DisconnectOption {
	_ = "STUB: not implemented"
	return *new(DisconnectOption)
}

// WithDisconnectClient allows setting Client.
func WithDisconnectClient(clientID string) DisconnectOption {
	_ = "STUB: not implemented"
	return *new(DisconnectOption)
}

// WithDisconnectSession allows setting session ID to disconnect.
func WithDisconnectSession(sessionID string) DisconnectOption {
	_ = "STUB: not implemented"
	return *new(DisconnectOption)
}

// WithDisconnectClientWhitelist allows setting ClientWhitelist.
func WithDisconnectClientWhitelist(whitelist []string) DisconnectOption {
	_ = "STUB: not implemented"
	return *new(DisconnectOption)
}

// WithDisconnectLabelFilter restricts the disconnect to connections whose
// Client.Labels match the filter. See DisconnectOptions.LabelFilter for the
// detailed contract.
func WithDisconnectLabelFilter(f *FilterNode) DisconnectOption {
	_ = "STUB: not implemented"
	return *new(DisconnectOption)
}

// HistoryOption is a type to represent various History options.
type HistoryOption func(options *HistoryOptions)

// NoLimit defines that limit should not be applied.
const NoLimit = -1

// WithLimit allows setting HistoryOptions.Limit.
func WithLimit(limit int) HistoryOption { _ = "STUB: not implemented"; return *new(HistoryOption) }

// WithSince allows setting HistoryOptions.Since option.
func WithSince(sp *StreamPosition) HistoryOption {
	_ = "STUB: not implemented"
	return *new(HistoryOption)
}

// WithReverse allows setting HistoryOptions.Reverse option.
func WithReverse(reverse bool) HistoryOption { _ = "STUB: not implemented"; return *new(HistoryOption) }

func WithHistoryFilter(filter HistoryFilter) HistoryOption {
	_ = "STUB: not implemented"
	return *new(HistoryOption)
}

func WithHistoryMetaTTL(metaTTL time.Duration) HistoryOption {
	_ = "STUB: not implemented"
	return *new(HistoryOption)
}
