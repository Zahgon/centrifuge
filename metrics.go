package centrifuge

import (
	"sync"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/maypok86/otter/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// default namespace for prometheus metrics. Can be changed over Config.
var defaultMetricsNamespace = "centrifuge"

var registryMu sync.RWMutex

// clientMetricDef defines a Prometheus metric with its subsystem and name.
type clientMetricDef struct {
	Subsystem string
	Name      string
}

// Client metric definitions.
var (
	metricClientConnectionsAccepted = clientMetricDef{
		Subsystem: "client",
		Name:      "connections_accepted",
	}
	metricClientConnectionsInflight = clientMetricDef{
		Subsystem: "client",
		Name:      "connections_inflight",
	}
	metricClientSubscriptionsAccepted = clientMetricDef{
		Subsystem: "client",
		Name:      "subscriptions_accepted",
	}
	metricClientSubscriptionsInflight = clientMetricDef{
		Subsystem: "client",
		Name:      "subscriptions_inflight",
	}
	metricClientCommandDuration = clientMetricDef{
		Subsystem: "client",
		Name:      "command_duration_seconds",
	}
	metricClientNumReplyErrors = clientMetricDef{
		Subsystem: "client",
		Name:      "num_reply_errors",
	}
	metricClientNumServerUnsubscribes = clientMetricDef{
		Subsystem: "client",
		Name:      "num_server_unsubscribes",
	}
	metricClientNumServerDisconnects = clientMetricDef{
		Subsystem: "client",
		Name:      "num_server_disconnects",
	}
	metricTransportMessagesSent = clientMetricDef{
		Subsystem: "transport",
		Name:      "messages_sent",
	}
	metricTransportMessagesSentSize = clientMetricDef{
		Subsystem: "transport",
		Name:      "messages_sent_size",
	}
	metricTransportMessagesReceived = clientMetricDef{
		Subsystem: "transport",
		Name:      "messages_received",
	}
	metricTransportMessagesReceivedSize = clientMetricDef{
		Subsystem: "transport",
		Name:      "messages_received_size",
	}
)

type metrics struct {
	messagesSentCount      *prometheus.CounterVec
	messagesReceivedCount  *prometheus.CounterVec
	actionCount            *prometheus.CounterVec
	buildInfoGauge         *prometheus.GaugeVec
	numClientsGauge        prometheus.Gauge
	numUsersGauge          prometheus.Gauge
	numSubsGauge           prometheus.Gauge
	numChannelsGauge       prometheus.Gauge
	numNodesGauge          prometheus.Gauge
	replyErrorCount        *prometheus.CounterVec
	connectionsAccepted    *prometheus.CounterVec
	connectionsInflight    *prometheus.GaugeVec
	subscriptionsAccepted  *prometheus.CounterVec
	subscriptionsInflight  *prometheus.GaugeVec
	serverUnsubscribeCount *prometheus.CounterVec
	serverDisconnectCount  *prometheus.CounterVec
	// commandDurationSummary holds the legacy Summary by default; when
	// EnableNativeHistograms is true it is a no-op (the Summary is not
	// exposed). The companion commandDurationHistogram below always carries
	// the real observations, with native schema when the flag is on.
	commandDurationSummary        prometheus.ObserverVec
	commandDurationHistogram      *prometheus.HistogramVec
	surveyDurationSummary         prometheus.ObserverVec
	surveyDurationHistogram       *prometheus.HistogramVec
	recoverCount                  *prometheus.CounterVec
	recoveredPublications         *prometheus.HistogramVec
	transportMessagesSent         *prometheus.CounterVec
	transportMessagesSentSize     *prometheus.CounterVec
	transportMessagesReceived     *prometheus.CounterVec
	transportMessagesReceivedSize *prometheus.CounterVec
	tagsFilterDroppedCount        *prometheus.CounterVec

	messagesReceivedCountPublication prometheus.Counter
	messagesReceivedCountJoin        prometheus.Counter
	messagesReceivedCountLeave       prometheus.Counter
	messagesReceivedCountControl     prometheus.Counter

	messagesSentCountPublication prometheus.Counter
	messagesSentCountJoin        prometheus.Counter
	messagesSentCountLeave       prometheus.Counter
	messagesSentCountControl     prometheus.Counter

	commandDurationConnect       prometheus.Observer
	commandDurationSubscribe     prometheus.Observer
	commandDurationUnsubscribe   prometheus.Observer
	commandDurationPublish       prometheus.Observer
	commandDurationPresence      prometheus.Observer
	commandDurationPresenceStats prometheus.Observer
	commandDurationHistory       prometheus.Observer
	commandDurationSend          prometheus.Observer
	commandDurationRPC           prometheus.Observer
	commandDurationRefresh       prometheus.Observer
	commandDurationSubRefresh    prometheus.Observer
	commandDurationUnknown       prometheus.Observer

	broadcastDurationHistogram  *prometheus.HistogramVec
	pubSubLagHistogram          *prometheus.HistogramVec
	pingPongDurationHistogram   *prometheus.HistogramVec
	mapPublishSuppressedCount   *prometheus.CounterVec
	mapBrokerCleanupLag         *prometheus.GaugeVec
	mapBrokerCleanupKeysRemoved *prometheus.CounterVec
	mapBrokerCleanupErrors      *prometheus.CounterVec

	redisBrokerPubSubErrors           *prometheus.CounterVec
	redisBrokerPubSubDroppedMessages  *prometheus.CounterVec
	redisBrokerPubSubBufferedMessages *prometheus.GaugeVec

	// Shared poll metrics.
	sharedPollCycleDurationHistogram     *prometheus.HistogramVec
	sharedPollCycleWorkDurationHistogram *prometheus.HistogramVec
	sharedPollHandlerDurationHistogram   *prometheus.HistogramVec
	sharedPollSemWaitDurationHistogram   *prometheus.HistogramVec
	sharedPollHandlerErrorCount          *prometheus.CounterVec
	sharedPollItemsCount                 *prometheus.CounterVec
	sharedPollNotifyCount                *prometheus.CounterVec
	sharedPollDroppedNotifyCount         *prometheus.CounterVec
	sharedPollPublishCount               *prometheus.CounterVec
	sharedPollNumChannelsGauge           prometheus.Gauge
	sharedPollNumKeysGauge               prometheus.Gauge

	config MetricsConfig

	transportMessagesSentCache     sync.Map
	transportMessagesReceivedCache sync.Map
	commandDurationCache           sync.Map
	replyErrorCache                sync.Map
	actionCache                    sync.Map
	recoverCache                   sync.Map
	unsubscribeCache               sync.Map
	disconnectCache                sync.Map
	messagesSentCache              sync.Map
	messagesReceivedCache          sync.Map
	tagsFilterDroppedCache         sync.Map
	mapPublishSuppressedCache      sync.Map
	pubSubLagCache                 sync.Map
	sharedPollHandlerCache         sync.Map
	sharedPollResultCache          sync.Map
	sharedPollChannelCache         sync.Map
	sharedPollPublishCache         sync.Map
	nsCache                        *otter.Cache[string, string]
	codeStrings                    map[uint32]string

	// Cache for client label combinations: maps cache key -> {labelValues, cacheKey}
	// This allows sharing pre-computed label data across all clients with the same label values
	clientLabelCombinationsCache sync.Map // map[string]*clientLabelCombination
}

// clientLabelCombination holds pre-computed label values and cache key for a unique combination
type clientLabelCombination struct {
	labelValues []string
	cacheKey    string
}

func getMetricsNamespace(config MetricsConfig) string { _ = "STUB: not implemented"; return "" }

// clientLabelPrefix is prepended to every client-label name exported as a
// Prometheus dimension. It guarantees that user-chosen names in
// MetricsConfig.ClientLabels can never collide with built-in metric labels
// like "transport", "code", "method", etc.
const clientLabelPrefix = "app_"

// buildMetricLabels creates a label slice, optionally appending client labels if enabled.
// Exported client-label names are prefixed with clientLabelPrefix.
func (m *metrics) buildMetricLabels(baseLabels []string) []string {
	_ = "STUB: not implemented"
	return nil
}

// appendClientLabels appends client label values to base labels if client labels are enabled.
// Returns a new slice with client labels appended, or the original base labels if disabled.
func (m *metrics) appendClientLabels(baseLabels []string, c *Client) []string {
	_ = "STUB: not implemented"
	return nil
}

// Append empty strings for missing client labels to match metric definition

// getOrCreateClientLabelCombinationFromLabels returns a cached combination for the given labels map.
// This is used during client connect to precompute and cache the combination.
func (m *metrics) getOrCreateClientLabelCombinationFromLabels(labels map[string]string) *clientLabelCombination {
	_ = "STUB: not implemented"
	return nil
}

// Build cache key directly from the map to check if it's already cached

// Try to load existing combination from global cache

// Not cached - now build the values slice (only done once per unique combination)

// Create new combination

// Store in global cache (even if another goroutine stored it first, we'll use theirs)

// getCachedClientLabelCombination returns the cached combination for the given client.
// The combination is pre-cached during client connect. This is used only in non-hot paths
// and for tests. Hot paths should call c.labelCombinationCached.Load() directly.
func (m *metrics) getCachedClientLabelCombination(c *Client) *clientLabelCombination {
	_ = "STUB: not implemented"
	return nil
}

// Load pre-cached combination from client (set during connect)

// Fallback: shouldn't happen in normal flow, but handle gracefully
// This can happen if metrics are recorded before client is fully connected or in tests

// extractClientLabelValues extracts client label values from a client, returning empty strings for missing labels.
// This is a helper for non-hot-path uses. For hot paths, call c.labelCombinationCached.Load() directly.
func (m *metrics) extractClientLabelValues(c *Client) []string {
	_ = "STUB: not implemented"
	return nil
}

// Try to get the cached combination first

// Fallback: client doesn't have combination cached (e.g., in tests)
// Build label values directly from client.labels map
// Note: c.labels is set once during connect and never modified, so safe to read without lock

// buildClientLabelsCacheKey builds a cache key from client label values without allocations.
// Uses strings.Builder with pre-sized buffer to minimize allocations.
func buildClientLabelsCacheKey(values []string) string { _ = "STUB: not implemented"; return "" }

// Pre-calculate size to avoid Builder growth allocations

// +1 for separator

// Use null byte as separator

// buildClientLabelsCacheKeyFromMap builds a cache key directly from a labels map.
// This avoids allocating the intermediate values slice.
func buildClientLabelsCacheKeyFromMap(labelNames []string, labelsMap map[string]string) string {
	_ = "STUB: not implemented"
	return ""
}

// Pre-calculate size to avoid Builder growth allocations
// separators

// Use null byte as separator

// dualObserver fans observations out to both a Summary and a Histogram
// observer. Used when a metric is exposed as both instrument types — the
// Summary preserves existing {quantile="..."} dashboards while the Histogram
// supplies the histogram_quantile()- and OpenTelemetry-friendly form. When
// EnableNativeHistograms is true the Summary side is a no-op, so only the
// Histogram records data.
type dualObserver struct {
	summary, histogram prometheus.Observer
}

func (d dualObserver) Observe(v float64) { _ = "STUB: not implemented"; return }

// noopObserverVec implements prometheus.ObserverVec with all no-op methods.
// Assigned to a Summary accessor when EnableNativeHistograms is true so the
// Summary side of a dual-instrument metric is not exposed and contributes no
// observation cost. Callers that cache observers via WithLabelValues do not
// need nil-checks — they get a noopObserver that silently drops Observe()
// calls.
type noopObserverVec struct{}

func (noopObserverVec) Describe(chan<- *prometheus.Desc) { _ = "STUB: not implemented"; return }
func (noopObserverVec) Collect(chan<- prometheus.Metric) { _ = "STUB: not implemented"; return }
func (noopObserverVec) WithLabelValues(...string) prometheus.Observer {
	_ = "STUB: not implemented"
	return *new(prometheus.Observer)
}
func (noopObserverVec) With(prometheus.Labels) prometheus.Observer {
	_ = "STUB: not implemented"
	return *new(prometheus.Observer)
}
func (noopObserverVec) GetMetricWith(prometheus.Labels) (prometheus.Observer, error) {
	_ = "STUB: not implemented"
	return *new(prometheus.Observer), nil
}

func (noopObserverVec) GetMetricWithLabelValues(...string) (prometheus.Observer, error) {
	_ = "STUB: not implemented"
	return *new(prometheus.Observer), nil
}

func (noopObserverVec) CurryWith(prometheus.Labels) (prometheus.ObserverVec, error) {
	_ = "STUB: not implemented"
	return *new(prometheus.ObserverVec), nil
}

func (noopObserverVec) MustCurryWith(prometheus.Labels) prometheus.ObserverVec {
	_ = "STUB: not implemented"
	return *new(prometheus.ObserverVec)
}

type noopObserver struct{}

func (noopObserver) Observe(float64) {
	_ = "STUB: not implemented"

	// nativeHistogramOpts returns opts unchanged when native is false. When true,
	// it enables Prometheus native histogram schema with no explicit buckets —
	// the metric exposes only _count, _sum, and the native histogram chunk.
	return
}

func nativeHistogramOpts(opts prometheus.HistogramOpts, native bool) prometheus.HistogramOpts {
	_ = "STUB: not implemented"
	return *new(prometheus.HistogramOpts)
}

func newMetricsRegistry(config MetricsConfig) (*metrics, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Microsecond resolution.
// Millisecond resolution.
// Second resolution.

// Microsecond resolution.
// Millisecond resolution.
// Second resolution.

// Millisecond resolution.
// Second resolution.

// Millisecond resolution.
// Second resolution.

// Millisecond resolution.
// Second resolution.

// Helper to build message labels for node-level broker message metrics
// These metrics don't support client labels as they track node-to-broker communication

// Helper to build initial label values with empty client labels if configured.
// Client labels are added as empty strings since these pre-cached observers
// are only used when no per-call ClientLabels lookup is needed.

func (m *metrics) incRedisBrokerPubSubErrors(name string, error string) {
	_ = "STUB: not implemented"
	return
}

func (m *metrics) getChannelNamespaceLabel(ch string) string { _ = "STUB: not implemented"; return "" }

type commandDurationLabels struct {
	ChannelNamespace string
	FrameType        protocol.FrameType
}

func (m *metrics) observeCommandDuration(frameType protocol.FrameType, d time.Duration, ch string, c *Client) {
	_ = "STUB: not implemented"
	return
}

func (m *metrics) observePubSubDeliveryLag(lagTimeMilli int64, ch string) {
	_ = "STUB: not implemented"
	return
}

func (m *metrics) observeBroadcastDuration(started time.Time, ch string) {
	_ = "STUB: not implemented"
	return
}

func (m *metrics) observePingPongDuration(duration time.Duration, transport string) {
	_ = "STUB: not implemented"
	return
}

func (m *metrics) setBuildInfo(version string) { _ = "STUB: not implemented"; return }

func (m *metrics) setNumClients(n float64) { _ = "STUB: not implemented"; return }

func (m *metrics) setNumUsers(n float64) { _ = "STUB: not implemented"; return }

func (m *metrics) setNumSubscriptions(n float64) { _ = "STUB: not implemented"; return }

func (m *metrics) setNumChannels(n float64) { _ = "STUB: not implemented"; return }

func (m *metrics) setNumNodes(n float64) { _ = "STUB: not implemented"; return }

type replyErrorLabels struct {
	FrameType        protocol.FrameType
	ChannelNamespace string
	Code             string
}

func (m *metrics) incReplyError(frameType protocol.FrameType, code uint32, ch string, c *Client) {
	_ = "STUB: not implemented"
	return
}

type recoverLabels struct {
	ChannelNamespace string
	Success          string
	HasPublications  string
}

func (m *metrics) incRecover(success bool, ch string, hasPublications bool) {
	_ = "STUB: not implemented"
	return
}

func (m *metrics) observeRecoveredPublications(count int, ch string) {
	_ = "STUB: not implemented"
	return
}

type transportMessageLabels struct {
	Transport        string
	ChannelNamespace string
	FrameType        string
	ClientLabels     string // Concatenated client label values for cache key
}

type transportMessagesSent struct {
	counterSent     prometheus.Counter
	counterSentSize prometheus.Counter
}

type transportMessagesReceived struct {
	counterReceived     prometheus.Counter
	counterReceivedSize prometheus.Counter
}

func (m *metrics) getTransportMessagesSentCounters(transport string, frameType string, namespace string, clientLabelValues []string, clientLabelCacheKey string) transportMessagesSent {
	_ = "STUB: not implemented"
	// Apply client labels if they are configured
	return *new(transportMessagesSent)
}

// Create empty strings for missing client labels

// Client labels not configured - don't use them even if provided

// Use pre-computed cache key - no allocation here

// Pre-allocate labelValues with exact capacity to avoid append reallocation

func (m *metrics) incTransportMessagesSent(transport string, frameType protocol.FrameType, channel string, size int, c *Client) {
	_ = "STUB: not implemented"
	return
}

// Use pre-cached combination from client (set during connect)

func (m *metrics) incTransportMessagesReceived(transport string, frameType protocol.FrameType, channel string, size int, c *Client) {
	_ = "STUB: not implemented"
	return
}

// Apply client labels if they are configured

// Client labels not configured - don't use them even if provided

// Build cache key including client labels

func (m *metrics) getCodeLabel(code uint32) string { _ = "STUB: not implemented"; return "" }

type disconnectLabels struct {
	Code string
}

func (m *metrics) incServerDisconnect(code uint32, c *Client) { _ = "STUB: not implemented"; return }

type unsubscribeLabels struct {
	Code             string
	ChannelNamespace string
}

func (m *metrics) incServerUnsubscribe(code uint32, ch string, c *Client) {
	_ = "STUB: not implemented"
	return
}

type messageSentLabels struct {
	MsgType          string
	ChannelNamespace string
}

func (m *metrics) incMessagesSent(msgType string, ch string) { _ = "STUB: not implemented"; return }

type messageReceivedLabels struct {
	MsgType          string
	ChannelNamespace string
}

func (m *metrics) incMessagesReceived(msgType string, ch string) { _ = "STUB: not implemented"; return }

type actionLabels struct {
	Action           string
	ChannelNamespace string
}

func (m *metrics) incActionCount(action string, ch string) { _ = "STUB: not implemented"; return }

func (m *metrics) observeSurveyDuration(op string, d time.Duration) {
	_ = "STUB: not implemented"
	return
}

type tagsFilterDroppedLabels struct {
	ChannelNamespace string
}

func (m *metrics) incTagsFilterDropped(ch string, count int) { _ = "STUB: not implemented"; return }

type mapPublishSuppressedLabels struct {
	Reason           string
	ChannelNamespace string
}

func (m *metrics) incMapPublishSuppressed(reason SuppressReason, ch string) {
	_ = "STUB: not implemented"
	return
}

func (m *metrics) setMapBrokerCleanupLag(name string, seconds float64) {
	_ = "STUB: not implemented"
	return
}

func (m *metrics) addMapBrokerCleanupKeysRemoved(name string, count int64) {
	_ = "STUB: not implemented"
	return
}

func (m *metrics) incMapBrokerCleanupErrors(name string) { _ = "STUB: not implemented"; return }

// Shared poll cached metric bundles and helpers.

type sharedPollTriggerNsLabels struct {
	Trigger          string
	ChannelNamespace string
}

type sharedPollHandlerCached struct {
	errorCount  prometheus.Counter
	itemsPolled prometheus.Counter
	duration    prometheus.Observer
	semWait     prometheus.Observer
}

type sharedPollResultCached struct {
	changed   prometheus.Counter
	unchanged prometheus.Counter
	removed   prometheus.Counter
}

type sharedPollChannelCached struct {
	cycleDuration      prometheus.Observer
	cycleWorkDuration  prometheus.Observer
	notifyCount        prometheus.Counter
	droppedNotifyCount prometheus.Counter
}

type sharedPollPublishCached struct {
	applied prometheus.Counter
	skipped prometheus.Counter
}

func (m *metrics) getSharedPollHandlerCached(trigger, ch string) sharedPollHandlerCached {
	_ = "STUB: not implemented"
	return *new(sharedPollHandlerCached)
}

func (m *metrics) getSharedPollResultCached(trigger, ch string) sharedPollResultCached {
	_ = "STUB: not implemented"
	return *new(sharedPollResultCached)
}

func (m *metrics) getSharedPollChannelCached(ch string) sharedPollChannelCached {
	_ = "STUB: not implemented"
	return *new(sharedPollChannelCached)
}

func (m *metrics) getSharedPollPublishCached(ch string) sharedPollPublishCached {
	_ = "STUB: not implemented"
	return *new(sharedPollPublishCached)
}

func (m *metrics) setSharedPollNumChannels(n float64) { _ = "STUB: not implemented"; return }

func (m *metrics) setSharedPollNumKeys(n float64) { _ = "STUB: not implemented"; return }

// getAcceptProtocolLabel returns the transport accept protocol label based on HTTP version.
func getAcceptProtocolLabel(protoMajor int8) string { _ = "STUB: not implemented"; return "" }
