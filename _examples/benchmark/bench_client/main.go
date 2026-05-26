// This code is an adapted version of Nats benchmarking suite from
// https://github.com/nats-io/go-nats/blob/master/examples/nats-bench.go
// for Centrifuge. Use together with benchmark program from Centrifuge repo:
// https://github.com/centrifugal/centrifuge/blob/master/examples/benchmark/main.go
package main

import (
	"flag"
	"log"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge-go"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Some sane defaults.
const (
	DefaultNumMsg      = 100000
	DefaultNumPubs     = 1
	DefaultNumSubs     = 0
	DefaultMessageSize = 128
	DefaultDeadline    = 300
)

var messageReceivedCounter int32 = 0
var messagePublishedCounter int32 = 0

func usage() { _ = "STUB: not implemented"; return }

var url = flag.String("s", "ws://localhost:8000/connection/websocket", "Connection URI")
var useProtobuf = flag.Bool("p", false, "Use protobuf format (by default JSON is used)")
var numPubs = flag.Int("np", DefaultNumPubs, "Number of concurrent publishers")
var numSubs = flag.Int("ns", DefaultNumSubs, "Number of concurrent subscribers")
var numMsg = flag.Int("n", DefaultNumMsg, "Number of messages to publish")
var msgSize = flag.Int("ms", DefaultMessageSize, "Size of the message")
var deadline = flag.Int("d", DefaultDeadline, "Deadline for the test to finish")
var pubRateLimit = flag.Int("pl", 0, "Rate limit for each publisher in messages per second")
var mode = flag.String("m", "pubsub", "Benchmark mode: pubsub (default), idle, connect, rpc")
var connectRate = flag.Int("cr", 100, "Target connection rate per second (for connect mode)")
var waitTime = flag.Int("w", 60, "Wait time in seconds (for idle mode)")
var rpcRate = flag.Int("rr", 100, "RPC calls per second per connection (for rpc mode)")

var benchmark *Benchmark

func main() {
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()

	switch *mode {
	case "pubsub":
		runPubSubMode(args)
	case "idle":
		runIdleMode(args)
	case "connect":
		runConnectRateMode(args)
	case "rpc":
		runRPCMode(args)
	default:
		log.Fatalf("Unknown mode: %s. Available modes: pubsub, idle, connect, rpc", *mode)
	}
}

func runPubSubMode(args []string) { _ = "STUB: not implemented"; return }

// No fixed duration for pubsub

// Total expected messages

// Run Subscribers first

// Now Publishers

type errorCollector struct {
	errors []string
	mu     sync.Mutex
}

func (ec *errorCollector) add(err string) { _ = "STUB: not implemented"; return }

func (ec *errorCollector) hasErrors() bool { _ = "STUB: not implemented"; return false }

func (ec *errorCollector) display() { _ = "STUB: not implemented"; return }

var globalErrorCollector = &errorCollector{}

func newConnection(connType string) *centrifuge.Client { _ = "STUB: not implemented"; return nil }

func runPublisher(startWg, doneWg *sync.WaitGroup, numMsg int, msgSize int) {
	_ = "STUB: not implemented"
	return
}

type subEventHandler struct {
	numMsg    int
	msgSize   int
	received  int
	connected bool
	doneWg    *sync.WaitGroup
	startWg   *sync.WaitGroup
	client    *centrifuge.Client
	start     time.Time
}

func (h *subEventHandler) OnPublication(_ centrifuge.PublicationEvent) {
	_ = "STUB: not implemented"
	return
}

func (h *subEventHandler) OnSubscribe(_ centrifuge.SubscribedEvent) {
	_ = "STUB: not implemented"
	// call Done() only for the first time and not on reconnects to avoid a panic
	return
}

func (h *subEventHandler) OnError(e centrifuge.SubscriptionErrorEvent) {
	_ = "STUB: not implemented"
	return
}

func runSubscriber(startWg, doneWg *sync.WaitGroup, numMsg int, msgSize int) {
	_ = "STUB: not implemented"
	return
}

func runIdleMode(_ []string) { _ = "STUB: not implemented"; return }

func runConnectRateMode(_ []string) { _ = "STUB: not implemented"; return }

// Thread-safe map to store latencies

// Start the UI

// Goroutine to collect results as they come in

// Goroutine to spawn connections at target rate

// Collect latencies into a slice for stats

func displayConnectRateStats(successCount, failedCount int, totalDuration time.Duration, actualRate float64, latencies []time.Duration) {
	_ = "STUB: not implemented"
	return
}

func runRPCMode(_ []string) { _ = "STUB: not implemented"; return }

// Thread-safe map to store latencies
// Determine if we're using count-based or duration-based mode

// Check if we've reached the target count

// Store latency (key is just a counter)

// Wait until we reach the target

// Wait for duration

// Display errors and final stats

func displayRPCStats(startTime time.Time, rpcSentCount, rpcSuccessCount, rpcErrorCount *int64, latencies *sync.Map) {
	_ = "STUB: not implemented"
	return
}

// Collect latencies into a slice

// A Sample for a particular client
type Sample struct {
	JobMsgCnt int
	MsgCnt    uint64
	MsgBytes  uint64
	IOBytes   uint64
	Start     time.Time
	End       time.Time
}

// SampleGroup for a number of samples, the group is a Sample itself agregating the values the Samples
type SampleGroup struct {
	Sample
	Samples []*Sample
}

// Benchmark to hold the various Samples organized by publishers and subscribers
type Benchmark struct {
	Sample
	Name       string
	RunID      string
	Pubs       *SampleGroup
	Subs       *SampleGroup
	subChannel chan *Sample
	pubChannel chan *Sample
}

// NewBenchmark initializes a Benchmark. After creating a bench call AddSubSample/AddPubSample.
// When done collecting samples, call EndBenchmark
func NewBenchmark(name string, subCnt, pubCnt int) *Benchmark {
	_ = "STUB: not implemented"
	return nil
}

// Close organizes collected Samples and calculates aggregates. After Close(), no more samples can be added.
func (bm *Benchmark) Close() { _ = "STUB: not implemented"; return }

// AddSubSample to the benchmark
func (bm *Benchmark) AddSubSample(s *Sample) {
	_ = "STUB: not implemented"

	// AddPubSample to the benchmark
	return
}

func (bm *Benchmark) AddPubSample(s *Sample) {
	_ = "STUB: not implemented"

	// NewSample creates a new Sample initialized to the provided values.
	return
}

func NewSample(jobCount int, msgSize int, start, end time.Time) *Sample {
	_ = "STUB: not implemented"
	return nil
}

// Throughput of bytes per second.
func (s *Sample) Throughput() float64 { _ = "STUB: not implemented"; return 0 }

// Rate of messages in the job per second.
func (s *Sample) Rate() int64 { _ = "STUB: not implemented"; return 0 }

func (s *Sample) String() string { _ = "STUB: not implemented"; return "" }

// Duration that the sample was active.
func (s *Sample) Duration() time.Duration {
	_ = "STUB: not implemented"
	return *

	// Seconds that the sample or samples were active.
	new(time.Duration)
}

func (s *Sample) Seconds() float64 { _ = "STUB: not implemented"; return 0 }

// NewSampleGroup initializer.
func NewSampleGroup() *SampleGroup { _ = "STUB: not implemented"; return nil }

// Statistics information of the sample group (min, average, max and standard deviation).
func (sg *SampleGroup) Statistics() string { _ = "STUB: not implemented"; return "" }

// MinRate returns the smallest message rate in the SampleGroup.
func (sg *SampleGroup) MinRate() int64 { _ = "STUB: not implemented"; return 0 }

// MaxRate returns the largest message rate in the SampleGroup.
func (sg *SampleGroup) MaxRate() int64 { _ = "STUB: not implemented"; return 0 }

// AvgRate returns the average of all the message rates in the SampleGroup.
func (sg *SampleGroup) AvgRate() int64 { _ = "STUB: not implemented"; return 0 }

// StdDev returns the standard deviation the message rates in the SampleGroup.
func (sg *SampleGroup) StdDev() float64 { _ = "STUB: not implemented"; return 0 }

// AddSample adds a Sample to the SampleGroup. After adding a Sample it shouldn't be modified.
func (sg *SampleGroup) AddSample(e *Sample) { _ = "STUB: not implemented"; return }

// HasSamples returns true if the group has samples.
func (sg *SampleGroup) HasSamples() bool { _ = "STUB: not implemented"; return false }

// Report returns a human readable report of the samples taken in the Benchmark.
func (bm *Benchmark) Report() string { _ = "STUB: not implemented"; return "" }

func commaFormat(n int64) string { _ = "STUB: not implemented"; return "" }

// HumanBytes formats bytes as a human readable string
func HumanBytes(bytes float64, si bool) string { _ = "STUB: not implemented"; return "" }

// MsgPerClient divides the number of messages by the number of clients and tries
// to distribute them as evenly as possible
func MsgPerClient(numMsg, numClients int) []int { _ = "STUB: not implemented"; return nil }

// In case of network reconnects, the subscriber clients can lose messages and might never finish waiting for the messages.
// In such a scenario, even a single subscriber client can block the test from finishing. so use the configured deadline to finish waiting and get the accumulated report.
func waitForTestCompletion(wg *sync.WaitGroup, deadlineSeconds *int) bool {
	_ = "STUB: not implemented"
	return false
}

// completed normally

// wait timed out

var (
	titleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#7D56F4"))

	headerStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#7D56F4"))

	labelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#888888"))

	valueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575"))

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF4444"))

	boxStyle = lipgloss.NewStyle().
			Padding(0, 0)
)

type tickMsg time.Time

type UIModel struct {
	mode           string
	startTime      time.Time
	duration       time.Duration
	stats          map[string]interface{}
	quitting       bool
	width          int
	height         int
	errorCollector *errorCollector
	lastTickTime   time.Time
	lastPubCount   int64
	lastSubCount   int64
	currentPubRate float64
	currentSubRate float64
}

func NewUIModel(mode string, duration time.Duration, errorCollector *errorCollector) UIModel {
	_ = "STUB: not implemented"
	return *new(UIModel)
}

func (m UIModel) Init() tea.Cmd { _ = "STUB: not implemented"; return *new(tea.Cmd) }

func tickCmd() tea.Cmd { _ = "STUB: not implemented"; return *new(tea.Cmd) }

func (m UIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	_ = "STUB: not implemented"
	return *new(tea.Model), *new(tea.Cmd)
}

// Calculate per-second rates for pubsub mode

func (m UIModel) View() string { _ = "STUB: not implemented"; return "" }

// Title

// Time info

// Mode-specific stats

// Show error count if there are errors

func (m UIModel) renderIdleStats() string { _ = "STUB: not implemented"; return "" }

func (m UIModel) renderConnectRateStats() string { _ = "STUB: not implemented"; return "" }

func (m UIModel) renderRPCStats() string { _ = "STUB: not implemented"; return "" }

// Progress bar if we have a target

// Leave padding for box borders and margins

func (m UIModel) renderPubSubStats() string { _ = "STUB: not implemented"; return "" }

// Use the calculated per-second rates

// Progress bar for received messages

// Leave padding for box borders and margins

func renderProgressBar(percent int, width int) string { _ = "STUB: not implemented"; return "" }

func (m UIModel) getInt64Stat(key string) int64 { _ = "STUB: not implemented"; return 0 }

func formatInt(n int64) string { _ = "STUB: not implemented"; return "" }
