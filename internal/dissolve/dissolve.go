package dissolve

// Dissolver allows to put function to in-memory queue and process
// it with workers until success. The order of execution is not maintained.
// Jobs will be lost after closing. Jobs not saved to persistent store so
// do not survive process restart.
// Centrifuge uses this for asynchronously unsubscribing node from channels
// in broker. As soon as process restarts all connections to broker get
// closed automatically so it's ok to lose jobs inside Dissolver queue.
type Dissolver struct {
	queue      queue
	numWorkers int
}

// New creates new Dissolver.
func New(numWorkers int) *Dissolver { _ = "STUB: not implemented"; return nil }

// Run launches workers to process Jobs from queue concurrently.
func (d *Dissolver) Run() error { _ = "STUB: not implemented"; return nil }

// Close stops processing Jobs, no more Jobs can be submitted after closing.
func (d *Dissolver) Close() error { _ = "STUB: not implemented"; return nil }

// Submit Job to be reliably processed.
func (d *Dissolver) Submit(job Job) error { _ = "STUB: not implemented"; return nil }

func (d *Dissolver) runWorker() { _ = "STUB: not implemented"; return }

// Put to the end of queue.
