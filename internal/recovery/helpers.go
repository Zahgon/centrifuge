package recovery

import (
	"github.com/centrifugal/protocol"
)

// uniqueNonFilteredPublications returns slice of unique Publications which were not filtered.
func uniqueNonFilteredPublications(s []*protocol.Publication) ([]*protocol.Publication, uint64, []uint64) {
	_ = "STUB: not implemented"
	return nil, 0, nil
}

// Special value -1 indicates filtered publication, see in hub.go.

// MergePublications allows to merge recovered pubs with buffered pubs
// collected during extracting recovered so result is ordered and with
// duplicates removed.
func MergePublications(recoveredPubs []*protocol.Publication, bufferedPubs []*protocol.Publication) ([]*protocol.Publication, uint64, bool) {
	_ = "STUB: not implemented"
	return nil, 0, false
}

// All offsets from expectedOffset till pubOffset-1 must be in skippedOffsets.
// Otherwise, we have a gap in recovered publications.

// All offsets are present in skippedOffsets, can continue.
