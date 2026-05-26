// Command precompute regenerates ../../precomputed.go with partition tags
// for a configurable list of partition counts.
//
// Usage:
//
//	go run ./internal/redispartition/cmd/precompute            # default sizes
//	go run ./internal/redispartition/cmd/precompute -extra 8192,12288
//	go run ./internal/redispartition/cmd/precompute -sizes 16,32,64
//
// The cmd reuses tags already present in the package (via FindTags) for
// any size that is already precomputed, so re-running with the default
// list is fast and existing entries stay byte-identical. SA only runs
// for sizes that are new (i.e. not yet in the table).
//
// To add a new partition count: pass it via -extra (merged with the
// canonical defaults) and re-run. The new size is appended; existing
// entries are preserved.
//
// To regenerate from scratch: delete precomputed.go first, then re-run.
// Output is deterministic so a full regeneration produces the same data
// as the existing table for every size in the canonical list.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/centrifugal/centrifuge/internal/redispartition"
)

// defaultSizes is the canonical list shipped with the package. Add new
// entries here to make them part of every full regeneration.
var defaultSizes = []int{16, 32, 64, 128, 256, 512, 1024, 2048, 4096}

func main() {
	sizesFlag := flag.String("sizes", "", "comma-separated list of partition counts to emit (default: canonical list)")
	extraFlag := flag.String("extra", "", "comma-separated list of additional partition counts to merge with the canonical list")
	outFlag := flag.String("out", "", "output path (default: ../../precomputed.go relative to this main.go)")
	flag.Parse()

	sizes, err := resolveSizes(*sizesFlag, *extraFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	out, err := resolveOutPath(*outFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	tagsBySize := make(map[int][]string, len(sizes))
	for _, n := range sizes {
		if existing, err := redispartition.FindTags(n); err == nil {
			fmt.Fprintf(os.Stderr, "Reusing existing tags for n=%d\n", n)
			// Defensive copy: the writer below sorts/iterates, so we
			// don't want to keep an alias to the package's table.
			tagsBySize[n] = append([]string(nil), existing...)
			continue
		}
		fmt.Fprintf(os.Stderr, "Computing n=%d (this may take a while)...\n", n)
		tags, err := redispartition.ComputeTags(n)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error for n=%d: %v\n", n, err)
			os.Exit(1)
		}
		tagsBySize[n] = tags
	}

	src := renderSource(sizes, tagsBySize)
	if err := os.WriteFile(out, []byte(src), 0644); err != nil { //nolint:gosec // Generated Go source committed to repo; 0644 matches other source files.
		fmt.Fprintf(os.Stderr, "write %s: %v\n", out, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Wrote %s (%d sizes)\n", out, len(sizes))
}

func resolveSizes(sizesFlag, extraFlag string) ([]int, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func parseCSVInts(s string) ([]int, error) { _ = "STUB: not implemented"; return nil, nil }

func uniqueSortedPositive(in []int) []int { _ = "STUB: not implemented"; return nil }

// resolveOutPath locates precomputed.go relative to the working directory.
// We prefer the centrifuge-module-root layout (most common invocation)
// and fall back to running from inside the package directory.
func resolveOutPath(flagVal string) (string, error) { _ = "STUB: not implemented"; return "", nil }

func renderSource(sizes []int, tagsBySize map[int][]string) string {
	_ = "STUB: not implemented"
	return ""
}
