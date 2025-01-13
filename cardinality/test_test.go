package cardinality

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/axiomhq/hyperminhash"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"runtime/pprof"
	"testing"
)

var stringPool = map[string]string{}

func internString(s string) string {
	if pooled, exists := stringPool[s]; exists {
		return pooled
	}
	stringPool[s] = s
	return s
}

func Test(t *testing.T) {
	// Step 1: Create the series and insert into the HLL
	store := teststorage.New(t)
	defer store.Close()

	hllMap := make(map[string]map[string]*hyperminhash.Sketch)
	app := store.Appender(context.TODO())

	builder := labels.NewBuilder(labels.Labels{})
	var err error

	// Generate series labels for pods and metrics
	pods := generatePodLabels(1000)
	metricNames := generateMetricNames(1000)

	totalSeries := 0
	for _, metricName := range metricNames {
		for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE"} {
			for _, pod := range pods {
				builder.Reset(labels.Labels{})
				builder.Set("__name__", metricName)
				builder.Set("method", method)
				builder.Set("pod", pod)

				lbls := builder.Labels()
				_, err = app.Append(0, lbls, 0, 1)
				require.NoError(t, err)

				hash := lbls.Hash()
				hashBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(hashBytes, hash)

				// Update the HLL sketch
				updateHLL(hllMap, lbls, hashBytes)
				totalSeries++
			}
		}
	}

	t.Logf("Total series: %d", totalSeries)

	err = app.Commit()
	require.NoError(t, err)

	testCases := []struct {
		name     string
		matchers []*labels.Matcher
	}{
		{
			name: "Pod and Metric Match",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-0"),
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "metric-0"),
			},
		},
		{
			name: "Method Match",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "method", "GET|POST|PUT|PATCH|DELETE"),
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "metric-0"),
				labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-[0-9]"),
			},
		},
		{
			name: "Heavy Match",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "method", "GET"),
			},
		},
		{
			name: "Heavy Match with pods",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "method", "GET"),
				labels.MustNewMatcher(labels.MatchEqual, "pod", "pod-0"),
			},
		},
		{
			name: "Heavy Match",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "method", ".+"),
			},
		},
		{
			name: "All metric",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".+"),
			},
		},
		{
			name: "All metric with unknown pod",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".+"),
				labels.MustNewMatcher(labels.MatchRegexp, "method", ".+"),
				labels.MustNewMatcher(labels.MatchRegexp, "pod", "unknown"),
			},
		},
		{
			name: "All metric with specific pod",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".+"),
				labels.MustNewMatcher(labels.MatchRegexp, "method", ".+"),
				labels.MustNewMatcher(labels.MatchEqual, "pod", "pod-1"),
			},
		},
		{
			name: "All metric with specific pod",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "__name__", "abc"),
				labels.MustNewMatcher(labels.MatchRegexp, "method", ".+"),
				labels.MustNewMatcher(labels.MatchEqual, "pod", "pod-1"),
			},
		},
		{
			name: "1 million",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "method", "GET"),
				labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-([0-9][0-9][0-9])"),
			},
		},
		{
			name: "20K",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "method", "GET|POST"),
				labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-([0-9])"),
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".*"),
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			actualCard := getActualCard(t, store, tt.matchers...)
			// Compare the actual and estimated cardinality
			jacaardEstimate := estimateForMatchersJaccardPairWise(hllMap, tt.matchers...)
			inExEstimate := estimateUsingInclusionExclusion(hllMap, tt.matchers...)

			t.Logf("Test: %s, Actual Cardinality: %d, Jacaard Estimated Cardinality: %d, Inclusion-Exclusion Cardinality: %d", tt.name, actualCard, jacaardEstimate, inExEstimate)

			validateDelta(t, actualCard, inExEstimate, "InclusionExclusion")
			validateDelta(t, actualCard, jacaardEstimate, "Jacaard")
		})
	}

	// pprof
	printProfile()
}

func getActualCard(t *testing.T, store storage.Storage, matchers ...*labels.Matcher) int64 {
	// Query the actual cardinality using the provided matchers
	querier, err := store.Querier(0, math.MaxInt64)
	require.NoError(t, err)

	set := querier.Select(context.TODO(), false, nil, matchers...)
	card := 0
	for set.Next() {
		card++
	}

	return int64(card)
}

func validateDelta(t *testing.T, actual, estimate int64, name string) {
	delta := math.Abs(float64(actual - estimate))
	threshold := float64(50000)

	assert.LessOrEqual(t, delta, threshold, "[%s] Actual cardinality %d differs too much from estimated cardinality %d", name, actual, estimate)
}

// Helper function to generate pod labels
func generatePodLabels(count int) []string {
	pods := make([]string, 0, count)
	for i := 0; i < count; i++ {
		pods = append(pods, internString(fmt.Sprintf("pod-%d", i)))
	}
	return pods
}

// Helper function to generate metric names
func generateMetricNames(count int) []string {
	metricNames := make([]string, 0, count)
	for i := 0; i < count; i++ {
		metricNames = append(metricNames, internString(fmt.Sprintf("metric-%d", i)))
	}
	return metricNames
}

// Helper function to update HLL sketch for a given label set
func updateHLL(hllMap map[string]map[string]*hyperminhash.Sketch, lbls labels.Labels, hashBytes []byte) {
	for _, l := range lbls {
		lName := internString(l.Name)
		lValue := internString(l.Value)

		// Retrieve or create the value map
		valueMap, ok := hllMap[lName]
		if !ok {
			valueMap = make(map[string]*hyperminhash.Sketch)
			hllMap[lName] = valueMap
		}

		// Retrieve or create the HLL sketch for the label value
		hll, ok := valueMap[lValue]
		if !ok {
			hll = hyperminhash.New()
			valueMap[lValue] = hll
		}

		// Add the hash to the HLL sketch
		hll.Add(hashBytes)
	}
}

func printProfile() {
	f, _ := os.Create("memprofile.prof")
	pprof.WriteHeapProfile(f)
	f.Close()
}

// Estimate cardinality for a single matcher
func getSketchForMatcher(hllMap map[string]map[string]*hyperminhash.Sketch, matcher *labels.Matcher) *hyperminhash.Sketch {
	resultSketch := hyperminhash.New()

	if valueMap, ok := hllMap[matcher.Name]; ok {
		switch matcher.Type {
		case labels.MatchEqual:
			if hll, exists := valueMap[matcher.Value]; exists {
				resultSketch = resultSketch.Merge(hll) // Exact match: Merge single HLL
			}

		case labels.MatchRegexp:
			for value, hll := range valueMap {
				if matcher.Matches(value) {
					resultSketch = resultSketch.Merge(hll) // Regex match: Merge all matching HLLs
				}
			}

		case labels.MatchNotEqual:
			for value, hll := range valueMap {
				if value != matcher.Value {
					resultSketch = resultSketch.Merge(hll) // Exclude the specified value
				}
			}

		case labels.MatchNotRegexp:
			for value, hll := range valueMap {
				if !matcher.Matches(value) {
					resultSketch = resultSketch.Merge(hll) // Exclude values matching the regex
				}
			}
		}
	}
	return resultSketch
}

func estimateForMatchersJaccardPairWise(hllMap map[string]map[string]*hyperminhash.Sketch, matchers ...*labels.Matcher) int64 {
	if len(matchers) == 0 {
		return 0
	}

	// Retrieve the sketches for each matcher
	var sketches []*hyperminhash.Sketch
	for _, matcher := range matchers {
		sketch := getSketchForMatcher(hllMap, matcher)
		if sketch == nil {
			// If any matcher has no corresponding data, the intersection is 0
			return 0
		}
		sketches = append(sketches, sketch)
	}

	intersection := int64(sketches[0].Cardinality())
	// Iterate over pairs of sketches and track the smallest intersection
	for i := 0; i < len(sketches); i++ {
		for j := i + 1; j < len(sketches); j++ {
			i := int64(sketches[i].Intersection(sketches[j]))
			if i < intersection {
				intersection = i
			}
		}
	}

	return intersection
}

func estimateUsingInclusionExclusion(hllMap map[string]map[string]*hyperminhash.Sketch, matchers ...*labels.Matcher) int64 {
	if len(matchers) == 0 {
		return 0
	}

	// Generate all possible combinations of matchers (powerset)
	n := len(matchers)
	totalMatchers := 1 << n // 2^n subsets

	// Use Inclusion-Exclusion formula
	var result int64
	for subset := 1; subset < totalMatchers; subset++ {
		subsetSketch := hyperminhash.New()
		includedMatchers := 0

		for i := 0; i < n; i++ {
			if subset&(1<<i) != 0 { // Check if matcher i is in the current subset
				subsetSketch = subsetSketch.Merge(getSketchForMatcher(hllMap, matchers[i]))
				includedMatchers++
			}
		}

		// Calculate cardinality for the subset and include/exclude based on the number of matchers in the subset
		subsetCardinality := int64(subsetSketch.Cardinality())
		if includedMatchers%2 == 1 {
			result += subsetCardinality // Include odd-sized intersections
		} else {
			result -= subsetCardinality // Exclude even-sized intersections
		}
	}

	return result
}
