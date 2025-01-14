package cardinality

import (
	"context"
	"fmt"
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

func BenchmarkCardinality(b *testing.B) {
	store, err := teststorage.NewWithError()
	require.NoError(b, err)
	defer store.Close()

	hmhIndex := NewHyperMinHashIndex()
	bitmapIndex := NewBitmapIndex()
	app := store.Appender(context.TODO())

	totalSeries, err := ingestData(app, func(ref storage.SeriesRef, lbls labels.Labels) {
		hmhIndex.AddSeries(lbls, ref)
		bitmapIndex.AddSeries(lbls, ref)
	})

	require.NoError(b, err)
	b.Logf("Total series: %d", totalSeries)

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, "user", ".*"),
		labels.MustNewMatcher(labels.MatchRegexp, "instance", ".*"),
		labels.MustNewMatcher(labels.MatchRegexp, "__name__", "blocks_loaded"),
	}

	card, err := getActualCard(store, matchers...)
	require.NoError(b, err)

	threshold := float64(50000)

	b.Run("HyperMinHash", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			estimate := hmhIndex.GetCardinality(matchers...)
			delta := math.Abs(float64(card - estimate))
			threshold := float64(50000)

			require.LessOrEqual(b, delta, threshold)
		}
	})

	b.Run("Bitmap", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			estimate := bitmapIndex.GetCardinality(matchers...)
			delta := math.Abs(float64(card - estimate))

			require.LessOrEqual(b, delta, threshold)
		}
	})
}

func TestCardinality(t *testing.T) {
	store := teststorage.New(t)
	defer store.Close()

	bitmapIndex := NewBitmapIndex()
	hmhIndex := NewHyperMinHashIndex()

	app := store.Appender(context.TODO())

	totalSeries, err := ingestData(app, func(ref storage.SeriesRef, lbls labels.Labels) {
		bitmapIndex.AddSeries(lbls, ref)
		hmhIndex.AddSeries(lbls, ref)
	})
	require.NoError(t, err)
	t.Logf("Total series: %d", totalSeries)

	testCases := []struct {
		name     string
		matchers []*labels.Matcher
	}{
		{
			name: "Pod and Metric Match",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-0"),
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "http_request_total|ingester_active_series"),
			},
		},
		{
			name: "Method Match",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "method", "GET|POST|PUT|PATCH|DELETE"),
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_request_total"),
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
			name: "Metric not equal metric with specific pod",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "__name__", "ingester_active_series"),
				labels.MustNewMatcher(labels.MatchRegexp, "method", ".+"),
				labels.MustNewMatcher(labels.MatchEqual, "pod", "pod-1"),
			},
		},
		{
			name: "900K",
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
		{
			name: "1M with regex",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "user", ".*"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", ".*"),
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "blocks_loaded"),
			},
		},
	}

	indexes := []struct {
		name  string
		index CardinalityIndex
	}{
		{"Bitmap", bitmapIndex},
		{"HyperMinMax", hmhIndex},
	}

	for _, tt := range testCases {
		for _, ix := range indexes {
			t.Run(fmt.Sprintf("%s %s", ix.name, tt.name), func(t *testing.T) {
				// Get actual cardinality by querying the storage.
				actualCard, err := getActualCard(store, tt.matchers...)
				require.NoError(t, err)

				// Get estimated cardinality
				estimated := ix.index.GetCardinality(tt.matchers...)

				t.Logf("Test: %s, Actual GetCardinality: %d, Estimated GetCardinality: %d", tt.name, actualCard, estimated)

				// Compare the actual and estimated cardinality
				validateDelta(t, actualCard, int64(estimated), "BitMap")
			})
		}
	}

	// pprof
	printProfile()
}

func ingestData(app storage.Appender, updateFn func(storage.SeriesRef, labels.Labels)) (int, error) {
	builder := labels.NewBuilder(labels.Labels{})

	metricNames := generateMetricMap()

	totalSeries := 0
	// Iterate over all metric names
	for metricName, metricLabels := range metricNames {
		// Start with the first label's values
		labelValueCombinations := generateCombinations(metricLabels)

		// Iterate over each combination of label values
		for _, labelCombination := range labelValueCombinations {
			// Set the metric name and the label combination
			builder.Reset(labels.Labels{})
			builder.Set("__name__", metricName)
			for _, label := range labelCombination {
				builder.Set(label.Name, label.Value)
			}

			lbls := builder.Labels()
			ref, err := app.Append(0, lbls, 0, 1)
			if err != nil {
				return 0, err
			}

			updateFn(ref, lbls)
			totalSeries++
		}
	}
	err := app.Commit()
	return totalSeries, err
}

func getActualCard(store storage.Storage, matchers ...*labels.Matcher) (int64, error) {
	// Query the actual cardinality using the provided matchers
	querier, err := store.Querier(0, math.MaxInt64)
	if err != nil {
		return 0, err
	}

	set := querier.Select(context.TODO(), false, nil, matchers...)
	card := 0
	for set.Next() {
		card++
	}

	return int64(card), nil
}

func validateDelta(t *testing.T, actual, estimate int64, name string) {
	delta := math.Abs(float64(actual - estimate))
	threshold := float64(50000)

	assert.LessOrEqual(t, delta, threshold, "[%s] Actual cardinality %d differs too much from estimated cardinality %d", name, actual, estimate)
}

// Helper function to generate pod labels
func generateValues(pre string, count int) []string {
	values := make([]string, 0, count)
	for i := 0; i < count; i++ {
		values = append(values, internString(fmt.Sprintf("%s-%d", pre, i)))
	}
	return values
}

type labelValue struct {
	name      string
	values    []string
	valueFunc func() []string
}

func generateMetricMap() map[string][]labelValue {
	metricNameMap := map[string][]labelValue{
		"http_request_total": {
			{
				name:   "method",
				values: []string{"GET", "POST", "PUT", "PATCH", "DELETE"},
			},
			{
				name: "pod",
				valueFunc: func() []string {
					return generateValues("pod", 1000)
				},
			},
			{
				name: "user",
				valueFunc: func() []string {
					return generateValues("user", 100)
				},
			},
		},
		"ingester_active_series": {
			{
				name:   "api",
				values: []string{"QueryStream", "Push", "MetricsForLabelMatchers", "LabelValues", "LabelNames"},
			},
			{
				name: "ingester",
				valueFunc: func() []string {
					return generateValues("ingester", 1000)
				},
			},
			{
				name: "user",
				valueFunc: func() []string {
					return generateValues("user", 100)
				},
			},
		},
		"blocks_loaded": {
			{
				name: "block",
				valueFunc: func() []string {
					return generateValues("ID", 1000)
				},
			},
			{
				name: "instance",
				valueFunc: func() []string {
					return generateValues("store-gateway", 100)
				},
			},
			{
				name: "user",
				valueFunc: func() []string {
					return generateValues("user", 10)
				},
			},
		},
	}

	return metricNameMap
}

func generateCombinations(metricLabels []labelValue) [][]labels.Label {
	// Prepare to store all combinations of labels and their values
	var labelCombinations [][]labels.Label
	// Initialize with an empty combination
	labelCombinations = append(labelCombinations, []labels.Label{})

	// For each label, generate all possible combinations
	for _, label := range metricLabels {
		// Get the values for this label
		var values []string
		if label.valueFunc != nil {
			values = label.valueFunc()
		} else {
			values = label.values
		}

		// Create new combinations by appending each value for the current label
		var newCombinations [][]labels.Label
		for _, combination := range labelCombinations {
			for _, value := range values {
				// Append this value to the existing combination
				newCombination := append(combination, labels.Label{Name: label.name, Value: value})
				newCombinations = append(newCombinations, newCombination)
			}
		}

		// AddSeries the labelCombinations with the new combinations
		labelCombinations = newCombinations
	}

	return labelCombinations
}

func printProfile() {
	f, _ := os.Create("memprofile.prof")
	pprof.WriteHeapProfile(f)
	f.Close()
}
