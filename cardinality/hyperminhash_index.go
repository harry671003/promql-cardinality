package cardinality

import (
	"encoding/binary"
	"github.com/axiomhq/hyperminhash"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type HyperMinHashIndex struct {
	index map[string]map[string]*hyperminhash.Sketch
}

func NewHyperMinHashIndex() *HyperMinHashIndex {
	return &HyperMinHashIndex{
		index: make(map[string]map[string]*hyperminhash.Sketch),
	}
}

func (h *HyperMinHashIndex) AddSeries(lbls labels.Labels, _ storage.SeriesRef) {
	hash := lbls.Hash()
	hashBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(hashBytes, hash)

	for _, l := range lbls {
		lName := internString(l.Name)
		lValue := internString(l.Value)

		// Retrieve or create the value map
		valueMap, ok := h.index[lName]
		if !ok {
			valueMap = make(map[string]*hyperminhash.Sketch)
			h.index[lName] = valueMap
		}

		// Retrieve or create the HLL sketch for the label value
		hll, ok := valueMap[lValue]
		if !ok {
			hll = hyperminhash.New()
			valueMap[lValue] = hll
		}

		hll.Add(hashBytes)
	}
}

func (h *HyperMinHashIndex) GetCardinality(matchers ...*labels.Matcher) int64 {
	return h.cardinalityUsingJacaards(matchers...)
}

// Estimate cardinality for a single matcher
func (h *HyperMinHashIndex) getSketchForMatcher(matcher *labels.Matcher) *hyperminhash.Sketch {
	resultSketch := hyperminhash.New()

	if valueMap, ok := h.index[matcher.Name]; ok {
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

func (h *HyperMinHashIndex) cardinalityUsingJacaards(matchers ...*labels.Matcher) int64 {
	if len(matchers) == 0 {
		return 0
	}

	// Retrieve the sketches for each matcher
	var sketches []*hyperminhash.Sketch
	for _, matcher := range matchers {
		sketch := h.getSketchForMatcher(matcher)
		if sketch == nil {
			// If any matcher has no corresponding data, the card is 0
			return 0
		}
		sketches = append(sketches, sketch)
	}

	card := int64(sketches[0].Cardinality())
	// Iterate over pairs of sketches and track the smallest card
	for i := 0; i < len(sketches); i++ {
		for j := i + 1; j < len(sketches); j++ {
			i := int64(sketches[i].Intersection(sketches[j]))
			if i < card {
				card = i
			}
		}
	}

	return card
}

func (h *HyperMinHashIndex) cardinalityUsingInclusionExclusion(matchers ...*labels.Matcher) int64 {
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
				subsetSketch = subsetSketch.Merge(h.getSketchForMatcher(matchers[i]))
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
