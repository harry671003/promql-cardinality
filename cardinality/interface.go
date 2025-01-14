package cardinality

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

var stringPool = map[string]string{}

func internString(s string) string {
	if pooled, exists := stringPool[s]; exists {
		return pooled
	}
	stringPool[s] = s
	return s
}

type CardinalityIndex interface {
	AddSeries(lbls labels.Labels, ref storage.SeriesRef)
	GetCardinality(matchers ...*labels.Matcher) int64
}
