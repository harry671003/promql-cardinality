package cardinality

import (
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type BitmapIndex struct {
	index map[string]map[string]*roaring64.Bitmap
}

func NewBitmapIndex() *BitmapIndex {
	return &BitmapIndex{
		index: make(map[string]map[string]*roaring64.Bitmap),
	}
}

func (b *BitmapIndex) AddSeries(lbls labels.Labels, ref storage.SeriesRef) {
	for _, l := range lbls {
		lName := internString(l.Name)
		lValue := internString(l.Value)

		valueMap, ok := b.index[lName]
		if !ok {
			valueMap = make(map[string]*roaring64.Bitmap)
			b.index[lName] = valueMap
		}

		bitmap, ok := valueMap[lValue]
		if !ok {
			bitmap = roaring64.NewBitmap()
			valueMap[lValue] = bitmap
		}

		bitmap.Add(uint64(ref))
	}
}

func (b *BitmapIndex) GetCardinality(matchers ...*labels.Matcher) int64 {
	if len(matchers) == 0 {
		return 0
	}

	intersectionBitmap := b.getUnionBitmapForMatcher(matchers[0])

	for _, matcher := range matchers[1:] {
		matcherBitmap := b.getUnionBitmapForMatcher(matcher)
		intersectionBitmap.And(matcherBitmap)

		if intersectionBitmap.IsEmpty() {
			return 0
		}
	}

	return int64(intersectionBitmap.GetCardinality())
}

func (b *BitmapIndex) getUnionBitmapForMatcher(matcher *labels.Matcher) *roaring64.Bitmap {
	unionBitmap := roaring64.NewBitmap()

	if valueMap, ok := b.index[matcher.Name]; ok {
		switch matcher.Type {
		case labels.MatchEqual:
			if bitmap, exists := valueMap[matcher.Value]; exists {
				unionBitmap.Or(bitmap) // Exact match: Add the single bitmap
			}

		case labels.MatchRegexp:
			for value, bitmap := range valueMap {
				if matcher.Matches(value) {
					unionBitmap.Or(bitmap) // Regex match: Union all matching bitmaps
				}
			}

		case labels.MatchNotEqual:
			for value, bitmap := range valueMap {
				if value != matcher.Value {
					unionBitmap.Or(bitmap) // Exclude the specified value
				}
			}

		case labels.MatchNotRegexp:
			for value, bitmap := range valueMap {
				if !matcher.Matches(value) {
					unionBitmap.Or(bitmap) // Exclude values matching the regex
				}
			}
		}
	}

	return unionBitmap
}
