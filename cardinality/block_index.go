package cardinality

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/teststorage"
)

type BlockIndex struct {
	store *teststorage.TestStorage
}

func NewBlockIndex(store *teststorage.TestStorage) *BlockIndex {
	return &BlockIndex{store}
}

func (b *BlockIndex) AddSeries(_ labels.Labels, _ storage.SeriesRef) {}

func (b *BlockIndex) GetCardinality(matchers ...*labels.Matcher) int64 {
	// Get the head block from the test storage
	head := b.store.Head()

	// Retrieve the index reader for querying postings
	indexReader, err := head.Index()
	if err != nil {
		panic(fmt.Sprintf("failed to get index reader: %v", err))
	}
	defer indexReader.Close()

	// Get postings for the matchers
	var postings index.Postings
	for _, matcher := range matchers {
		var matcherPostings index.Postings

		switch matcher.Type {
		case labels.MatchEqual:
			// Get postings for exact match
			matcherPostings, err = indexReader.Postings(context.TODO(), matcher.Name, matcher.Value)
			if err != nil {
				panic(fmt.Sprintf("failed to get postings for matcher %s: %v", matcher.String(), err))
			}

		case labels.MatchNotEqual:
			// Get all postings for the label and exclude the specified value
			allPostings, err := indexReader.Postings(context.TODO(), matcher.Name, "")
			if err != nil {
				panic(fmt.Sprintf("failed to get all postings for label %s: %v", matcher.Name, err))
			}
			excludedPostings, err := indexReader.Postings(context.TODO(), matcher.Name, matcher.Value)
			if err != nil {
				panic(fmt.Sprintf("failed to get excluded postings for value %s: %v", matcher.Value, err))
			}
			matcherPostings = index.Without(allPostings, excludedPostings)

		case labels.MatchRegexp:
			// Iterate over all label values and match against the regex
			matcherPostings = nil
			allValues, err := indexReader.LabelValues(context.TODO(), matcher.Name)
			if err != nil {
				panic(fmt.Sprintf("failed to get all values for label %s: %v", matcher.Name, err))
			}
			for _, value := range allValues {
				if matcher.Matches(value) {
					valuePostings, err := indexReader.Postings(context.TODO(), matcher.Name, value)
					if err != nil {
						panic(fmt.Sprintf("failed to get postings for value %s: %v", value, err))
					}
					if matcherPostings == nil {
						matcherPostings = valuePostings
					} else {
						matcherPostings = index.Merge(context.TODO(), matcherPostings, valuePostings)
					}
				}
			}

		case labels.MatchNotRegexp:
			// Iterate over all label values and exclude matches against the regex
			matcherPostings = nil
			allValues, err := indexReader.LabelValues(context.TODO(), matcher.Name)
			if err != nil {
				panic(fmt.Sprintf("failed to get all values for label %s: %v", matcher.Name, err))
			}
			for _, value := range allValues {
				if !matcher.Matches(value) {
					valuePostings, err := indexReader.Postings(context.TODO(), matcher.Name, value)
					if err != nil {
						panic(fmt.Sprintf("failed to get postings for value %s: %v", value, err))
					}
					if matcherPostings == nil {
						matcherPostings = valuePostings
					} else {
						matcherPostings = index.Merge(context.TODO(), matcherPostings, valuePostings)
					}
				}
			}
		}

		if matcherPostings == nil {
			// No postings found for the matcher; no series match
			return 0
		}

		if postings == nil {
			// Initialize with the first matcher
			postings = matcherPostings
		} else {
			// Intersect postings for subsequent matchers
			postings = index.Intersect(postings, matcherPostings)
		}
	}

	// Iterate over the postings to count the number of series
	cardinality := int64(0)
	for postings.Next() {
		cardinality++
	}

	if err := postings.Err(); err != nil {
		panic(fmt.Sprintf("error iterating postings: %v", err))
	}

	return cardinality
}
