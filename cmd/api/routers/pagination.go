package routers

import (
	"fmt"
	"net/url"
	"strconv"
)

const (
	defaultPage     = 1
	maxPage         = 1_000_000
	defaultPageSize = 50
	maxPageSize     = 100
)

func parsePagination(query url.Values) (page, pageSize int, err error) {
	page, err = boundedIntParameter(query, "page", defaultPage, maxPage)
	if err != nil {
		return 0, 0, err
	}

	pageSize, err = boundedIntParameter(query, "page_size", defaultPageSize, maxPageSize)
	if err != nil {
		return 0, 0, err
	}

	return page, pageSize, nil
}

func boundedIntParameter(query url.Values, name string, defaultValue, maxValue int) (int, error) {
	if !query.Has(name) {
		return defaultValue, nil
	}

	value, err := strconv.Atoi(query.Get(name))
	if err != nil || value < 1 || value > maxValue {
		return 0, fmt.Errorf("%s must be a whole number between 1 and %d", name, maxValue)
	}

	return value, nil
}
