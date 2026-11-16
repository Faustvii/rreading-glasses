package internal

import (
	"errors"
	"fmt"
	"net/http"
)

var (
	errNotFound   = statusErr(http.StatusNotFound)
	errBadRequest = statusErr(http.StatusBadRequest)

	errMissingIDs = errors.Join(fmt.Errorf(`missing "ids"`), errBadRequest)

	// errCatalogTruncated reports that the upstream refuses to page further
	// through an author's catalogue. Items yielded before it are valid, but
	// the catalogue is incomplete and retrying cannot get past the limit.
	errCatalogTruncated = errors.New("upstream catalogue truncated")
)

type statusErr int

var _ error = (*statusErr)(nil)

func (s statusErr) Status() int {
	return int(s)
}

func (s statusErr) Error() string {
	return fmt.Sprintf("HTTP %d: %s", s, http.StatusText(int(s)))
}
