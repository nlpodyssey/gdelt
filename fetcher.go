// Copyright 2023 The NLP Odyssey Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gdelt

import (
	"archive/zip"
	"bytes"
	"crypto/md5"
	"encoding/csv"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	// LastUpdateURL provides the last 15 Minutes CSV Data File List – English.
	// (Updated every 15 minutes).
	LastUpdateURL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

	// LastUpdateTranslationURL provides the last 15 Minutes CSV Data File
	// List – GDELT Translingual. (Updated every 15 minutes).
	LastUpdateTranslationURL = "http://data.gdeltproject.org/gdeltv2/lastupdate-translation.txt"
)

var DefaultOpts = Opts{
	AllowedCameoRootCodes: []string{"13", "14", "15", "17", "18", "19", "20"},
	SkipDuplicates:        true,
	SkipFutureEvents:      true,
	Translingual:          false,
	MaxTitleLength:        150,
}

// BadStatusCodeError indicates an unexpected HTTP response status code.
// It provides minimal information. It can be wrapped and recognized
// using IsBadStatusCodeError.
type BadStatusCodeError struct {
	StatusCode int
}

func (err BadStatusCodeError) Error() string {
	return fmt.Sprintf("bad HTTP response status code %d", err.StatusCode)
}

func NewBadStatusCodeError(statusCode int) BadStatusCodeError {
	return BadStatusCodeError{StatusCode: statusCode}
}

func IsBadStatusCodeError(err error) bool {
	return errors.As(err, &BadStatusCodeError{})
}

// Opts contains options for FetchLatestEvents.
type Opts struct {
	SkipDuplicates        bool
	SkipFutureEvents      bool
	MaxTitleLength        int
	Translingual          bool
	AllowedCameoRootCodes []string
}

// FetchLatestEvents returns the latest GDELT events.
func FetchLatestEvents(opts Opts) (_ []*Event, err error) {
	a, err := getLatestEvents(LastUpdateURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest events from %q: %w", LastUpdateURL, err)
	}
	if !opts.Translingual {
		return filterEvents(a, opts)
	}

	b, err := getLatestEvents(LastUpdateTranslationURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest events from %q: %w", LastUpdateTranslationURL, err)
	}
	return filterEvents(append(a, b...), opts)
}

func filterEvents(evs []*Event, opts Opts) (_ []*Event, err error) {
	result := make([]*Event, 0, len(evs))

	visitedURLs := make(map[string]struct{}, len(evs))

	for _, ev := range evs {
		if len(ev.SourceURL) == 0 || ev.GKGArticle == nil || len(ev.GKGArticle.Extras.PageTitle) == 0 {
			continue
		}
		publishedAt, err := ev.DateAddedTime()
		if err != nil {
			// Ignore the error, just discard the event.
			continue
		}
		if opts.SkipFutureEvents && publishedAt.After(time.Now()) {
			// Ignore future events.
			continue
		}
		if len([]rune(ev.GKGArticle.Extras.PageTitle)) > opts.MaxTitleLength {
			// Ignore events with long titles.
			continue
		}
		if !isEventCodeAllowed(opts.AllowedCameoRootCodes, ev.EventRootCode) {
			continue
		}
		if _, ok := visitedURLs[ev.SourceURL]; ok && opts.SkipDuplicates {
			continue
		}
		result = append(result, ev)
		visitedURLs[ev.SourceURL] = struct{}{}
	}

	return result, nil
}

func getLatestEvents(url string) (_ []*Event, err error) {
	defer func() {
		// Avoid hard failures because of bad server responses.
		if IsBadStatusCodeError(err) {
			log.Warn().Err(err).Str("URL", url).Msgf("failed to get latest GDELT events")
			err = nil
		}
	}()

	fr, err := getFileReferences(url)
	if err != nil {
		return nil, err
	}

	evs, err := getEventsFromURL(fr.Export.URL, fr.Export.MD5Sum, fr.Export.Size)
	if err != nil {
		return nil, fmt.Errorf("failed to get export data: %w", err)
	}

	articles, err := getArticleFromURL(fr.GKG.URL, fr.GKG.MD5Sum, fr.GKG.Size)
	if err != nil {
		return nil, fmt.Errorf("failed to get GKG data: %w", err)
	}

	am := make(map[string]*Article, len(articles))
	for _, a := range articles {
		if _, ok := am[a.DocumentIdentifier]; ok {
			return nil, fmt.Errorf("duplicate document identifier in articles: %q", a.DocumentIdentifier)
		}
		am[a.DocumentIdentifier] = a
	}

	for _, e := range evs {
		a, ok := am[e.SourceURL]
		if !ok {
			continue
		}
		e.GKGArticle = a
	}

	return evs, nil
}

func isEventCodeAllowed(allowedEventRootCodes []string, currentEventCode string) bool {
	if allowedEventRootCodes == nil || len(allowedEventRootCodes) == 0 {
		return true
	}
	for _, code := range allowedEventRootCodes {
		if code == currentEventCode {
			return true
		}
	}
	return false
}

type fileReference struct {
	Size   int
	MD5Sum string
	URL    string
}

type fileReferences struct {
	Export   fileReference
	Mentions fileReference
	GKG      fileReference
}

func getFileReferences(url string) (_ *fileReferences, err error) {
	resp, err := httpGetFileReferences(url)
	if err != nil {
		return nil, fmt.Errorf("failed to HTTP get %q: %w", url, err)
	}
	frs, err := parseFileReferencesResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response from %q: %w", url, err)
	}
	return frs, nil
}

func httpGetFileReferences(url string) (_ string, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("HTTP getFileReferences error: %w", err)
	}
	defer func() {
		if e := resp.Body.Close(); e != nil && err == nil {
			err = e
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return "", NewBadStatusCodeError(resp.StatusCode)
	}

	bs, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}
	return string(bs), err
}

func parseFileReferencesResponse(resp string) (*fileReferences, error) {
	resp = strings.TrimSpace(resp)
	rows := strings.Split(resp, "\n")
	if len(rows) != 3 {
		return nil, fmt.Errorf("want 3 rows, got %d", len(rows))
	}

	frs := new(fileReferences)
	for _, row := range rows {
		err := parseFileReferencesRow(row, frs)
		if err != nil {
			return nil, err
		}
	}
	return frs, nil
}

func parseFileReferencesRow(row string, frs *fileReferences) error {
	fields := strings.Split(row, " ")
	if len(fields) != 3 {
		return fmt.Errorf("want 3 fields, got %d", len(fields))
	}
	size, err := strconv.Atoi(fields[0])
	if err != nil {
		return fmt.Errorf("failed to parse Size field as int: %q", fields[0])
	}

	fr := fileReference{
		Size:   size,
		MD5Sum: fields[1],
		URL:    fields[2],
	}
	switch {
	case strings.HasSuffix(fr.URL, ".export.CSV.zip"):
		frs.Export = fr
	case strings.HasSuffix(fr.URL, ".mentions.CSV.zip"):
		frs.Mentions = fr
	case strings.HasSuffix(fr.URL, ".gkg.csv.zip"):
		frs.GKG = fr
	default:
		return fmt.Errorf("unexpected suffix for URL: %q", fr.URL)
	}
	return nil
}

func getArticleFromURL(url, md5sum string, size int) ([]*Article, error) {
	content, err := httpGet(url)
	if err != nil {
		return nil, fmt.Errorf("failed to HTTP get %q: %w", url, err)
	}

	if len(content) != size {
		return nil, fmt.Errorf("expected content size %d, actual %d", size, len(content))
	}

	err = checkMD5Sum(content, md5sum)
	if err != nil {
		return nil, fmt.Errorf("failed to validate %q: %w", url, err)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(content), int64(size))
	if err != nil {
		return nil, fmt.Errorf("zip reader error: %w", err)
	}

	if len(zipReader.File) != 1 {
		return nil, fmt.Errorf("want 1 file in zip, got %d", len(zipReader.File))
	}

	records, err := processArticleFile(zipReader.File[0])
	if err != nil {
		return nil, err
	}
	return records, nil
}

func processArticleFile(zf *zip.File) (records []*Article, err error) {
	f, err := zf.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open zip file: %w", err)
	}
	defer func() {
		if e := f.Close(); e != nil && err == nil {
			err = e
		}
	}()

	records = make([]*Article, 0)

	r := csv.NewReader(f)
	r.Comma = '\t'
	r.LazyQuotes = true
	for i := 0; ; i++ {
		fields, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Warn().Err(err).Int("row", i).Msg("failed to read GDELT GKG CSV record")
			continue
		}
		a, err := makeArticle(fields)
		if err != nil {
			return nil, err
		}
		records = append(records, a)
	}

	return records, nil
}

func makeArticle(fields []string) (a *Article, err error) {
	if len(fields) != 27 {
		return nil, fmt.Errorf("expected 27 CSV columns, actual %d", len(fields))
	}
	a = new(Article)
	a.ID = fields[0]
	a.DocumentIdentifier = fields[4]
	a.SharingImage = strings.TrimSpace(fields[18])
	a.Extras = parseArticleExtras(fields[26])
	return
}

var pageTitleRe = regexp.MustCompile(`<PAGE_TITLE>(.*)</PAGE_TITLE>`)
var spaceRegexp = regexp.MustCompile(`\s`)

func parseArticleExtras(extrasXML string) (ex ArticleExtras) {
	sm := pageTitleRe.FindStringSubmatch(extrasXML)
	if len(sm) == 2 {
		s := html.UnescapeString(sm[1])
		s = spaceRegexp.ReplaceAllString(s, " ")
		ex.PageTitle = strings.TrimSpace(s)
	}
	return
}

func httpGet(url string) (_ []byte, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() {
		if e := resp.Body.Close(); e != nil && err == nil {
			err = e
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, NewBadStatusCodeError(resp.StatusCode)
	}

	bs, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	return bs, err
}

func getEventsFromURL(url, md5sum string, size int) ([]*Event, error) {
	content, err := httpGet(url)
	if err != nil {
		return nil, fmt.Errorf("failed to HTTP get %q: %w", url, err)
	}

	if len(content) != size {
		return nil, fmt.Errorf("expected content size %d, actual %d", size, len(content))
	}

	err = checkMD5Sum(content, md5sum)
	if err != nil {
		return nil, fmt.Errorf("failed to validate %q: %w", url, err)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(content), int64(size))
	if err != nil {
		return nil, fmt.Errorf("zip reader error: %w", err)
	}

	if len(zipReader.File) != 1 {
		return nil, fmt.Errorf("want 1 file in zip, got %d", len(zipReader.File))
	}

	records, err := processEventFile(zipReader.File[0])
	if err != nil {
		return nil, err
	}
	return records, nil
}

func processEventFile(zf *zip.File) (records []*Event, err error) {
	f, err := zf.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open zip file: %w", err)
	}
	defer func() {
		if e := f.Close(); e != nil && err == nil {
			err = e
		}
	}()

	records = make([]*Event, 0)

	r := newEventsCsvReader(f)
	for i := 0; ; i++ {
		event, err := r.read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Warn().Err(err).Int("row", i).Msg("failed to read GDELT export CSV record")
			continue
		}
		records = append(records, event)
	}

	return records, nil
}

func checkMD5Sum(content []byte, expected string) error {
	actual := fmt.Sprintf("%x", md5.Sum(content))
	if actual != expected {
		return fmt.Errorf("md5 sum: expected %q, actual %q", expected, actual)
	}
	return nil
}
