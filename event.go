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
	"fmt"
	"strconv"
	"time"
)

type Event struct {
	// GlobalEventID is the globally unique identifier assigned to each event
	// record that uniquely identifies it in GDELT master dataset.
	GlobalEventID uint64
	Day           int
	MonthYear     int
	Year          int
	FractionDate  float64

	Actor1 ActorData
	Actor2 ActorData

	IsRootEvent int
	// EventCode is the raw CAMEO action code describing the action that Actor1
	// performed upon Actor2.
	EventCode string
	// EventBaseCode is the level two leaf root node category, when applicable.
	// CAMEO event codes are defined in a three-level taxonomy. For events at
	// level three in the taxonomy, this yields its level two leaf root node.
	// For example, code "0251" ("Appeal for easing of administrative
	// sanctions") would yield an EventBaseCode of "025" ("Appeal to yield").
	// This makes it possible to aggregate events at various resolutions of
	// specificity. For events at levels two or one, this field will be set
	// to EventCode.
	EventBaseCode string
	// EventRootCode is similar to EventBaseCode and defines the root-level
	// category the event code falls under. For example, code "0251" ("Appeal
	// for easing of administrative sanctions") has a root code of "02"
	// ("Appeal"). This makes it possible to aggregate events at various
	// resolutions of specificity. For events at levels two or one, this field
	// will be set to EventCode.
	EventRootCode  string
	QuadClass      int
	GoldsteinScale NullableFloat64
	NumMentions    int
	NumSources     int
	NumArticles    int
	AvgTone        float64

	Actor1Geo GeoData
	Actor2Geo GeoData
	// ActionGeo captures the location information closest to the point in the
	// event description that contains the actual statement of action and is
	// the best location to use for placing events on a map or in other spatial
	// context.
	ActionGeo GeoData

	// DateAdded stores the date the event was added to the master database in
	// "YYYYMMDDHHMMSS" format in the UTC timezone.
	DateAdded uint64
	// SourceURL records the URL or citation of the first news report it found
	// this event in. In most cases this is the first report it saw the article
	// in, but due to the timing and flow of news reports through the processing
	// pipeline, this may not always be the very first report, but is at least
	// in the first few reports.
	SourceURL string

	GKGArticle *Article
}

// NullableFloat64 represents a float64 value that may be null.
type NullableFloat64 struct {
	Float64 float64
	// Valid is true if Float64 is not NULL
	Valid bool
}

var nullNullableFloat64 = NullableFloat64{Float64: 0, Valid: false}

// ParseNullableFloat64 parses a string value, converting it to NullableFloat64.
func ParseNullableFloat64(value string) (NullableFloat64, error) {
	if len(value) == 0 {
		return nullNullableFloat64, nil
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nullNullableFloat64, err
	}
	return NullableFloat64{Float64: f, Valid: true}, nil
}

type Article struct {
	ID                 string
	DocumentIdentifier string
	SharingImage       string
	Extras             ArticleExtras
}

type ArticleExtras struct {
	PageTitle string `xml:"PAGE_TITLE"`
}

type ActorData struct {
	Code           string
	Name           string
	CountryCode    string
	KnownGroupCode string
	EthnicCode     string
	Religion1Code  string
	Religion2Code  string
	Type1Code      string
	Type2Code      string
	Type3Code      string
}

type GeoData struct {
	// Type specifies the geographic resolution of the match type.
	Type GeoType
	// Fullname is the full human-readable name of the matched location. In
	// the case of a country it is simply the country name. For US and World
	// states it is in the format of "State, Country Name", while for all other
	// matches it is in the format of "City/Landmark, State, Country".
	// This can be used to label locations when placing events on a map.
	Fullname string
	// CountryCode is the 2-character FIPS10-4 country code for the location.
	CountryCode string
	ADM1Code    string
	ADM2Code    string
	// Lat is the centroid latitude of the landmark for mapping.
	Lat NullableFloat64
	// Long is the centroid longitude of the landmark for mapping.
	Long      NullableFloat64
	FeatureID string
}

func (g *GeoData) CountryCodeISO31661() (string, error) {
	if len(g.CountryCode) == 0 {
		return "", nil
	}
	isoCode, ok := FIPS104ToISO31661[g.CountryCode]
	if !ok {
		return "", fmt.Errorf("gdelt: unknown FIPS 10-4 country code %#v", g.CountryCode)
	}
	return isoCode, nil
}

// GeoType specifies the geographic resolution of the match type.
type GeoType uint8

const (
	NoGeoType GeoType = iota
	Country
	USState
	USCity
	WorldCity
	WorldState
)

func GeoTypeFromInt(value int) (GeoType, bool) {
	if value < 0 && value > 5 {
		return 0, false
	}
	return GeoType(value), true
}

func (g GeoType) String() string {
	switch g {
	case Country:
		return "COUNTRY"
	case USState:
		return "USSTATE"
	case USCity:
		return "USCITY"
	case WorldCity:
		return "WORLDCITY"
	case WorldState:
		return "WORLDSTATE"
	default:
		return ""
	}
}

var dateAddedTimeLayout = "20060102150405"

// DateAddedTime converts DateAdded int value to time.Time.
func (e *Event) DateAddedTime() (time.Time, error) {
	s := fmt.Sprintf("%014d", e.DateAdded)
	if len(s) != 14 {
		return time.Time{}, fmt.Errorf("unexpected DateAdded value %d", e.DateAdded)
	}
	return time.Parse(dateAddedTimeLayout, s)
}

// PublishedAt returns the time the event was published.
// It is an alias for DateAddedTime without error.
func (e *Event) PublishedAt() time.Time {
	t, err := e.DateAddedTime()
	if err != nil {
		return time.Time{} // Should never happen.
	}
	return t
}

// AllCameoEventCodes returns one or more CAMEO event codes from EventCode,
// EventBaseCode, and EventRootCode, keeping only one unique category code per
// level.
func (e *Event) AllCameoEventCodes() []string {
	s := make([]string, 0)
	if len(e.EventRootCode) == 0 {
		return s
	}
	s = append(s, e.EventRootCode)
	if e.EventBaseCode == e.EventRootCode || len(e.EventBaseCode) == 0 {
		return s
	}
	s = append(s, e.EventBaseCode)
	if e.EventCode == e.EventBaseCode || e.EventCode == e.EventRootCode || len(e.EventCode) == 0 {
		return s
	}
	s = append(s, e.EventCode)
	return s
}
