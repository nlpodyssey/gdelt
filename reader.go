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
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
)

type eventsCsvReader struct {
	r *csv.Reader
}

func newEventsCsvReader(r io.Reader) *eventsCsvReader {
	csvReader := csv.NewReader(r)
	csvReader.Comma = '\t'
	return &eventsCsvReader{r: csvReader}
}

func (r *eventsCsvReader) read() (*Event, error) {
	csvRecord, err := r.r.Read()
	if err != nil {
		return nil, err // This includes io.EOF
	}

	if len(csvRecord) != 61 {
		return nil, fmt.Errorf("expected 61 CSV columns, actual %d", len(csvRecord))
	}

	event := &Event{}

	event.GlobalEventID, err = strconv.ParseUint(csvRecord[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GlobalEventID %#v", csvRecord[0])
	}

	event.Day, err = strconv.Atoi(csvRecord[1])
	if err != nil {
		return nil, fmt.Errorf("failed to parse Day %#v", csvRecord[1])
	}

	event.MonthYear, err = strconv.Atoi(csvRecord[2])
	if err != nil {
		return nil, fmt.Errorf("failed to parse MonthYear %#v", csvRecord[2])
	}

	event.Year, err = strconv.Atoi(csvRecord[3])
	if err != nil {
		return nil, fmt.Errorf("failed to parse Year %#v", csvRecord[3])
	}

	event.FractionDate, err = strconv.ParseFloat(csvRecord[4], 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse FractionDate %#v", csvRecord[4])
	}

	event.Actor1 = readActorData(csvRecord[5:15])
	event.Actor2 = readActorData(csvRecord[15:25])

	event.IsRootEvent, err = strconv.Atoi(csvRecord[25])
	if err != nil {
		return nil, fmt.Errorf("failed to parse IsRootEvent %#v", csvRecord[25])
	}

	event.EventCode = csvRecord[26]
	event.EventBaseCode = csvRecord[27]
	event.EventRootCode = csvRecord[28]

	event.QuadClass, err = strconv.Atoi(csvRecord[29])
	if err != nil {
		return nil, fmt.Errorf("failed to parse QuadClass %#v", csvRecord[29])
	}

	if len(csvRecord[30]) > 0 {
		event.GoldsteinScale.Valid = true
		event.GoldsteinScale.Float64, err = strconv.ParseFloat(csvRecord[30], 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse GoldsteinScale %#v", csvRecord[30])
		}
	}

	event.NumMentions, err = strconv.Atoi(csvRecord[31])
	if err != nil {
		return nil, fmt.Errorf("failed to parse NumMentions %#v", csvRecord[31])
	}

	event.NumSources, err = strconv.Atoi(csvRecord[32])
	if err != nil {
		return nil, fmt.Errorf("failed to parse NumSources %#v", csvRecord[32])
	}

	event.NumArticles, err = strconv.Atoi(csvRecord[33])
	if err != nil {
		return nil, fmt.Errorf("failed to parse NumArticles %#v", csvRecord[33])
	}

	event.AvgTone, err = strconv.ParseFloat(csvRecord[34], 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse AvgTone %#v", csvRecord[34])
	}

	event.Actor1Geo, err = readGeoData(csvRecord[35:43])
	if err != nil {
		return nil, fmt.Errorf("failed to read Actor1Geo: %v", err)
	}
	event.Actor2Geo, err = readGeoData(csvRecord[43:51])
	if err != nil {
		return nil, fmt.Errorf("failed to read Actor2Geo: %v", err)
	}
	event.ActionGeo, err = readGeoData(csvRecord[51:59])
	if err != nil {
		return nil, fmt.Errorf("failed to read ActionGeo: %v", err)
	}

	event.DateAdded, err = strconv.ParseUint(csvRecord[59], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DATEADDED %#v", csvRecord[59])
	}

	event.SourceURL = csvRecord[60]

	return event, nil
}

func readActorData(csvFields []string) (a ActorData) {
	a.Code = csvFields[0]
	a.Name = csvFields[1]
	a.CountryCode = csvFields[2]
	a.KnownGroupCode = csvFields[3]
	a.EthnicCode = csvFields[4]
	a.Religion1Code = csvFields[5]
	a.Religion2Code = csvFields[6]
	a.Type1Code = csvFields[7]
	a.Type2Code = csvFields[8]
	a.Type3Code = csvFields[9]
	return
}

func readGeoData(csvFields []string) (g GeoData, err error) {
	intGeoType, err := strconv.Atoi(csvFields[0])
	if err != nil {
		return g, fmt.Errorf("failed to parse Type %#v", csvFields[0])
	}
	var geoTypeOk bool
	g.Type, geoTypeOk = GeoTypeFromInt(intGeoType)
	if !geoTypeOk {
		return g, fmt.Errorf("unexpected GeoType value %d", intGeoType)
	}

	g.Fullname = csvFields[1]
	g.CountryCode = csvFields[2]
	g.ADM1Code = csvFields[3]
	g.ADM2Code = csvFields[4]

	if len(csvFields[5]) > 0 {
		g.Lat, err = ParseNullableFloat64(csvFields[5])
		if err != nil {
			return g, fmt.Errorf("failed to parse Lat %#v", csvFields[5])
		}
	}

	if len(csvFields[6]) > 0 {
		g.Long, err = ParseNullableFloat64(csvFields[6])
		if err != nil {
			return g, fmt.Errorf("failed to parse Long %#v", csvFields[6])
		}
	}

	g.FeatureID = csvFields[7]
	return
}
