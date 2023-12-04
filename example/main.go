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

package main

import (
	"fmt"
	"time"

	"github.com/nlpodyssey/gdelt"
	"github.com/rs/zerolog/log"
)

func main() {

	log.Info().Msg("getting latest events")

	events, err := gdelt.FetchLatestEvents(gdelt.DefaultOpts)
	if err != nil {
		log.Fatal().Err(err).Msg("error fetching latest events")
	}

	log.Info().Msgf("processing %d events", len(events))

	for _, event := range events {
		doc := struct {
			EventID     uint64
			URI         string
			Headline    string
			ImageURI    string
			PublishedAt time.Time
		}{
			EventID:     event.GlobalEventID,
			URI:         event.SourceURL,
			Headline:    event.GKGArticle.Extras.PageTitle,
			ImageURI:    event.GKGArticle.SharingImage,
			PublishedAt: event.PublishedAt(),
		}

		fmt.Printf("%+v\n", doc)
	}
}
