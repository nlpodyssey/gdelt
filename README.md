# GDELT Fetcher

## Overview
This package provides tools for fetching and parsing the latest GDELT events data in Go.

## Installation
To use this package, import it into your Go project:

```console
go get -u github.com/nlpodyssey/gdelt
```

## Example
```go
package main

import (
	"fmt"
	"time"

	"github.com/nlpodyssey/gdelt"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Info().Msg("getting latest GDELT events")

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
```

## Contributions

Contributions to this package are welcome.

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
