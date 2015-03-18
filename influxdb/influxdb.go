package influxdb

import (
	"fmt"
	"log"
	"net/url"
	"time"

	client "github.com/influxdb/influxdb/client"
	"github.com/rcrowley/go-metrics"
)

type Config struct {
	Host     string
	Database string
	Username string
	Password string
}

func Influxdb(r metrics.Registry, d time.Duration, config *Config) {
	urlDB, err := url.Parse(config.Host)
	if err != nil {
		log.Println(err)
		return
	}
	client, err := client.NewClient(client.Config{
		URL:      *urlDB,
		Username: config.Username,
		Password: config.Password,
	})
	if err != nil {
		log.Println(err)
		return
	}

	for _ = range time.Tick(d) {
		if err := send(r, config.Database, client); err != nil {
			log.Println(err)
		}
	}
}

func send(r metrics.Registry, database string, c *client.Client) error {
	var series []client.Point

	type m map[string]interface{}

	r.Each(func(name string, i interface{}) {
		now := time.Now() // getCurrentTime()
		switch metric := i.(type) {
		case metrics.Counter:
			series = append(series, client.Point{
				Timestamp: now,
				Name:      fmt.Sprintf("%s.count", name),
				Fields: m{
					"count": metric.Count(),
					"time":  now,
				},
			})

		case metrics.Gauge:
			series = append(series, client.Point{
				Name:      fmt.Sprintf("%s.value", name),
				Timestamp: now,
				Fields: m{
					"value": metric.Value(),
				},
			})
		case metrics.GaugeFloat64:
			series = append(series, client.Point{
				Name:      fmt.Sprintf("%s.value", name),
				Timestamp: now,
				Fields: m{
					"value": metric.Value(),
				},
			})
		case metrics.Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			series = append(series, client.Point{
				Name:      fmt.Sprintf("%s.histogram", name),
				Timestamp: now,
				Fields: m{
					"count":          h.Count(),
					"min":            h.Min(),
					"max":            h.Max(),
					"mean":           h.Mean(),
					"std-dev":        h.StdDev(),
					"50-percentile":  ps[0],
					"75-percentile":  ps[1],
					"95-percentile":  ps[2],
					"99-percentile":  ps[3],
					"999-percentile": ps[4],
				},
			})
		case metrics.Meter:
			snap := metric.Snapshot()
			series = append(series, client.Point{
				Name:      fmt.Sprintf("%s.meter", name),
				Timestamp: now,
				Fields: m{
					"count":          snap.Count(),
					"one-minute":     snap.Rate1(),
					"five-minute":    snap.Rate5(),
					"fifteen-minute": snap.Rate15(),
					"mean":           snap.RateMean(),
				},
			})
		case metrics.Timer:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			series = append(series, client.Point{
				Name: fmt.Sprintf("%s.timer", name),
				Fields: m{
					"count":          h.Count(),
					"min":            h.Min(),
					"max":            h.Max(),
					"mean":           h.Mean(),
					"std-dev":        h.StdDev(),
					"50-percentile":  ps[0],
					"75-percentile":  ps[1],
					"95-percentile":  ps[2],
					"99-percentile":  ps[3],
					"999-percentile": ps[4],
					"one-minute":     h.Rate1(),
					"five-minute":    h.Rate5(),
					"fifteen-minute": h.Rate15(),
					"mean-rate":      h.RateMean()},
			})
		}
	})

	bpoints := client.BatchPoints{
		Points:   series,
		Database: database,
		//	RetentionPolicy string            `json:"retentionPolicy,omitempty"`
		//	Tags            map[string]string `json:"tags,omitempty"`
		//  Timestamp: now,
		Precision: "ms",
	}

	if _, err := c.Write(bpoints); err != nil {
		log.Println(err)
	}
	return nil
}

func getCurrentTime() int64 {
	return time.Now().UnixNano() / 1000000
}
