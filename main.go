package main
import (
	"flag"
	"os"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	listenAddress		= flag.String("web.listen-address", ":9121", "Address to listen on for web interface and telemetry.")
	metricPath		= flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	configFile       	= flag.String("config", "/config.json", "config file location.")
	burrowEndpoint          = flag.String("burrow-endpoint", "127.0.0.1:8080", "Burrow http endpoint")
)

type burrow_config map[string][]string

type burrow struct {
	config	 burrow_config
	maxlag   *prometheus.Desc
	totallag *prometheus.Desc
}

type StatusResponse struct {
	Error bool
	Message string
	Status Status
}

type Status struct {
	Cluster    string
	Group      string
	Status     string
	Complete   bool
	Maxlag     Lag
	Partitions []Lag
	Totallag   int64 
}

type Lag struct {
	Topic     string
	Partition int32
	Status    string
	Start     Partition
	End       Partition
}

type Partition struct {
	Offset int64
	Timestamp int64
	Lag int64
}

func (b *burrow) Describe(ch chan<- *prometheus.Desc) {
	ch <- b.maxlag
	ch <- b.totallag
	
}

func (b *burrow) Collect(ch chan<- prometheus.Metric) {
	b.collectBurrowMetrics(ch)
}

func (b *burrow) collectBurrowMetrics(ch chan<- prometheus.Metric) {
	for  cluster, groups := range b.config {
		for _, group := range groups {
			resp, err := http.Get("http://" + *burrowEndpoint + "/v2/kafka/" + cluster + "/consumer/" + group + "/lag")
			if err == nil {
				defer resp.Body.Close()
				var t StatusResponse 
				body, _ := ioutil.ReadAll(resp.Body)			
				json.Unmarshal(body, &t)
				if t.Error == false {
					ch <- prometheus.MustNewConstMetric(b.maxlag, prometheus.GaugeValue, float64(t.Status.Maxlag.End.Lag), cluster, t.Status.Maxlag.Topic, t.Status.Group)
					ch <- prometheus.MustNewConstMetric(b.totallag, prometheus.GaugeValue, float64(t.Status.Totallag), cluster, t.Status.Group)
				} else {
					log.Printf("Cannot retrieve information for group: %s\n", group)
				}				
			} else {
				log.Printf("Cannot retrieve information for group: %s\n", group)
			}
		}
	}
}


func newBurrowExporter(configfile string) *burrow {
	var b burrow
	b.maxlag = prometheus.NewDesc("kafka_max_lag", "Maximum lag", []string{"cluster", "topic", "group"}, nil)
	b.totallag = prometheus.NewDesc("kafka_total_lag", "Total lag of all partition for a specific group", []string{"cluster", "group"}, nil)
	file, e := ioutil.ReadFile(configfile)
	if e != nil {
		log.Fatal("File error: %v\n", e)
		os.Exit(1)
	}
	err := json.Unmarshal(file, &b.config)
	if err != nil {
		log.Fatal("Cannot parse confile file: %v\n", err)
		os.Exit(1)
	}
	return &b
}

func main() {
	flag.Parse()
	b := newBurrowExporter(*configFile)
	prometheus.MustRegister(b)
	http.Handle(*metricPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		       <head><title>Burrow exporter</title></head>
		       <body>
		       <h1>Burrow exporter</h1>
		       <p><a href='` + *metricPath + `'>Metrics</a></p>
		       </body>
		       </html>`))
	})
	log.Printf("providing metrics at %s%s", *listenAddress, *metricPath)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
