package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

type MetricConfig struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Unit  string `json:"unit"`
	Query string `json:"-"`
}

type DataPoint struct {
	Timestamp int64   `json:"t"`
	Value     float64 `json:"v"`
}

type MetricResult struct {
	ID     string      `json:"id"`
	Label  string      `json:"label"`
	Unit   string      `json:"unit"`
	Series []DataPoint `json:"series"`
}

type MetricsPayload struct {
	UpdatedAt int64          `json:"updated_at"`
	Metrics   []MetricResult `json:"metrics"`
}

var metrics = []MetricConfig{
	{ID: "water_islands_brygge", Label: "Water · Islands Brygge", Unit: "°C", Query: `badevand_water_temperature_celsius{site_id="badevand_Islands_Brygge_Havnebad"}`},
	{ID: "air_copenhagen", Label: "Air · Copenhagen", Unit: "°C", Query: `weather_temperature_celsius{city="Copenhagen"}`},
	{ID: "wind_copenhagen", Label: "Wind · Copenhagen", Unit: "m/s", Query: `weather_wind_speed_mps{city="Copenhagen"}`},
	{ID: "pi_temps", Label: "Pi Temps · Cluster", Unit: "°C", Query: `node_thermal_zone_temp{type="cpu-thermal"}`},
	{ID: "icebreakers_moving", Label: "Icebreakers · Baltic", Unit: "ships", Query: `count(icebreaker_speed_over_ground_knots > 0)`},
	{ID: "pods_running", Label: "Pods · Homelab", Unit: "pods", Query: `count(kube_pod_status_phase{phase="Running"})`},
	{ID: "cluster_cpu", Label: "CPU · Cluster", Unit: "%", Query: `100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])))`},
	{ID: "network_rx", Label: "Traffic In · Cluster", Unit: "MB/s", Query: `sum(rate(node_network_receive_bytes_total{device=~"eth.*|enp.*"}[5m])) / 1024 / 1024`},
}

type server struct {
	thanosURL string
	client    *http.Client
	mu        sync.RWMutex
	payload   []byte
	allowedOrigins []string
}

func main() {
	thanosURL := os.Getenv("THANOS_URL")
	if thanosURL == "" {
		thanosURL = "http://thanos-query-frontend.thanos.svc.cluster.local:9090"
	}

	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":8080"
	}

	allowedOrigins := []string{"https://joluc.de", "https://www.joluc.de"}
	if origins := os.Getenv("ALLOWED_ORIGINS"); origins != "" {
		allowedOrigins = append(allowedOrigins, origins)
	}

	s := &server{
		thanosURL: thanosURL,
		client:    &http.Client{Timeout: 30 * time.Second},
		allowedOrigins: allowedOrigins,
	}

	slog.Info("starting metrics-proxy", "thanos_url", thanosURL, "listen", listenAddr)

	// Initial fetch
	s.refresh()

	// Background refresh loop
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s.refresh()
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics.json", s.handleMetrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: listenAddr, Handler: mux}
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	for _, allowed := range s.allowedOrigins {
		if origin == allowed {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			break
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=30")

	s.mu.RLock()
	data := s.payload
	s.mu.RUnlock()

	if data == nil {
		http.Error(w, `{"error":"no data yet"}`, http.StatusServiceUnavailable)
		return
	}
	w.Write(data)
}

func (s *server) refresh() {
	now := time.Now().Unix()
	start := now - (3 * 3600) // 3 hours
	step := 300              // 5 minute resolution

	var results []MetricResult

	for _, m := range metrics {
		series, err := s.queryRange(m.Query, start, now, step)
		if err != nil {
			slog.Warn("query failed", "metric", m.ID, "error", err)
			continue
		}
		results = append(results, MetricResult{
			ID:     m.ID,
			Label:  m.Label,
			Unit:   m.Unit,
			Series: series,
		})
	}

	payload := MetricsPayload{
		UpdatedAt: now,
		Metrics:   results,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		slog.Error("marshal failed", "error", err)
		return
	}

	s.mu.Lock()
	s.payload = data
	s.mu.Unlock()

	slog.Info("refreshed metrics", "count", len(results))
}

type prometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Values [][]interface{}   `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func (s *server) queryRange(query string, start, end int64, step int) ([]DataPoint, error) {
	u := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=%d",
		s.thanosURL, url.QueryEscape(query), start, end, step)

	resp, err := s.client.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var pr prometheusResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, err
	}

	if pr.Status != "success" || len(pr.Data.Result) == 0 {
		return nil, fmt.Errorf("no data")
	}

	// For multi-series (like pi_temps), average them
	// For single series, just take the first
	if len(pr.Data.Result) == 1 {
		return extractSeries(pr.Data.Result[0].Values), nil
	}

	// Average across all series (e.g., multiple Pi temps → one line)
	return averageSeries(pr.Data.Result), nil
}

func extractSeries(values [][]interface{}) []DataPoint {
	points := make([]DataPoint, 0, len(values))
	for _, v := range values {
		if len(v) < 2 {
			continue
		}
		ts, ok := v[0].(float64)
		if !ok {
			continue
		}
		val := parseFloat(v[1])
		points = append(points, DataPoint{Timestamp: int64(ts), Value: val})
	}
	return points
}

func averageSeries(results []struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values"`
}) []DataPoint {
	// Build map of timestamp → sum of values and count
	type acc struct {
		sum   float64
		count int
	}
	byTS := make(map[int64]*acc)
	var order []int64

	for _, r := range results {
		for _, v := range r.Values {
			if len(v) < 2 {
				continue
			}
			ts := int64(v[0].(float64))
			val := parseFloat(v[1])
			if _, exists := byTS[ts]; !exists {
				byTS[ts] = &acc{}
				order = append(order, ts)
			}
			byTS[ts].sum += val
			byTS[ts].count++
		}
	}

	points := make([]DataPoint, 0, len(order))
	for _, ts := range order {
		a := byTS[ts]
		points = append(points, DataPoint{Timestamp: ts, Value: a.sum / float64(a.count)})
	}
	return points
}

func parseFloat(v interface{}) float64 {
	switch val := v.(type) {
	case string:
		var f float64
		fmt.Sscanf(val, "%f", &f)
		return f
	case float64:
		return val
	default:
		return 0
	}
}
