# metrics-proxy

Lightweight Go proxy that queries Thanos on a schedule and serves cached metrics as JSON for [joluc.de](https://joluc.de).

## What it does

- Queries Thanos `query_range` every 60s for a pool of homelab metrics
- Caches results in memory
- Serves `GET /metrics.json` with CORS headers
- Deployed at `metrics-proxy.joluc.de`

## Metrics served

- Water temperature (Islands Brygge Havnebad)
- Air temperature & wind (Copenhagen)
- Raspberry Pi CPU temperatures
- Active icebreakers (Baltic)
- Running pods
- Cluster CPU usage
- Network traffic

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `THANOS_URL` | `http://thanos-query-frontend.thanos.svc.cluster.local:9090` | Thanos query endpoint |
| `LISTEN_ADDR` | `:8080` | Listen address |
| `ALLOWED_ORIGINS` | — | Additional CORS origins |

## Development

```bash
go build ./cmd/metrics-proxy
THANOS_URL=http://localhost:9090 ./metrics-proxy
```
