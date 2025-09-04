package monitor

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	TagName  = "task_name"
	ChainTag = "chain_name"
)

func NewMetricCronWorkStatus() *prometheus.GaugeVec {
	syncWorkStatusMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ibc_explorer_backend",
			Subsystem: "",
			Name:      "cron_task_status",
			Help:      "ibc_explorer_backend cron task working status (1:Normal  -1:UNormal)",
		},
		[]string{TagName},
	)
	return syncWorkStatusMetric
}

func NewMetricRedisStatus() prometheus.Gauge {
	redisNodeStatusMetric := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ibc_explorer_backend",
			Subsystem: "",
			Name:      "redis_connection_status",
			Help:      "ibc_explorer_backend  node connection status of redis service (1:Normal  -1:UNormal)",
		},
	)
	return redisNodeStatusMetric
}

func NewMetricLcdStatus() *prometheus.GaugeVec {
	lcdConnectionStatusMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ibc_explorer_backend",
			Subsystem: "",
			Name:      "lcd_connection_status",
			Help:      "ibc_explorer_backend  lcd connection status of blockchain (1:Normal  -1:UNormal)",
		},
		[]string{ChainTag},
	)
	return lcdConnectionStatusMetric
}
