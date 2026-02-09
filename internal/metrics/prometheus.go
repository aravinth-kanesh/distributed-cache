package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aravinth/distributed-cache/internal/persistence"
	"github.com/aravinth/distributed-cache/internal/replication"
	"github.com/aravinth/distributed-cache/internal/store"
)

// ServerStats abstracts the server metrics we need. I use an interface
// here so the metrics package doesn't import the server package (which
// would create a circular dependency since handler imports metrics types).
type ServerStats interface {
	ActiveConnections() int
	TotalConnections() uint64
}

// Collector implements prometheus.Collector by pulling current values
// from the store, server, persistence, and replication subsystems on
// each scrape. Most of the heavy lifting is already done by existing
// atomic counters — I just expose them in Prometheus format.
type Collector struct {
	store      *store.ShardedMap
	server     ServerStats
	replState  *replication.ReplState
	master     *replication.MasterState
	snapEngine *persistence.SnapshotEngine
	startTime  time.Time

	// Descriptors
	uptime          *prometheus.Desc
	connsTotal      *prometheus.Desc
	connsActive     *prometheus.Desc
	keysTotal       *prometheus.Desc
	storeOps        *prometheus.Desc
	cacheHits       *prometheus.Desc
	cacheMisses     *prometheus.Desc
	cacheHitRatio   *prometheus.Desc
	keysExpired     *prometheus.Desc
	snapshotLastSave *prometheus.Desc
	snapshotSaving  *prometheus.Desc
	replRole        *prometheus.Desc
	replOffset      *prometheus.Desc
	replSlaves      *prometheus.Desc
}

// CommandCount and CommandDuration are registered directly (not via
// the custom Collector) because they're incremented in the hot path
// by Handler.Execute().
var (
	CommandCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dcache",
			Name:      "commands_total",
			Help:      "Total number of commands processed, partitioned by command name.",
		},
		[]string{"cmd"},
	)

	CommandDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dcache",
			Name:      "command_duration_seconds",
			Help:      "Command execution latency in seconds, partitioned by command name.",
			Buckets:   []float64{.000001, .000005, .00001, .00005, .0001, .0005, .001, .005, .01, .05, .1},
		},
		[]string{"cmd"},
	)
)

// NewCollector creates a Collector that scrapes live stats from each
// subsystem. All arguments are optional — pass nil for any component
// that is disabled.
func NewCollector(
	sm *store.ShardedMap,
	srv ServerStats,
	rs *replication.ReplState,
	master *replication.MasterState,
	snap *persistence.SnapshotEngine,
	startTime time.Time,
) *Collector {
	ns := "dcache"
	return &Collector{
		store:      sm,
		server:     srv,
		replState:  rs,
		master:     master,
		snapEngine: snap,
		startTime:  startTime,

		uptime:          prometheus.NewDesc(ns+"_uptime_seconds", "Seconds since server start.", nil, nil),
		connsTotal:      prometheus.NewDesc(ns+"_connections_total", "Total connections accepted since startup.", nil, nil),
		connsActive:     prometheus.NewDesc(ns+"_connections_active", "Currently connected clients.", nil, nil),
		keysTotal:       prometheus.NewDesc(ns+"_keys_total", "Number of keys in the store.", nil, nil),
		storeOps:        prometheus.NewDesc(ns+"_store_ops_total", "Total store-level operations.", []string{"op"}, nil),
		cacheHits:       prometheus.NewDesc(ns+"_cache_hits_total", "Total cache hits.", nil, nil),
		cacheMisses:     prometheus.NewDesc(ns+"_cache_misses_total", "Total cache misses.", nil, nil),
		cacheHitRatio:   prometheus.NewDesc(ns+"_cache_hit_ratio", "Cache hit ratio (0.0 to 1.0).", nil, nil),
		keysExpired:     prometheus.NewDesc(ns+"_keys_expired_total", "Total keys removed by lazy expiration.", nil, nil),
		snapshotLastSave: prometheus.NewDesc(ns+"_snapshot_last_save_timestamp", "Unix timestamp of last successful snapshot.", nil, nil),
		snapshotSaving:  prometheus.NewDesc(ns+"_snapshot_in_progress", "Whether a snapshot is currently being saved (1 or 0).", nil, nil),
		replRole:        prometheus.NewDesc(ns+"_replication_role", "Current replication role (1 = active).", []string{"role"}, nil),
		replOffset:      prometheus.NewDesc(ns+"_replication_offset", "Current replication stream offset in bytes.", nil, nil),
		replSlaves:      prometheus.NewDesc(ns+"_replication_slaves_connected", "Number of connected slave replicas.", nil, nil),
	}
}

// Describe sends all descriptor definitions to the channel.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.uptime
	ch <- c.connsTotal
	ch <- c.connsActive
	ch <- c.keysTotal
	ch <- c.storeOps
	ch <- c.cacheHits
	ch <- c.cacheMisses
	ch <- c.cacheHitRatio
	ch <- c.keysExpired
	ch <- c.snapshotLastSave
	ch <- c.snapshotSaving
	ch <- c.replRole
	ch <- c.replOffset
	ch <- c.replSlaves
}

// Collect pulls current values from each subsystem and sends them
// as Prometheus metrics. This runs on every scrape (~5-15s), not on
// the hot command path.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.uptime, prometheus.GaugeValue, time.Since(c.startTime).Seconds())

	// Server connection metrics
	if c.server != nil {
		ch <- prometheus.MustNewConstMetric(c.connsTotal, prometheus.GaugeValue, float64(c.server.TotalConnections()))
		ch <- prometheus.MustNewConstMetric(c.connsActive, prometheus.GaugeValue, float64(c.server.ActiveConnections()))
	}

	// Store metrics — these aggregate across all 256 shards
	if c.store != nil {
		m := c.store.GetMetrics()
		ch <- prometheus.MustNewConstMetric(c.keysTotal, prometheus.GaugeValue, float64(m["keys"]))
		ch <- prometheus.MustNewConstMetric(c.storeOps, prometheus.GaugeValue, float64(m["gets"]), "get")
		ch <- prometheus.MustNewConstMetric(c.storeOps, prometheus.GaugeValue, float64(m["sets"]), "set")
		ch <- prometheus.MustNewConstMetric(c.storeOps, prometheus.GaugeValue, float64(m["deletes"]), "delete")
		ch <- prometheus.MustNewConstMetric(c.cacheHits, prometheus.GaugeValue, float64(m["hits"]))
		ch <- prometheus.MustNewConstMetric(c.cacheMisses, prometheus.GaugeValue, float64(m["misses"]))
		ch <- prometheus.MustNewConstMetric(c.cacheHitRatio, prometheus.GaugeValue, c.store.HitRatio())
		ch <- prometheus.MustNewConstMetric(c.keysExpired, prometheus.GaugeValue, float64(m["expired_count"]))
	}

	// Snapshot metrics
	if c.snapEngine != nil {
		ch <- prometheus.MustNewConstMetric(c.snapshotLastSave, prometheus.GaugeValue, float64(c.snapEngine.LastSaveTime()))
		saving := 0.0
		if c.snapEngine.IsSaving() {
			saving = 1.0
		}
		ch <- prometheus.MustNewConstMetric(c.snapshotSaving, prometheus.GaugeValue, saving)
	}

	// Replication metrics
	if c.replState != nil {
		role := c.replState.Role()
		masterVal, slaveVal := 0.0, 0.0
		if role == replication.RoleMaster {
			masterVal = 1.0
		} else {
			slaveVal = 1.0
		}
		ch <- prometheus.MustNewConstMetric(c.replRole, prometheus.GaugeValue, masterVal, "master")
		ch <- prometheus.MustNewConstMetric(c.replRole, prometheus.GaugeValue, slaveVal, "slave")
		ch <- prometheus.MustNewConstMetric(c.replOffset, prometheus.GaugeValue, float64(c.replState.Offset()))
	}

	if c.master != nil {
		ch <- prometheus.MustNewConstMetric(c.replSlaves, prometheus.GaugeValue, float64(c.master.SlaveCount()))
	}
}

// Register registers the custom collector and the command-level
// metrics with Prometheus's default registry.
func Register(c *Collector) {
	prometheus.MustRegister(c)
	prometheus.MustRegister(CommandCount)
	prometheus.MustRegister(CommandDuration)
}
