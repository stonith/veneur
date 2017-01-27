package veneur

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/Sirupsen/logrus"
	raven "github.com/getsentry/raven-go"
	"github.com/hashicorp/consul/api"
	"github.com/pkg/profile"
	"github.com/stripe/veneur/discovery"
	"github.com/stripe/veneur/samplers"
	"github.com/stripe/veneur/trace"
	"github.com/zenazn/goji/bind"
	"github.com/zenazn/goji/graceful"
	"stathat.com/c/consistent"

	"goji.io"
	"goji.io/pat"
)

type Proxy struct {
	statsd *statsd.Client
	sentry *raven.Client

	Hostname               string
	ForwardDestinations    *consistent.Consistent
	TraceDestinations      *consistent.Consistent
	Discoverer             discovery.Discoverer
	ConsulForwardService   string
	ConsulTraceService     string
	ConsulInterval         time.Duration
	ForwardDestinationsMtx sync.Mutex
	TraceDestinationsMtx   sync.Mutex
	HTTPAddr               string
	HTTPClient             *http.Client

	enableProfiling bool
}

func NewProxyFromConfig(conf ProxyConfig) (ret Proxy, err error) {

	hostname, err := os.Hostname()
	if err != nil {
		log.WithError(err).Error("Error finding hostname")
		return
	}
	ret.Hostname = hostname

	log.Hooks.Add(sentryHook{
		c:        ret.sentry,
		hostname: hostname,
		lv: []logrus.Level{
			logrus.ErrorLevel,
			logrus.FatalLevel,
			logrus.PanicLevel,
		},
	})

	ret.HTTPAddr = conf.HTTPAddress
	// TODO Timeout?
	ret.HTTPClient = &http.Client{}

	ret.enableProfiling = conf.EnableProfiling

	ret.ConsulForwardService = conf.ConsulForwardServiceName
	ret.ConsulTraceService = conf.ConsulTraceServiceName

	ret.ForwardDestinations = consistent.New()
	ret.TraceDestinations = consistent.New()

	ret.ConsulInterval, err = time.ParseDuration(conf.ConsulRefreshInterval)
	if err != nil {
		log.WithError(err).Error("Error parsing Consul refresh interval")
		return
	}
	log.WithField("interval", conf.ConsulRefreshInterval).Info("Will use Consul for service discovery")

	// TODO Size of replicas in config?
	//ret.ForwardDestinations.NumberOfReplicas = ???

	return
}

func (p Proxy) GetHostname() string {
	return p.Hostname
}

func (p Proxy) GetSentry() *raven.Client {
	return p.sentry
}

func (p Proxy) GetStats() *statsd.Client {
	return p.statsd
}

func (p Proxy) GetHTTPClient() *http.Client {
	return p.HTTPClient
}

func (p *Proxy) Start() {
	log.WithField("version", VERSION).Info("Starting server")

	config := api.DefaultConfig()
	// Use the same HTTP Client we're using for other things, so we can leverage
	// it for testing.
	config.HttpClient = p.HTTPClient
	disc, consulErr := discovery.NewConsul(config)
	if consulErr != nil {
		log.WithError(consulErr).Error("Error creating Consul discoverer")
		return
	}
	p.Discoverer = disc

	p.RefreshDestinations(p.ConsulForwardService, p.ForwardDestinations, &p.ForwardDestinationsMtx)
	if len(p.ForwardDestinations.Members()) == 0 {
		log.WithField("serviceName", p.ConsulForwardService).Error("Refusing to start with zero destinations for forwarding.")
	}

	// Else use Consul
	p.RefreshDestinations(p.ConsulTraceService, p.TraceDestinations, &p.TraceDestinationsMtx)
	if len(p.ForwardDestinations.Members()) == 0 {
		log.WithField("serviceName", p.ConsulTraceService).Error("Refusing to start with zero destinations for tracing.")
	}

	go func() {
		defer func() {
			ConsumePanic(p, recover())
		}()
		ticker := time.NewTicker(p.ConsulInterval)
		for range ticker.C {
			if p.ConsulForwardService != "" {
				p.RefreshDestinations(p.ConsulForwardService, p.ForwardDestinations, &p.ForwardDestinationsMtx)
			}
			if p.ConsulTraceService != "" {
				p.RefreshDestinations(p.ConsulTraceService, p.TraceDestinations, &p.TraceDestinationsMtx)
			}
		}
	}()
}

// HTTPServe starts the HTTP server and listens perpetually until it encounters an unrecoverable error.
func (p *Proxy) HTTPServe() {
	var prf interface {
		Stop()
	}

	// We want to make sure the profile is stopped
	// exactly once (and only once), even if the
	// shutdown pre-hook does not run (which it may not)
	profileStopOnce := sync.Once{}

	if p.enableProfiling {
		profileStartOnce.Do(func() {
			prf = profile.Start()
		})

		defer func() {
			profileStopOnce.Do(prf.Stop)
		}()
	}
	httpSocket := bind.Socket(p.HTTPAddr)
	graceful.Timeout(10 * time.Second)
	graceful.PreHook(func() {

		if prf != nil {
			profileStopOnce.Do(prf.Stop)
		}

		log.Info("Terminating HTTP listener")
	})

	// Ensure that the server responds to SIGUSR2 even
	// when *not* running under einhorn.
	graceful.AddSignal(syscall.SIGUSR2, syscall.SIGHUP)
	graceful.HandleSignals()
	log.WithField("address", p.HTTPAddr).Info("HTTP server listening")
	bind.Ready()

	if err := graceful.Serve(httpSocket, p.Handler()); err != nil {
		log.WithError(err).Error("HTTP server shut down due to error")
	}

	graceful.Shutdown()
}

// RefreshDestinations updates the server's list of valid destinations
// for flushing. This should be called periodically to ensure we have
// the latest data.
func (p *Proxy) RefreshDestinations(serviceName string, ring *consistent.Consistent, mtx *sync.Mutex) {

	start := time.Now()
	destinations, err := p.Discoverer.UpdateDestinations(serviceName)
	p.statsd.TimeInMilliseconds("discoverer.update_duration_ns", float64(time.Since(start).Nanoseconds()), []string{fmt.Sprintf("service:%s", serviceName)}, 1.0)
	if err != nil || len(destinations) == 0 {
		log.WithError(err).Error("Discoverer returned an error, destinations may be stale!")
		p.statsd.Incr("discoverer.errors", []string{fmt.Sprintf("service:%s", serviceName)}, 1.0)
		// Return since we got no hosts. We don't want to zero out the list. This
		// should result in us leaving the "last good" values in the ring.
		return
	}

	// At the last moment, lock the mutex and defer so we unlock after setting.
	// We do this after we've fetched info so we don't hold the lock during long
	// queries, timeouts or errors. The flusher can lock the mutex and prevent us
	// from updating at the same time.
	mtx.Lock()
	defer mtx.Unlock()
	ring.Set(destinations)
	p.statsd.Gauge("discoverer.destination_number", float64(len(destinations)), []string{fmt.Sprintf("service:%s", serviceName)}, 1.0)
}

// Handler returns the Handler responsible for routing request processing.
func (p *Proxy) Handler() http.Handler {
	mux := goji.NewMux()

	mux.HandleFuncC(pat.Get("/healthcheck"), func(c context.Context, w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})

	mux.Handle(pat.Post("/import"), handleProxy(p))

	mux.Handle(pat.Get("/debug/pprof/cmdline"), http.HandlerFunc(pprof.Cmdline))
	mux.Handle(pat.Get("/debug/pprof/profile"), http.HandlerFunc(pprof.Profile))
	mux.Handle(pat.Get("/debug/pprof/symbol"), http.HandlerFunc(pprof.Symbol))
	mux.Handle(pat.Get("/debug/pprof/trace"), http.HandlerFunc(pprof.Trace))
	// TODO match without trailing slash as well
	mux.Handle(pat.Get("/debug/pprof/*"), http.HandlerFunc(pprof.Index))

	return mux
}

// ProxyMetrics takes a sliceof JSONMetrics and breaks them up into multiple
// HTTP requests by MetricKey using the hash ring.
func (p *Proxy) ProxyMetrics(ctx context.Context, jsonMetrics []samplers.JSONMetric) {
	span, _ := trace.StartSpanFromContext(ctx, "veneur.opentracing.proxy.proxy_metrics")
	defer span.Finish()

	jsonMetricsByDestination := make(map[string][]samplers.JSONMetric)
	for _, h := range p.ForwardDestinations.Members() {
		jsonMetricsByDestination[h] = make([]samplers.JSONMetric, 0)
	}

	for _, jm := range jsonMetrics {
		dest, _ := p.ForwardDestinations.Get(jm.MetricKey.String())
		jsonMetricsByDestination[dest] = append(jsonMetricsByDestination[dest], jm)
	}

	for dest, batch := range jsonMetricsByDestination {
		// always re-resolve the host to avoid dns caching
		log.WithField("destination", dest).Debug("Beginning flush forward")
		dnsStart := time.Now()
		endpoint, err := resolveEndpoint(fmt.Sprintf("%s/import", dest))
		if err != nil {
			// not a fatal error if we fail
			// we'll just try to use the host as it was given to us
			p.GetStats().Count("forward.error_total", 1, []string{"cause:dns"}, 1.0)
			log.WithError(err).Warn("Could not re-resolve host for forward")
		}
		p.GetStats().TimeInMilliseconds("forward.duration_ns", float64(time.Since(dnsStart).Nanoseconds()), []string{"part:dns"}, 1.0)

		// the error has already been logged (if there was one), so we only care
		// about the success case
		if postHelper(context.TODO(), p, endpoint, batch, "forward", true) == nil {
			log.WithField("metrics", len(batch)).Info("Completed forward to upstream Veneur")
		}
	}
}

// Shutdown signals the server to shut down after closing all
// current connections.
func (p *Proxy) Shutdown() {
	log.Info("Shutting down server gracefully")
	graceful.Shutdown()
}
