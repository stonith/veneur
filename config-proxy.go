package veneur

type ProxyConfig struct {
	ConsulForwardServiceName string `yaml:"consul_forward_service_name"`
	ConsulRefreshInterval    string `yaml:"consul_refresh_interval"`
	ConsulTraceServiceName   string `yaml:"consul_trace_service_name"`
	Debug                    bool   `yaml:"debug"`
	EnableProfiling          bool   `yaml:"enable_profiling"`
	ForwardAddress           string `yaml:"forward_address"`
	HTTPAddress              string `yaml:"http_address"`
	SentryDsn                string `yaml:"sentry_dsn"`
	TraceAddress             string `yaml:"trace_address"`
	TraceAPIAddress          string `yaml:"trace_api_address"`
}
