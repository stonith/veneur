package veneur

//go:generate protoc --go_out=. ssf/sample.proto
//go:generate gojson -input example.yaml -o config.go -fmt yaml -pkg veneur -name Config
//go:generate gojson -input example-proxy.yaml -o config-proxy.go -fmt yaml -pkg veneur -name ProxyConfig
//TODO(aditya) reenable go:generate gojson -input fixtures/datadog_trace.json -o datadog_trace_span.go -fmt json -pkg veneur -name DatadogTraceSpan
