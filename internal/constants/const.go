package constants

import "time"

const (
	QueueNameIN          string        = "payment-queue"
	QueueNameRetry       string        = "payment-retry-queue"
	QueueNameOutDefault  string        = "payment-result-default"
	QueueNameOutFallback string        = "payment-result-fallback"
	PaymentDefaultURL    string        = "http://payment-processor-default:8080"
	PaymentFallbackURL   string        = "http://payment-processor-fallback:8080"
	CacheKey             string        = "BestInstance"
	UpdateFreq           time.Duration = 5
	TTL                  time.Duration = 5
)
