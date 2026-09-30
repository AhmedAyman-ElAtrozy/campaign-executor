// Package httpapi exposes liveness, readiness, and Prometheus metrics
// endpoints for the campaign executor.
package httpapi

import (
	"context"
	"net/http"
	"time"

	"campaign-executor/internal/registry"

	_ "campaign-executor/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// Server exposes /healthz, /readyz, and /metrics over HTTP.
type Server struct {
	httpServer   *http.Server
	reg          *registry.Registry
	redisClient  *redis.Client
	kafkaBrokers []string
}

// New builds a Server listening on port, using redisClient and
// kafkaBrokers[0] for readiness checks.
func New(port string, reg *registry.Registry, redisClient *redis.Client, kafkaBrokers []string) *Server {
	s := &Server{
		reg:          reg,
		redisClient:  redisClient,
		kafkaBrokers: kafkaBrokers,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", promhttp.Handler())

	s.httpServer = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	return s
}

// handleHealthz always reports the process is alive.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// handleReadyz reports ready only if both Redis and Kafka are reachable.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := s.redisClient.Ping(ctx).Err(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("redis check failed"))
		return
	}

	if len(s.kafkaBrokers) == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("kafka check failed: no brokers configured"))
		return
	}

	dialer := kafka.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", s.kafkaBrokers[0])
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("kafka check failed"))
		return
	}
	conn.Close()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ready"))
}

// Run starts serving HTTP until ctx is cancelled, then shuts down
// gracefully with a short timeout, returning nil on a clean shutdown.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.httpServer.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			return err
		}
		<-errCh
		return nil
	}
}
