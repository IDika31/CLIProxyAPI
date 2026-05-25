package helps

import (
	"context"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"io"
	"sync"
	"time"
)

const codexStreamFirstEventDefaultTimeout = 90 * time.Second

func CodexStreamFirstEventTimeout(cfg *config.Config) time.Duration {
	if cfg == nil {
		return codexStreamFirstEventDefaultTimeout
	}
	raw := cfg.CodexHeaderDefaults.StreamFirstEventTimeout
	if raw == "" {
		return codexStreamFirstEventDefaultTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return codexStreamFirstEventDefaultTimeout
	}
	return d
}

func WatchCodexFirstEvent(
	parentCtx context.Context,
	body io.Closer,
	firstEvent <-chan struct{},
	done <-chan struct{},
	timeout time.Duration,
) {
	if timeout <= 0 {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-firstEvent:
	case <-timer.C:
		_ = body.Close()
	case <-done:
	case <-parentCtx.Done():
	}
}

type ReadSignalerCloser struct {
	io.ReadCloser
	once      sync.Once
	firstRead chan<- struct{}
}

func NewReadSignalerCloser(rc io.ReadCloser, firstRead chan<- struct{}) *ReadSignalerCloser {
	return &ReadSignalerCloser{ReadCloser: rc, firstRead: firstRead}
}

func (r *ReadSignalerCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		r.once.Do(func() {
			select {
			case r.firstRead <- struct{}{}:
			default:
			}
		})
	}
	return n, err
}
