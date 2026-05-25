package helps

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

type closerSpy struct {
	io.ReadCloser
	closed atomic.Bool
}

func (c *closerSpy) Close() error {
	c.closed.Store(true)
	return c.ReadCloser.Close()
}

func newCloserSpy() *closerSpy {
	return &closerSpy{ReadCloser: io.NopCloser(strings.NewReader(""))}
}

func TestCodexStreamFirstEventTimeout_NilConfig(t *testing.T) {
	if got := CodexStreamFirstEventTimeout(nil); got != codexStreamFirstEventDefaultTimeout {
		t.Errorf("want %v, got %v", codexStreamFirstEventDefaultTimeout, got)
	}
}

func TestCodexStreamFirstEventTimeout_EmptyField(t *testing.T) {
	cfg := &config.Config{}
	if got := CodexStreamFirstEventTimeout(cfg); got != codexStreamFirstEventDefaultTimeout {
		t.Errorf("want %v, got %v", codexStreamFirstEventDefaultTimeout, got)
	}
}

func TestCodexStreamFirstEventTimeout_CustomDuration(t *testing.T) {
	cfg := &config.Config{}
	cfg.CodexHeaderDefaults.StreamFirstEventTimeout = "2m"
	if got := CodexStreamFirstEventTimeout(cfg); got != 2*time.Minute {
		t.Errorf("want 2m, got %v", got)
	}
}

func TestCodexStreamFirstEventTimeout_ZeroDisables(t *testing.T) {
	cfg := &config.Config{}
	cfg.CodexHeaderDefaults.StreamFirstEventTimeout = "0s"
	if got := CodexStreamFirstEventTimeout(cfg); got != 0 {
		t.Errorf("want 0, got %v", got)
	}
}

func TestCodexStreamFirstEventTimeout_InvalidFallsBack(t *testing.T) {
	cfg := &config.Config{}
	cfg.CodexHeaderDefaults.StreamFirstEventTimeout = "not-a-duration"
	if got := CodexStreamFirstEventTimeout(cfg); got != codexStreamFirstEventDefaultTimeout {
		t.Errorf("want default %v, got %v", codexStreamFirstEventDefaultTimeout, got)
	}
}

func TestWatchCodexFirstEvent_ClosesBodyOnTimeout(t *testing.T) {
	spy := newCloserSpy()
	firstEvent := make(chan struct{}, 1)
	done := make(chan struct{})
	go WatchCodexFirstEvent(context.Background(), spy, firstEvent, done, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	if !spy.closed.Load() {
		t.Error("expected body to be closed after timeout")
	}
	close(done)
}

func TestWatchCodexFirstEvent_DoesNotCloseBodyWhenFirstEventArrives(t *testing.T) {
	spy := newCloserSpy()
	firstEvent := make(chan struct{}, 1)
	done := make(chan struct{})
	go WatchCodexFirstEvent(context.Background(), spy, firstEvent, done, 200*time.Millisecond)
	firstEvent <- struct{}{}
	time.Sleep(50 * time.Millisecond)
	if spy.closed.Load() {
		t.Error("body should NOT be closed when first event arrives before timeout")
	}
	close(done)
}

func TestWatchCodexFirstEvent_DoesNotCloseBodyWhenDoneClosed(t *testing.T) {
	spy := newCloserSpy()
	firstEvent := make(chan struct{}, 1)
	done := make(chan struct{})
	go WatchCodexFirstEvent(context.Background(), spy, firstEvent, done, 200*time.Millisecond)
	close(done)
	time.Sleep(50 * time.Millisecond)
	if spy.closed.Load() {
		t.Error("body should NOT be closed when stream finishes before timeout")
	}
}

func TestWatchCodexFirstEvent_ZeroTimeoutDisabled(t *testing.T) {
	spy := newCloserSpy()
	firstEvent := make(chan struct{}, 1)
	done := make(chan struct{})
	go WatchCodexFirstEvent(context.Background(), spy, firstEvent, done, 0)
	time.Sleep(50 * time.Millisecond)
	if spy.closed.Load() {
		t.Error("body should NOT be closed when timeout is 0")
	}
	close(done)
}

func TestReadSignalerCloser_SignalsOnFirstRead(t *testing.T) {
	ch := make(chan struct{}, 1)
	body := io.NopCloser(strings.NewReader("hello"))
	r := NewReadSignalerCloser(body, ch)
	buf := make([]byte, 5)
	n, err := r.Read(buf)
	if n == 0 || err != nil {
		t.Fatalf("unexpected read result: n=%d err=%v", n, err)
	}
	select {
	case <-ch:
	default:
		t.Error("expected signal on first read")
	}
}

func TestReadSignalerCloser_SignalsOnlyOnce(t *testing.T) {
	ch := make(chan struct{}, 2)
	body := io.NopCloser(strings.NewReader("hello world"))
	r := NewReadSignalerCloser(body, ch)
	buf := make([]byte, 5)
	r.Read(buf)
	r.Read(buf)
	if len(ch) != 1 {
		t.Errorf("expected exactly 1 signal, got %d", len(ch))
	}
}

func TestReadSignalerCloser_NoSignalOnZeroRead(t *testing.T) {
	ch := make(chan struct{}, 1)
	body := io.NopCloser(strings.NewReader(""))
	r := NewReadSignalerCloser(body, ch)
	buf := make([]byte, 5)
	r.Read(buf)
	select {
	case <-ch:
		t.Error("should not signal on zero-byte read")
	default:
	}
}