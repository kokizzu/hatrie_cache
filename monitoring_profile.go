package hatriecache

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"
)

const (
	monitoringProfileContentType  = "application/octet-stream"
	minCPUProfileDuration         = time.Second
	maxCPUProfileDuration         = 30 * time.Second
	maxMonitoringProfileBytes     = 256 << 20
	monitoringProfileErrorTrailer = "X-Hatrie-Profile-Error"
)

var errMonitoringProfileTooLarge = errors.New("hatriecache: profile exceeds 256 MiB response limit")

type monitoringProfileCaptureState struct {
	active atomic.Bool
}

type monitoringProfileRequest struct {
	Type           string `json:"type"`
	DurationMillis int64  `json:"duration_millis,omitempty"`
}

type monitoringProfileLimitedWriter struct {
	writer    io.Writer
	remaining int64
	err       error
}

func (writer *monitoringProfileLimitedWriter) Write(data []byte) (int, error) {
	if writer.err != nil {
		return 0, writer.err
	}
	if int64(len(data)) > writer.remaining {
		data = data[:writer.remaining]
		writer.err = errMonitoringProfileTooLarge
	}
	written, err := writer.writer.Write(data)
	writer.remaining -= int64(written)
	if err != nil {
		writer.err = err
		return written, err
	}
	if writer.err != nil {
		return written, writer.err
	}
	return written, nil
}

func (handler *MonitoringHandler) handleProfile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if !handler.authTokens.configured() {
		writeJSONStatus(w, http.StatusServiceUnavailable, commandError("diagnostics profiling requires a monitoring operator auth token"))
		return
	}
	request, ok := decodeMonitoringProfileRequest(w, r)
	if !ok {
		return
	}
	profileType, duration, err := validateMonitoringProfileRequest(request)
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, commandError(err.Error()))
		return
	}
	details := map[string]interface{}{"type": profileType}
	if duration > 0 {
		details["duration_millis"] = duration.Milliseconds()
	}
	if handler.options.RateLimiter != nil && !handler.options.RateLimiter.Allow(monitoringRateLimitKey(r)) {
		handler.options.Metrics.RecordRateLimitRejection()
		handler.auditHTTP(r, AuditEvent{Action: "profile.capture", OK: false, Status: http.StatusTooManyRequests, Message: "rate limit exceeded", Details: details})
		writeJSONStatus(w, http.StatusTooManyRequests, commandError("rate limit exceeded"))
		return
	}
	if !handler.profileCapture.active.CompareAndSwap(false, true) {
		handler.auditHTTP(r, AuditEvent{Action: "profile.capture", OK: false, Status: http.StatusConflict, Message: "profile capture is already running", Details: details})
		writeJSONStatus(w, http.StatusConflict, commandError("profile capture is already running"))
		return
	}
	defer handler.profileCapture.active.Store(false)

	w.Header().Set("Content-Type", monitoringProfileContentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, profileType+".pprof"))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Trailer", monitoringProfileErrorTrailer)
	limited := &monitoringProfileLimitedWriter{writer: w, remaining: maxMonitoringProfileBytes}
	err = captureMonitoringProfile(r, limited, profileType, duration)
	status := http.StatusOK
	if err != nil {
		status = http.StatusInternalServerError
		if errors.Is(err, errMonitoringProfileTooLarge) {
			status = http.StatusRequestEntityTooLarge
		} else if r.Context().Err() != nil && errors.Is(err, r.Context().Err()) {
			status = http.StatusRequestTimeout
		} else if strings.Contains(err.Error(), "CPU profiling already in use") {
			status = http.StatusConflict
		}
	}
	handler.auditHTTP(r, AuditEvent{Action: "profile.capture", OK: err == nil, Status: status, Message: monitoringProfileErrorMessage(err), Details: details})
	if err != nil && limited.remaining == maxMonitoringProfileBytes {
		w.Header().Del("Trailer")
		writeJSONStatus(w, status, commandError(err.Error()))
	} else if err != nil {
		w.Header().Set(monitoringProfileErrorTrailer, err.Error())
	}
}

func decodeMonitoringProfileRequest(w http.ResponseWriter, r *http.Request) (monitoringProfileRequest, bool) {
	decoder, closeBody, bodyTooLarge, ok := monitoringJSONDecoder(w, r)
	if !ok {
		return monitoringProfileRequest{}, false
	}
	defer closeBody()
	decoder.DisallowUnknownFields()
	var request monitoringProfileRequest
	if err := decoder.Decode(&request); err != nil {
		writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid profile request")
		return monitoringProfileRequest{}, false
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid profile request")
		return monitoringProfileRequest{}, false
	}
	if writeMonitoringRequestTooLarge(w, bodyTooLarge()) {
		return monitoringProfileRequest{}, false
	}
	return request, true
}

func validateMonitoringProfileRequest(request monitoringProfileRequest) (string, time.Duration, error) {
	profileType := strings.ToLower(strings.TrimSpace(request.Type))
	switch profileType {
	case "cpu":
		if request.DurationMillis < minCPUProfileDuration.Milliseconds() || request.DurationMillis > maxCPUProfileDuration.Milliseconds() {
			return "", 0, fmt.Errorf("CPU profile duration_millis must be between %d and %d", minCPUProfileDuration.Milliseconds(), maxCPUProfileDuration.Milliseconds())
		}
		duration := time.Duration(request.DurationMillis) * time.Millisecond
		return profileType, duration, nil
	case "heap", "goroutine":
		if request.DurationMillis != 0 {
			return "", 0, fmt.Errorf("%s profile does not accept duration_millis", profileType)
		}
		return profileType, 0, nil
	default:
		return "", 0, errors.New("profile type must be cpu, heap, or goroutine")
	}
}

func captureMonitoringProfile(r *http.Request, writer *monitoringProfileLimitedWriter, profileType string, duration time.Duration) error {
	switch profileType {
	case "cpu":
		if err := pprof.StartCPUProfile(writer); err != nil {
			return err
		}
		timer := time.NewTimer(duration)
		select {
		case <-timer.C:
		case <-r.Context().Done():
			timer.Stop()
			pprof.StopCPUProfile()
			if writer.err != nil {
				return writer.err
			}
			return r.Context().Err()
		}
		pprof.StopCPUProfile()
	case "heap", "goroutine":
		profile := pprof.Lookup(profileType)
		if profile == nil {
			return fmt.Errorf("runtime profile %q is unavailable", profileType)
		}
		if err := profile.WriteTo(writer, 0); err != nil {
			return err
		}
	}
	return writer.err
}

func monitoringProfileErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
