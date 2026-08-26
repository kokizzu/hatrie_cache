package hatMonitoring

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestProfileLimitedWriterStopsAtLimit(t *testing.T) {
	var output bytes.Buffer
	writer := NewProfileLimitedWriter(&output, 4)
	written, err := writer.Write([]byte("123456"))
	if !errors.Is(err, ErrProfileTooLarge) {
		t.Fatalf("Write() error = %v, want profile too large", err)
	}
	if written != 4 || output.String() != "1234" || writer.Remaining() != 0 {
		t.Fatalf("Write() = %d bytes, output %q, remaining %d", written, output.String(), writer.Remaining())
	}
}

func TestValidateProfileRequest(t *testing.T) {
	profileType, duration, err := ValidateProfileRequest(ProfileRequest{Type: "CPU", DurationMillis: 1000})
	if err != nil || profileType != "cpu" || duration != time.Second {
		t.Fatalf("ValidateProfileRequest(cpu) = %q/%s/%v", profileType, duration, err)
	}
	for _, request := range []ProfileRequest{
		{Type: "cpu"},
		{Type: "heap", DurationMillis: 1},
		{Type: "mutex"},
	} {
		if _, _, err := ValidateProfileRequest(request); err == nil {
			t.Fatalf("ValidateProfileRequest(%#v) error = nil", request)
		}
	}
}
