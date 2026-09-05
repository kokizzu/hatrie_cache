package hatCommand

import "testing"

func TestCommandResponseCodeProtobufRoundTrip(t *testing.T) {
	response := Response{
		OK:      false,
		Message: "key is required",
		Code:    ErrorCodeInvalidArgument,
	}
	encoded := cacheCommandResponseToPooledProto(response)
	decoded := cacheCommandResponseFromProto(encoded)
	releaseCommandResponseProto(encoded)
	if decoded.Code != response.Code {
		t.Fatalf("decoded.Code = %q, want %q", decoded.Code, response.Code)
	}
	if decoded.Message != response.Message || decoded.OK != response.OK {
		t.Fatalf("decoded response = %#v, want %#v", decoded, response)
	}
}
