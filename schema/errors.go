package schema

import "fmt"

// RegistryError reports a schema registry transport, status, or response
// error.
//
// Returned by [RegistryClient] when a request cannot be sent, the registry
// answers with a non-2xx status, or the response body cannot be parsed.
// StatusCode is the HTTP status, or -1 when no status was received.
type RegistryError struct {
	// StatusCode is the HTTP status code, or -1 for a transport or parsing
	// error.
	StatusCode int

	message string
	cause   error
}

func registryError(message string, cause error) *RegistryError {
	return &RegistryError{StatusCode: -1, message: message, cause: cause}
}

func registryStatusError(statusCode int, body string) *RegistryError {
	return &RegistryError{
		StatusCode: statusCode,
		message:    fmt.Sprintf("schema registry returned HTTP %d: %s", statusCode, body),
	}
}

// Error returns a description of the failure.
func (e *RegistryError) Error() string { return e.message }

// Unwrap returns the underlying error, or nil.
func (e *RegistryError) Unwrap() error { return e.cause }

// FetchPendingError signals that a background writer-schema fetch has not
// completed.
//
// [SchemaCache.WriterSchema] never blocks. When a deserializer meets a schema
// ID that is not cached yet, the cache starts one asynchronous registry fetch
// and returns this error. Treat it as retriable: by the time the record is
// retried, the fetch has usually completed and deserialization succeeds.
type FetchPendingError struct {
	// SchemaID is the schema ID whose writer schema is being fetched.
	SchemaID int
}

// Error names the pending schema ID.
func (e *FetchPendingError) Error() string {
	return fmt.Sprintf("writer schema for id %d is pending fetch", uint32(e.SchemaID))
}

// Retriable marks the error as safe to retry.
func (e *FetchPendingError) Retriable() bool { return true }
