package errors

import (
	"errors"
	"fmt"
)

type Severity uint8

const (
	SeverityInfo Severity = iota + 1
	SeverityWarn
	SeverityError
	SeverityFatal
)

type Code string

const (
	CodeInvalidURL            Code = "INVALID_URL"
	CodeUnsupportedDomain     Code = "UNSUPPORTED_DOMAIN"
	CodePrivateContent        Code = "PRIVATE_CONTENT"
	CodeGeoBlocked            Code = "GEO_BLOCKED"
	CodeTimeout               Code = "TIMEOUT"
	CodeDNS                   Code = "DNS_FAILURE"
	CodeConnRefused           Code = "CONNECTION_REFUSED"
	CodePermissionDenied      Code = "PERMISSION_DENIED"
	CodeDiskFull              Code = "DISK_FULL"
	CodeCorruptPartial        Code = "CORRUPT_PARTIAL"
	CodeChecksumMismatch      Code = "CHECKSUM_MISMATCH"
	CodeRateLimited           Code = "RATE_LIMITED"
	CodeHTTP                  Code = "HTTP_ERROR"
	CodeCanceled              Code = "CANCELED"
	CodeUnknown               Code = "UNKNOWN"
)

// OmniError is a production-oriented, user-presentable error with enough structure
// for both TUI rendering and retry strategy.
type OmniError struct {
	Code       Code
	UserMsg    string
	Details    string
	Retryable  bool
	Severity   Severity
	HTTPStatus int
	cause      error
}

func (e *OmniError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Details == "" {
		return string(e.Code) + ": " + e.UserMsg
	}
	return string(e.Code) + ": " + e.UserMsg + " (" + e.Details + ")"
}

func (e *OmniError) Unwrap() error { return e.cause }

func (e *OmniError) WithCause(err error) *OmniError {
	if err == nil {
		return e
	}
	cp := *e
	cp.cause = err
	return &cp
}

func New(code Code, userMsg string, opts ...Option) *OmniError {
	e := &OmniError{
		Code:      code,
		UserMsg:   userMsg,
		Severity:  SeverityError,
		Retryable: false,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

type Option func(*OmniError)

func Details(s string) Option { return func(e *OmniError) { e.Details = s } }
func Retryable(v bool) Option { return func(e *OmniError) { e.Retryable = v } }
func Sev(v Severity) Option   { return func(e *OmniError) { e.Severity = v } }
func HTTPStatus(v int) Option { return func(e *OmniError) { e.HTTPStatus = v } }

func IsCode(err error, code Code) bool {
	var oe *OmniError
	if errors.As(err, &oe) {
		return oe.Code == code
	}
	return false
}

func As(err error) (*OmniError, bool) {
	var oe *OmniError
	if errors.As(err, &oe) {
		return oe, true
	}
	return nil, false
}

func Wrap(code Code, userMsg string, cause error, opts ...Option) *OmniError {
	if cause == nil {
		return New(code, userMsg, opts...)
	}
	return New(code, userMsg, opts...).WithCause(cause)
}

func FormatTechnical(err error) string {
	if err == nil {
		return ""
	}
	var oe *OmniError
	if errors.As(err, &oe) {
		if oe.cause != nil {
			return fmt.Sprintf("%s: %v", oe.Details, oe.cause)
		}
		return oe.Details
	}
	return err.Error()
}

