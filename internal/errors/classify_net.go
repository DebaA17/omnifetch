package errors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"os"
	"syscall"
)

func InvalidURL(u string, cause error) *OmniError {
	return Wrap(CodeInvalidURL, "That link doesn’t look like a valid URL.", cause, Details(u), Sev(SeverityWarn))
}

func UnsupportedDomain(host string) *OmniError {
	return New(CodeUnsupportedDomain, "That domain isn’t supported by any engine.", Details(host), Sev(SeverityWarn))
}

func Canceled(cause error) *OmniError {
	return Wrap(CodeCanceled, "Canceled.", cause, Retryable(false), Sev(SeverityInfo))
}

// ClassifyNet maps common network / OS errors into a stable OmniError shape.
func ClassifyNet(op string, rawURL string, err error) *OmniError {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return Canceled(err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Wrap(CodeTimeout, "Timed out while contacting the server.", err, Retryable(true), Details(op+" "+rawURL))
	}

	var ue *url.Error
	if errors.As(err, &ue) && ue.Timeout() {
		return Wrap(CodeTimeout, "Timed out while contacting the server.", err, Retryable(true), Details(op+" "+rawURL))
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return Wrap(CodeDNS, "DNS lookup failed.", err, Retryable(true), Details(dnsErr.Name))
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Err != nil {
			var sysErr *os.SyscallError
			if errors.As(opErr.Err, &sysErr) {
				switch sysErr.Err {
				case syscall.ECONNREFUSED:
					return Wrap(CodeConnRefused, "Connection refused.", err, Retryable(true), Details(op+" "+rawURL))
				case syscall.EACCES:
					return Wrap(CodePermissionDenied, "Permission denied.", err, Retryable(false), Details(op+" "+rawURL), Sev(SeverityFatal))
				}
			}
		}
	}

	if errors.Is(err, syscall.ENOSPC) {
		return Wrap(CodeDiskFull, "Disk full.", err, Retryable(false), Sev(SeverityFatal), Details(op+" "+rawURL))
	}

	return Wrap(CodeUnknown, "Unexpected error.", err, Retryable(true), Details(op+" "+rawURL))
}

func ChecksumSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

