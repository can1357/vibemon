//go:build windows

package vmon

import "errors"

var errTakeoverFdUnsupported = errors.New("vmon: takeover worker fd redirection is not supported on windows")

func takeoverDupFd(int) (int, error) { return 0, errTakeoverFdUnsupported }

func takeoverDupToFd(int, int) error { return errTakeoverFdUnsupported }
