//go:build unix && !linux

package vmon

import "syscall"

func takeoverDupFd(fd int) (int, error) { return syscall.Dup(fd) }

func takeoverDupToFd(from, to int) error { return syscall.Dup2(from, to) }
