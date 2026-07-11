//go:build linux

package vmon

import "syscall"

func takeoverDupFd(fd int) (int, error) { return syscall.Dup(fd) }

func takeoverDupToFd(from, to int) error { return syscall.Dup3(from, to, 0) }
