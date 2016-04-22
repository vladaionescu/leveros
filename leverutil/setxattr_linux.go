package leverutil

import "syscall"

func setxattr(path, key string, value []byte, flags int) error {
	return syscall.Setxattr(path, key, value, flags)
}
