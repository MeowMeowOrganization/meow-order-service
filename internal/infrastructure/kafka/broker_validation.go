package kafka

import (
	"net"
	"strconv"
	"strings"
)

func validateBrokerAddress(b string) bool {
	host, port, err := net.SplitHostPort(b)
	if err != nil || strings.TrimSpace(b) != b || host == "" {
		return false
	}
	n, err := strconv.Atoi(port)
	return err == nil && n >= 1 && n <= 65535
}
