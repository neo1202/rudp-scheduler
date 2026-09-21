package transport

import (
	"fmt"

	"github.com/neo1202/rudp-scheduler/rudp"
)

// Dial connects to a server over the named transport: "rudp" or "tcp". p
// configures the reliable-UDP transport and is ignored for TCP.
func Dial(proto, hostport string, p *rudp.Params) (Client, error) {
	switch proto {
	case "rudp":
		c, err := rudp.NewClient(hostport, p)
		if err != nil {
			return nil, err // avoid a non-nil interface holding a nil *rudp.Client
		}
		return c, nil
	case "tcp":
		c, err := DialTCP(hostport)
		if err != nil {
			return nil, err
		}
		return c, nil
	}
	return nil, fmt.Errorf("transport: unknown transport %q", proto)
}
