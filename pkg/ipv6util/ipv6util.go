package ipv6util

import (
	"bufio"
	"strings"

	"github.com/asaskevich/govalidator"
)

const (
	// PxctlStatus defines the pxctl status command
	PxctlStatus = "status"
	// PxctlClusterList defines the pxctl cluster list command
	PxctlClusterList = "cluster list"
	// PxctlClusterInspect defines the pxctl cluster inspect command
	PxctlClusterInspect = "cluster inspect"
)

type parser struct {
	options []parserOption
	scanner *bufio.Scanner
	ips     []string
}

// parserOption defines the options for parsing IPv6 line
// the options includes:
// - prefix: prefix used to match the line
// - index: index of the IPv6 address in the line after split the line by commons
//          delimiters
// - count: in some cases after we match the prefix, we parse the lines after
//          the match. count is used to defined how many lines to parse
type parserOption struct {
	prefix string
	index  int
	count  int
}

// newIPv6Parser returns a parser instance
func newIPv6Parser(options []parserOption) parser {
	return parser{options, nil, nil}
}

// newIPv6ParserOption returns a parserOptioninstance
func newIPv6ParserOption(prefix string, index, count int) parserOption {
	return parserOption{prefix, index, count}
}

// parse parses the given command output and return the list of IPv6 address
func (p *parser) parse(cmdOutput string) []string {
	p.ips = []string{}
	p.scanner = bufio.NewScanner(strings.NewReader(cmdOutput))
	for p.scanner.Scan() {
		line := p.scanner.Text()
		line = strings.TrimPrefix(line, "\t")
		p.parseLine(line)
	}
	return p.ips
}

// parseLine check for one of the two conditions where IPs are printed:
// 1. The IP is on the same line. `IP: <addr>`
//    ex: IP: 0000:111:2222:3333:444:5555:6666:777
// 2. There are multiple IPs after the line that match prefix
//    ex: IP					ID					SchedulerNodeName	Auth		StorageNode	Used	Capacity	Status	StorageStatus	Version		Kernel			OS
//		0000:111:2222:3333:444:5555:6666:777	f703597a-9772-4bdb-b630-6395b3c98658	...	...
// 		0000:111:2222:3333:444:5555:6666:777	cedc897f-a489-4c28-9c20-12b8b4c3d1d8	...	...
func (p *parser) parseLine(line string) {
	for _, option := range p.options {
		if strings.HasPrefix(line, option.prefix) {
			// 1st condiition expect to parse a single line where the prefix is match
			if option.count == 0 {
				p.ips = append(p.ips, strings.Fields(line)[option.index])
			}

			// look for the IPs in lines after prefix are matched
			for i := 0; i < option.count; i++ {
				if !p.scanner.Scan() {
					break
				}
				line := p.scanner.Text()
				p.ips = append(p.ips, strings.Fields(line)[option.index])
			}
		}
	}
}

// ParseIPv6AddressInPxctlCommand takes output of `pxctl command` and return the list of IPs parsed
func ParseIPv6AddressInPxctlCommand(command string, output string, nodeCount int) []string {
	switch command {
	case PxctlStatus:
		return parseIPv6AddressInPxctlStatus(output, nodeCount)
	case PxctlClusterList:
		return parseIPv6AddressInPxctlClusterList(output, nodeCount)
	case PxctlClusterInspect:
		return parseIPv6AddressInPxctlClusterInspect(output, nodeCount)
	default:
		return []string{}
	}
}

// parseIpv6AddressInPxctlStatus takes output of `pxctl status` and return the list of IPs parsed
// iterate each line to check for two conditions where IPs are printed:
// 1. `IP: <addr>`
//    ex: IP: 0000:111:2222:3333:444:5555:6666:777
// 2. (number of nodes) lines after `IP \t ID...`
//    ex: IP					ID					SchedulerNodeName	Auth		StorageNode	Used	Capacity	Status	StorageStatus	Version		Kernel			OS
//		0000:111:2222:3333:444:5555:6666:777	f703597a-9772-4bdb-b630-6395b3c98658	...	...
// 		0000:111:2222:3333:444:5555:6666:777	cedc897f-a489-4c28-9c20-12b8b4c3d1d8	...	...
func parseIPv6AddressInPxctlStatus(status string, nodeCount int) []string {
	// parse the case for IP: 0000:111:2222:3333:444:5555:6666:777
	p1 := newIPv6ParserOption("IP:", 1, 0)

	// parse the case:
	// IP					ID					SchedulerNodeName	Auth		StorageNode	Used	Capacity	Status	StorageStatus	Version		Kernel			OS
	// 0000:111:2222:3333:444:5555:6666:777	f703597a-9772-4bdb-b630-6395b3c98658	...	...
	// 0000:111:2222:3333:444:5555:6666:777	cedc897f-a489-4c28-9c20-12b8b4c3d1d8	...	...
	p2 := newIPv6ParserOption("IP\t", 0, nodeCount)
	p := newIPv6Parser([]parserOption{p1, p2})

	return p.parse(status)
}

// parseIpv6AddressInPxctlClusterList takes output of `pxctl cluster list` and return the list of IPs parsed
// iterate each line to check for the conditions where IPs are printed:
// (number of nodes) lines after `ID\t... DATA IP\t`. ex:
// ID					SCHEDULER_NODE_NAME	DATA IP					CPU		MEM TOTAL	MEM FREE	CONTAINERS	VERSION		Kernel				OS		STATUS
// 2ca8932b-b17e-425c-bcbe-d33b0f64b623	node03			0000:111:2222:3333:444:5555:6666:777	... ...
// 6b9d12e0-fb28-459e-acf1-cea4d57004e2	node04			0000:111:2222:3333:444:5555:6666:777	... ...
func parseIPv6AddressInPxctlClusterList(output string, nodeCount int) []string {
	option := newIPv6ParserOption("ID\t", 2, nodeCount)
	p := newIPv6Parser([]parserOption{option})
	return p.parse(output)
}

// parseIPv6AddressInPxctlClusterInspect takes output of `pxctl cluster inspect` and return the list of IPs parsed
// iterate each line to check for two conditions where IPs are printed:
// 1. `Mgmt IP\t :\t  <addr>` ex: Mgmt IP       		:  0000:111:2222:3333:444:5555:6666:111
// 2. `Data IP\t :\t <addr>` ex: Data IP       		:  0000:111:2222:3333:444:5555:6666:111
func parseIPv6AddressInPxctlClusterInspect(output string, nodeCount int) []string {
	option1 := newIPv6ParserOption("Mgmt IP", 3, 0)
	option2 := newIPv6ParserOption("Data IP", 3, 0)
	p := newIPv6Parser([]parserOption{option1, option2})
	return p.parse(output)
}

// IsAddressIPv6 checks the given address is a valid Ipv6 address
func IsAddressIPv6(addr string) bool {
	return govalidator.IsIPv6(addr)
}

// AreAddressesIPv6 checks the given addresses are valid Ipv6 addresses
func AreAddressesIPv6(addrs []string) bool {
	isIpv6 := true

	for _, addr := range addrs {
		isIpv6 = isIpv6 && IsAddressIPv6(addr)
	}
	return isIpv6
}
