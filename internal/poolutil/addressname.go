/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package poolutil

import (
	"fmt"
	"net/netip"
	"strings"
)

// AddressToObjectName derives the name of the IPAddress object that represents
// the given IP address.
//
// The concurrency safety of allocation rests on this mapping being canonical
// and injective: the API server guarantees that two objects with the same name
// cannot exist in the same namespace, so if (and only if) every equivalent
// spelling of an address maps to the same name, and different addresses never
// map to the same name, then two allocators can never hold the same address at
// the same time. See docs/concurrent-claim-allocation.md.
//
// Normalization rules, applied before the name is formed:
//   - IPv4-mapped IPv6 addresses ("::ffff:10.0.0.1") are unmapped to their
//     IPv4 form, so an address cannot be claimed once as IPv4 and once as an
//     IPv4-mapped IPv6 address.
//   - A zone suffix ("fe80::1%eth0") is stripped.
//   - IPv4 uses the dotted-quad form, e.g. "10.0.0.1".
//   - IPv6 uses all eight hextets, lowercase, without leading zeros, joined by
//     '-', e.g. "2001-db8-0-0-0-0-0-1". The expanded form is used instead of
//     the compressed "::" notation because a compressed spelling can start or
//     end with '-' ("::1" -> "--1"), which is not a valid RFC 1123 label.
//     The expanded form always starts and ends with a hexadecimal digit.
//
// The result is a valid RFC 1123 subdomain of at most 39 characters. The
// mapping is reversible via ObjectNameToAddress. IPv4 names contain '.' but
// never '-'; IPv6 names contain '-' but never '.', so the two families cannot
// collide with each other.
func AddressToObjectName(addr netip.Addr) string {
	addr = normalizeAddress(addr)
	if addr.Is4() {
		return addr.String()
	}

	b := addr.As16()
	hextets := [8]uint16{}
	for i := range hextets {
		hextets[i] = uint16(b[2*i])<<8 | uint16(b[2*i+1])
	}
	var sb strings.Builder
	// Worst case: 8 groups of 4 hex digits plus 7 separators.
	sb.Grow(39)
	for i, hextet := range hextets {
		if i > 0 {
			sb.WriteByte('-')
		}
		fmt.Fprintf(&sb, "%x", hextet)
	}
	return sb.String()
}

// ObjectNameToAddress is the inverse of AddressToObjectName. It returns an
// error if the name cannot have been produced by AddressToObjectName, for
// example for IPAddress objects created by older controller versions, which
// were named after their claim.
func ObjectNameToAddress(name string) (netip.Addr, error) {
	candidate := name
	if strings.ContainsRune(name, '-') {
		candidate = strings.ReplaceAll(name, "-", ":")
	}
	addr, err := netip.ParseAddr(candidate)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("object name %q does not encode an IP address: %w", name, err)
	}
	// Reject names that parse but are not the canonical encoding of the
	// address, e.g. a compressed IPv6 spelling or a non-canonical IPv4 form.
	if AddressToObjectName(addr) != name {
		return netip.Addr{}, fmt.Errorf("object name %q is not the canonical name of an IP address", name)
	}
	return addr, nil
}

// normalizeAddress returns the canonical form of addr used for object names:
// IPv4-mapped IPv6 addresses are unmapped and any zone is stripped.
func normalizeAddress(addr netip.Addr) netip.Addr {
	return addr.Unmap().WithZone("")
}
