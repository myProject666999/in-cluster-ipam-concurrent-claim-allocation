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
	"net/netip"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

func TestAddressToObjectName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "ipv4", input: "10.0.0.1", expected: "10.0.0.1"},
		{name: "ipv6 expanded", input: "2001:0db8:0000:0000:0000:0000:0000:0001", expected: "2001-db8-0-0-0-0-0-1"},
		{name: "ipv6 compressed", input: "2001:db8::1", expected: "2001-db8-0-0-0-0-0-1"},
		{name: "ipv6 uppercase", input: "2001:DB8::1", expected: "2001-db8-0-0-0-0-0-1"},
		{name: "ipv6 loopback", input: "::1", expected: "0-0-0-0-0-0-0-1"},
		{name: "ipv6 trailing zeros", input: "1::", expected: "1-0-0-0-0-0-0-0"},
		{name: "ipv6 all zeros", input: "::", expected: "0-0-0-0-0-0-0-0"},
		{name: "ipv4-mapped ipv6", input: "::ffff:10.0.0.1", expected: "10.0.0.1"},
		{name: "ipv4-mapped ipv6 hex", input: "::ffff:a00:1", expected: "10.0.0.1"},
		{name: "ipv6 with zone", input: "fe80::1%eth0", expected: "fe80-0-0-0-0-0-0-1"},
		{name: "max ipv6", input: "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", expected: "ffff-ffff-ffff-ffff-ffff-ffff-ffff-ffff"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := mustParseAddr(t, tt.input)
			got := AddressToObjectName(addr)
			if got != tt.expected {
				t.Errorf("AddressToObjectName(%q) = %q, want %q", tt.input, got, tt.expected)
			}

			// The name must be a valid Kubernetes object name (RFC 1123
			// subdomain, at most 253 characters).
			if errs := validation.IsDNS1123Subdomain(got); len(errs) > 0 {
				t.Errorf("name %q is not a valid DNS1123 subdomain: %v", got, errs)
			}

			// The mapping must be reversible.
			back, err := ObjectNameToAddress(got)
			if err != nil {
				t.Fatalf("ObjectNameToAddress(%q) failed: %v", got, err)
			}
			if AddressToObjectName(back) != got {
				t.Errorf("round trip of %q yielded %q", got, AddressToObjectName(back))
			}
		})
	}
}

func TestAddressToObjectNameCanonical(t *testing.T) {
	// Equivalent spellings of the same address must collapse to one name, and
	// different addresses must never share a name.
	equivalent := [][]string{
		{"10.0.0.1", "::ffff:10.0.0.1", "::ffff:a00:1"},
		{"2001:db8::1", "2001:DB8::1", "2001:0db8:0000:0000:0000:0000:0000:0001"},
		{"fe80::1", "fe80::1%eth0", "fe80:0:0:0:0:0:0:1"},
	}
	seen := map[string]string{}
	for _, group := range equivalent {
		var want string
		for i, input := range group {
			got := AddressToObjectName(mustParseAddr(t, input))
			if i == 0 {
				want = got
			} else if got != want {
				t.Errorf("equivalent addresses %q and %q produced different names %q and %q", group[0], input, want, got)
			}
			if prev, ok := seen[got]; ok && prev != group[0] {
				t.Errorf("different addresses %q and %q map to the same name %q", prev, input, got)
			}
			seen[got] = group[0]
		}
	}
}

func TestAddressToObjectNameFamiliesDoNotCollide(t *testing.T) {
	// IPv4 names contain '.' and never '-'; IPv6 names contain '-' and never
	// '.'. A v4 address and a v6 address therefore never share a name.
	v4 := AddressToObjectName(mustParseAddr(t, "0.0.0.1"))
	v6 := AddressToObjectName(mustParseAddr(t, "::1"))
	if v4 == v6 {
		t.Fatalf("IPv4 %q and IPv6 %q map to the same name", v4, v6)
	}
}

func TestObjectNameToAddressRejectsNonAddressNames(t *testing.T) {
	for _, name := range []string{"my-claim", "claim-123", "test.address", "", "2001-db8--1", "10.0.0.01"} {
		if _, err := ObjectNameToAddress(name); err == nil {
			t.Errorf("ObjectNameToAddress(%q) should have failed", name)
		}
	}
}

func mustParseAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	parsed, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatalf("failed to parse address %q: %v", s, err)
	}
	return parsed
}
