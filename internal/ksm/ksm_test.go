// Copyright 2026
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ksm

import (
	"context"
	"testing"
	"time"

	"github.com/brongineer/ksm-provider-api/api/provider"
	"github.com/brongineer/ksm-provider-api/providertest"
	registryserver "github.com/brongineer/ksm-provider-api/registry/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseAllowList(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		want    map[string]string
		wantErr bool
	}{
		{name: "empty", entries: nil, want: map[string]string{}},
		{
			name:    "several providers",
			entries: []string{"ksm-projectsveltos=projectsveltos/ksm-provider-sveltos", "other=ns/sa"},
			want: map[string]string{
				"ksm-projectsveltos": "system:serviceaccount:projectsveltos:ksm-provider-sveltos",
				"other":              "system:serviceaccount:ns:sa",
			},
		},
		{name: "missing service account", entries: []string{"name"}, wantErr: true},
		{name: "missing namespace", entries: []string{"name=sa"}, wantErr: true},
		{name: "empty name", entries: []string{"=ns/sa"}, wantErr: true},
		{name: "empty service account name", entries: []string{"name=ns/"}, wantErr: true},
		{name: "duplicate name", entries: []string{"name=ns/a", "name=ns/b"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAllowList(tt.entries)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func testEntry(endpoint string) registryserver.Entry {
	return registryserver.Entry{
		Name:     "ksm-test",
		Endpoint: endpoint,
		State:    registryserver.StateActive,
		Timeout:  10 * time.Second,
	}
}

func TestClientCache_ReusesClientForSameEntry(t *testing.T) {
	srv := providertest.NewTestServer(t, &providertest.FakeProvider{})
	cache := &ClientCache{HTTPClient: srv.HTTPClient, TokenFile: srv.TokenFile, MaxTimeout: time.Minute}

	first, err := cache.Client(testEntry(srv.URL))
	require.NoError(t, err)
	second, err := cache.Client(testEntry(srv.URL))
	require.NoError(t, err)
	assert.Same(t, first, second)

	_, err = first.DeployServices(t.Context(), provider.DeployServicesRequest{Name: "ss", Namespace: "ns", Generation: 1})
	require.NoError(t, err)
}

func TestClientCache_RebuildsClientOnChangedConnection(t *testing.T) {
	srv := providertest.NewTestServer(t, &providertest.FakeProvider{})
	cache := &ClientCache{HTTPClient: srv.HTTPClient, TokenFile: srv.TokenFile, MaxTimeout: time.Minute}

	first, err := cache.Client(testEntry(srv.URL))
	require.NoError(t, err)

	moved := testEntry(srv.URL + "/")
	second, err := cache.Client(moved)
	require.NoError(t, err)
	assert.NotSame(t, first, second, "endpoint change must rebuild the client")

	slower := moved
	slower.Timeout = 20 * time.Second
	third, err := cache.Client(slower)
	require.NoError(t, err)
	assert.NotSame(t, second, third, "timeout change must rebuild the client")

	rotated := slower
	rotated.CABundle = []byte("new-ca")
	fourth, err := cache.Client(rotated)
	require.NoError(t, err)
	assert.NotSame(t, third, fourth, "CA change must rebuild the client")
}

func TestClientCache_CapsProviderTimeout(t *testing.T) {
	slow := &providertest.FakeProvider{
		DeployFunc: func(ctx context.Context, req provider.DeployServicesRequest) (provider.DeployServicesResult, error) {
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
			}
			return provider.DeployServicesResult{ObservedGeneration: req.Generation, Services: []provider.ServiceState{}}, nil
		},
	}
	srv := providertest.NewTestServer(t, slow)
	cache := &ClientCache{HTTPClient: srv.HTTPClient, TokenFile: srv.TokenFile, MaxTimeout: 100 * time.Millisecond}

	c, err := cache.Client(testEntry(srv.URL))
	require.NoError(t, err)

	start := time.Now()
	_, err = c.DeployServices(t.Context(), provider.DeployServicesRequest{Name: "ss", Namespace: "ns", Generation: 1})
	require.Error(t, err)
	assert.Less(t, time.Since(start), 5*time.Second, "the call must be bounded by MaxTimeout, not the provider's timeout")
}
