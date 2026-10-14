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
	"errors"
	"testing"
	"time"

	"github.com/brongineer/ksm-provider-api/api/provider"
	"github.com/brongineer/ksm-provider-api/providertest"
	registryserver "github.com/brongineer/ksm-provider-api/registry/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
)

func TestAuthorizeFromStateManagementProviders(t *testing.T) {
	const (
		providerName = "ksm-projectsveltos"
		adapterUser  = "system:serviceaccount:projectsveltos:ksm-provider-sveltos"
	)
	declared := &kcmv1.StateManagementProvider{
		Name: providerName,
		Spec: kcmv1.StateManagementProviderSpec{
			Registration: kcmv1.ProviderRegistration{
				ServiceAccount: kcmv1.ServiceAccountReference{Namespace: "projectsveltos", Name: "ksm-provider-sveltos"},
			},
		},
	}
	undeclared := &kcmv1.StateManagementProvider{Name: providerName}

	tests := []struct {
		name     string
		objects  []client.Object
		getErr   error
		username string
		want     bool
		wantErr  bool
	}{
		{name: "declared ServiceAccount", objects: []client.Object{declared}, username: adapterUser, want: true},
		{name: "another ServiceAccount", objects: []client.Object{declared}, username: "system:serviceaccount:other:sa"},
		{name: "no StateManagementProvider", username: adapterUser},
		{name: "no registration", objects: []client.Object{undeclared}, username: adapterUser},
		{name: "lookup fails", getErr: errors.New("cache not synced"), username: adapterUser, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, kcmv1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.objects...).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if tt.getErr != nil {
							return tt.getErr
						}
						return c.Get(ctx, key, obj, opts...)
					},
				}).Build()

			got, err := AuthorizeFromStateManagementProviders(c)(t.Context(), providerName, tt.username)
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
