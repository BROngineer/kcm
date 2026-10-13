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
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"time"

	providerclient "github.com/brongineer/ksm-provider-api/provider/client"
	registryserver "github.com/brongineer/ksm-provider-api/registry/server"
)

// ClientCache keeps one provider client per registered provider and rebuilds
// it when the provider announces different connection details.
type ClientCache struct {
	clients map[string]cachedClient
	// HTTPClient overrides the transport; tests use it to trust their server.
	HTTPClient *http.Client
	// TokenFile is the projected ServiceAccount token kcm presents to providers.
	TokenFile string
	// MaxTimeout caps provider-announced timeouts so one slow provider cannot hold all workers.
	MaxTimeout time.Duration
	mu         sync.Mutex
}

type cachedClient struct {
	client   *providerclient.Client
	endpoint string
	caBundle []byte
	timeout  time.Duration
}

// Client returns the client for the provider described by entry.
func (c *ClientCache) Client(entry registryserver.Entry) (*providerclient.Client, error) {
	timeout := entry.Timeout
	if c.MaxTimeout > 0 && (timeout <= 0 || timeout > c.MaxTimeout) {
		timeout = c.MaxTimeout
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if cached, ok := c.clients[entry.Name]; ok &&
		cached.endpoint == entry.Endpoint && cached.timeout == timeout && bytes.Equal(cached.caBundle, entry.CABundle) {
		return cached.client, nil
	}

	cl, err := providerclient.New(providerclient.Config{
		Endpoint:   entry.Endpoint,
		CABundle:   entry.CABundle,
		TokenFile:  c.TokenFile,
		Timeout:    timeout,
		HTTPClient: c.HTTPClient,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client for provider %s: %w", entry.Name, err)
	}

	if c.clients == nil {
		c.clients = make(map[string]cachedClient)
	}
	c.clients[entry.Name] = cachedClient{
		client:   cl,
		endpoint: entry.Endpoint,
		caBundle: bytes.Clone(entry.CABundle),
		timeout:  timeout,
	}
	return cl, nil
}
