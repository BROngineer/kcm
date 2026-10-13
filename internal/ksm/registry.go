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
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/brongineer/ksm-provider-api/auth"
	"github.com/brongineer/ksm-provider-api/httpserver"
	registryserver "github.com/brongineer/ksm-provider-api/registry/server"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

const (
	transitionsBufferSize = 1024
	authCacheTTL          = time.Minute
)

// RegistryOptions configures the provider registry served by kcm.
type RegistryOptions struct {
	// Allowed maps a provider name to the only ServiceAccount username that may register it.
	Allowed     map[string]string
	BindAddress string
	// CertDir holds tls.crt and tls.key; unused when Insecure is set.
	CertDir  string
	Insecure bool
}

// ParseAllowList parses "name=namespace/serviceaccount" entries into the registry allow-list.
func ParseAllowList(entries []string) (map[string]string, error) {
	allowed := make(map[string]string, len(entries))
	for _, entry := range entries {
		name, sa, ok := strings.Cut(entry, "=")
		namespace, saName, saOK := strings.Cut(sa, "/")
		if !ok || !saOK || name == "" || namespace == "" || saName == "" {
			return nil, fmt.Errorf("invalid allowed provider %q: expected name=namespace/serviceaccount", entry)
		}
		if _, exists := allowed[name]; exists {
			return nil, fmt.Errorf("provider %q is allowed more than once", name)
		}
		allowed[name] = auth.ServiceAccountUsername(namespace, saName)
	}
	return allowed, nil
}

// SetupRegistry adds the provider registry and its HTTP server to mgr. The
// returned channel carries registry transitions; the object name is the provider name.
func SetupRegistry(mgr ctrl.Manager, opts RegistryOptions) (*registryserver.Registry, <-chan event.GenericEvent, error) {
	logger := ctrl.Log.WithName("ksm-registry")

	transitions := make(chan event.GenericEvent, transitionsBufferSize)
	// transitions happen on the leader only (others answer not-leader), where the
	// controller drains the channel, so a full buffer means the controller is stuck.
	reg := registryserver.NewRegistry(registryserver.WithTransitionHandler(func(t registryserver.Transition) {
		select {
		case transitions <- event.GenericEvent{Object: &metav1.PartialObjectMetadata{Name: t.Name}}:
		default:
			logger.Info("Dropped provider registry transition", "provider", t.Name, "kind", t.Kind)
		}
	}))

	cs, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create clientset: %w", err)
	}

	slogger := slog.New(logr.ToSlogHandler(logger))
	h, err := registryserver.NewHandler(registryserver.HandlerConfig{
		Registry:      reg,
		Authenticator: auth.NewCachedAuthenticator(auth.NewTokenReviewAuthenticator(cs, auth.RegistryAudience), authCacheTTL),
		Allowed:       opts.Allowed,
		IsLeader:      registryserver.LeaderFromElected(mgr.Elected()),
		Logger:        slogger,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create registry handler: %w", err)
	}

	if err := mgr.Add(reg); err != nil {
		return nil, nil, fmt.Errorf("failed to add registry to manager: %w", err)
	}

	srv := &httpserver.Server{
		Handler:  h,
		Logger:   slogger,
		Addr:     opts.BindAddress,
		Insecure: opts.Insecure,
	}
	if !opts.Insecure {
		srv.CertFile = filepath.Join(opts.CertDir, "tls.crt")
		srv.KeyFile = filepath.Join(opts.CertDir, "tls.key")
	}
	if err := mgr.Add(srv); err != nil {
		return nil, nil, fmt.Errorf("failed to add registry server to manager: %w", err)
	}

	return reg, transitions, nil
}
