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

package serviceset

import (
	"context"

	"github.com/brongineer/ksm-provider-api/api/provider"
	registryserver "github.com/brongineer/ksm-provider-api/registry/server"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
)

// ProviderRegistry resolves a state management provider by the name ServiceSets refer to.
type ProviderRegistry interface {
	Lookup(name string) (registryserver.Entry, bool)
}

// ProviderClient calls a state management provider.
type ProviderClient interface {
	DeployServices(ctx context.Context, req provider.DeployServicesRequest) (provider.DeployServicesResult, error)
	DeleteServices(ctx context.Context, req provider.DeleteServicesRequest) (provider.DeleteServicesResult, error)
}

// ProviderClientFactory returns the client for a registered provider.
type ProviderClientFactory func(registryserver.Entry) (ProviderClient, error)

// deployServices asks the provider to deploy serviceSet, folds the result into
// its status and returns the fingerprint of each deployed service.
func (*ServiceSetReconciler) deployServices(
	context.Context,
	client.Client,
	registryserver.Entry,
	*kcmv1.ServiceSet,
) (map[client.ObjectKey]string, error) {
	return nil, errNotImplemented
}

// deleteServices asks the provider to delete serviceSet's services
// and reports whether they are gone.
func (*ServiceSetReconciler) deleteServices(context.Context, *kcmv1.ServiceSet) (bool, error) {
	return false, errNotImplemented
}

// requestsForProvider maps a provider registry transition to the provider's ServiceSets.
func (*ServiceSetReconciler) requestsForProvider(context.Context, client.Object) ([]ctrl.Request, error) {
	return nil, errNotImplemented
}

// requestForEvent maps a provider state-change notification to its ServiceSet.
func requestForEvent(context.Context, client.Object) []ctrl.Request {
	return nil
}
