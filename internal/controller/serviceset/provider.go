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
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/brongineer/ksm-provider-api/api/provider"
	"github.com/brongineer/ksm-provider-api/notify"
	providerclient "github.com/brongineer/ksm-provider-api/provider/client"
	registryserver "github.com/brongineer/ksm-provider-api/registry/server"
	libsveltosv1beta1 "github.com/projectsveltos/libsveltos/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
	"github.com/K0rdent/kcm/internal/record"
	kubeutil "github.com/K0rdent/kcm/internal/util/kube"
	pointerutil "github.com/K0rdent/kcm/internal/util/pointer"
)

var errStaleResult = errors.New("provider result is for an older ServiceSet generation")

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
func (r *ServiceSetReconciler) deployServices(
	ctx context.Context,
	rgnClient client.Client,
	entry registryserver.Entry,
	serviceSet *kcmv1.ServiceSet,
) (map[client.ObjectKey]string, error) {
	c, err := r.NewProviderClient(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to build client for provider %s: %w", entry.Name, err)
	}

	res, err := c.DeployServices(ctx, provider.DeployServicesRequest{
		Name:       serviceSet.Name,
		Namespace:  serviceSet.Namespace,
		Generation: serviceSet.Generation,
	})
	if err != nil {
		logProviderError(ctx, err)
		return nil, fmt.Errorf("failed to deploy services with provider %s: %w", entry.Name, err)
	}
	if res.ObservedGeneration < serviceSet.Generation {
		return nil, fmt.Errorf("%w: observed %d, current %d", errStaleResult, res.ObservedGeneration, serviceSet.Generation)
	}

	// the provider reports the services it keeps in memory even when it failed
	// to apply them, as the in-tree reconciler persisted them on failure.
	serviceSet.Status.Services = servicesFromProvider(serviceSet.Status.Services, res.Services, r.timeFunc())

	if err := r.setProviderApplied(serviceSet, res); err != nil {
		return nil, err
	}
	if err := r.setStatusesCollected(serviceSet, res); err != nil {
		return nil, err
	}

	targetExists, err := targetClusterExists(ctx, rgnClient, serviceSet)
	if err != nil {
		return nil, fmt.Errorf("failed to check target cluster: %w", err)
	}
	if targetExists {
		serviceSet.Status.Deployed = !slices.ContainsFunc(serviceSet.Status.Services, func(s kcmv1.ServiceState) bool {
			return s.State != kcmv1.ServiceStateDeployed
		})
	} else {
		ctrl.LoggerFrom(ctx).Info("No matching clusters found for ServiceSet")
		serviceSet.Status.Deployed = false
	}
	r.updateServicesInReadyStateCondition(serviceSet)

	hashes := make(map[client.ObjectKey]string, len(res.Services))
	for _, s := range res.Services {
		if h := pointerutil.Deref(s.DeployedHash, ""); h != "" {
			hashes[client.ObjectKey{Namespace: s.Namespace, Name: s.Name}] = h
		}
	}
	return hashes, nil
}

// setProviderApplied records whether the provider applied the services and
// returns an error when it did not, so the reconcile is retried.
func (r *ServiceSetReconciler) setProviderApplied(serviceSet *kcmv1.ServiceSet, res provider.DeployServicesResult) error {
	condition := findCondition(serviceSet, kcmv1.ServiceSetProviderAppliedCondition)
	initialStatus := condition.Status
	oldState := apimeta.FindStatusCondition(serviceSet.Status.Conditions, kcmv1.ServiceSetProviderAppliedCondition).DeepCopy()

	if res.Applied {
		updated := updateCondition(serviceSet, condition, metav1.ConditionTrue, kcmv1.ServiceSetProviderAppliedReason,
			kcmv1.ServiceSetProviderAppliedMessage, r.timeFunc())
		if initialStatus != metav1.ConditionTrue && updated {
			record.Eventf(serviceSet, nil, kcmv1.ServiceSetProviderApplySuccessEvent, kcmv1.ServiceSetApplyServicesEventAction,
				"Provider applied services for ServiceSet %s", serviceSet.Name)
		}
		return nil
	}

	reason := string(pointerutil.Deref(res.Reason, provider.DeployServicesResultReasonProviderError))
	message := pointerutil.Deref(res.Message, kcmv1.ServiceSetProviderNotAppliedMessage)
	updateCondition(serviceSet, condition, metav1.ConditionFalse, reason, message, r.timeFunc())
	newState := apimeta.FindStatusCondition(serviceSet.Status.Conditions, kcmv1.ServiceSetProviderAppliedCondition)
	if conditionStatusChangedToFalse(oldState, newState) || conditionReasonChanged(oldState, newState) {
		record.Warnf(serviceSet, nil, kcmv1.ServiceSetProviderApplyFailedEvent, kcmv1.ServiceSetApplyServicesEventAction,
			"Provider failed to apply services for ServiceSet %s: %s: %s", serviceSet.Name, reason, message)
	}
	return fmt.Errorf("provider did not apply services: %s: %s", reason, message)
}

// setStatusesCollected records whether the provider collected the service
// statuses and returns an error when it did not, so the reconcile is retried.
func (r *ServiceSetReconciler) setStatusesCollected(serviceSet *kcmv1.ServiceSet, res provider.DeployServicesResult) error {
	condition := findCondition(serviceSet, kcmv1.ServiceSetStatusesCollectedCondition)
	initialStatus := condition.Status
	oldState := apimeta.FindStatusCondition(serviceSet.Status.Conditions, kcmv1.ServiceSetStatusesCollectedCondition).DeepCopy()

	if res.Collected {
		updated := updateCondition(serviceSet, condition, metav1.ConditionTrue, kcmv1.ServiceSetStatusesCollectedReason,
			kcmv1.ServiceSetStatusesCollectedMessage, r.timeFunc())
		if initialStatus != metav1.ConditionTrue && updated {
			record.Eventf(serviceSet, nil, kcmv1.ServiceSetCollectServiceStatusesSuccessEvent, kcmv1.ServiceSetCollectServiceStatusesEventAction,
				"Successfully collected service statuses for ServiceSet %s", serviceSet.Name)
		}
		return nil
	}

	collectMessage := pointerutil.Deref(res.CollectMessage, kcmv1.ServiceSetStatusesNotCollectedMessage)
	updateCondition(serviceSet, condition, metav1.ConditionFalse, kcmv1.ServiceSetStatusesNotCollectedReason,
		kcmv1.ServiceSetStatusesNotCollectedMessage, r.timeFunc())
	newState := apimeta.FindStatusCondition(serviceSet.Status.Conditions, kcmv1.ServiceSetStatusesCollectedCondition)
	if conditionStatusChangedToFalse(oldState, newState) {
		record.Warnf(serviceSet, nil, kcmv1.ServiceSetCollectServiceStatusesFailedEvent, kcmv1.ServiceSetCollectServiceStatusesEventAction,
			"Failed to collect Service statuses for ServiceSet %s: %s", serviceSet.Name, collectMessage)
	}
	return fmt.Errorf("provider did not collect service statuses: %s", collectMessage)
}

// servicesFromProvider converts the services reported by the provider. The
// last deployed hash and the per-service conditions are owned by kcm's
// verifier, so they are kept from the current status.
func servicesFromProvider(current []kcmv1.ServiceState, reported []provider.ServiceState, now time.Time) []kcmv1.ServiceState {
	byKey := make(map[client.ObjectKey]kcmv1.ServiceState, len(current))
	for _, s := range current {
		byKey[client.ObjectKey{Namespace: s.Namespace, Name: s.Name}] = s
	}

	services := make([]kcmv1.ServiceState, 0, len(reported))
	for _, s := range reported {
		state := kcmv1.ServiceState{
			Name:           s.Name,
			Namespace:      s.Namespace,
			Template:       s.Template,
			Type:           kcmv1.ServiceType(s.Type),
			State:          string(s.State),
			Version:        pointerutil.Deref(s.Version, ""),
			FailureMessage: pointerutil.Deref(s.FailureMessage, ""),
		}
		prev, known := byKey[client.ObjectKey{Namespace: s.Namespace, Name: s.Name}]
		if known {
			state.LastDeployedHash = prev.LastDeployedHash
			state.Conditions = prev.Conditions
		}
		// the contract leaves the transition time optional while the CRD requires it.
		switch {
		case s.LastStateTransitionTime != nil:
			state.LastStateTransitionTime = new(metav1.NewTime(*s.LastStateTransitionTime))
		case known && prev.LastStateTransitionTime != nil:
			state.LastStateTransitionTime = prev.LastStateTransitionTime
		default:
			state.LastStateTransitionTime = new(metav1.NewTime(now))
		}
		services = append(services, state)
	}
	return services
}

// targetClusterExists reports whether the cluster serviceSet targets exists,
// looking it up the same way the child client is resolved.
func targetClusterExists(ctx context.Context, rgnClient client.Client, serviceSet *kcmv1.ServiceSet) (bool, error) {
	if serviceSet.Spec.Provider.SelfManagement {
		return true, nil
	}

	clusterKey := client.ObjectKey{Namespace: serviceSet.Namespace, Name: serviceSet.Spec.Cluster}
	err := rgnClient.Get(ctx, kubeutil.GetKubeconfigSecretKey(clusterKey), new(corev1.Secret))
	if !apierrors.IsNotFound(err) {
		return err == nil, err
	}
	err = rgnClient.Get(ctx, clusterKey, new(libsveltosv1beta1.SveltosCluster))
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

// deleteServices asks the provider to delete serviceSet's services
// and reports whether they are gone.
func (r *ServiceSetReconciler) deleteServices(ctx context.Context, serviceSet *kcmv1.ServiceSet) (bool, error) {
	l := ctrl.LoggerFrom(ctx)
	entry, ok := r.Registry.Lookup(serviceSet.Spec.Provider.Name)
	if !ok || entry.State == registryserver.StateSuspended {
		l.Info("Provider is not available, waiting to delete services", "provider", serviceSet.Spec.Provider.Name)
		return false, nil
	}

	c, err := r.NewProviderClient(entry)
	if err != nil {
		return false, fmt.Errorf("failed to build client for provider %s: %w", entry.Name, err)
	}
	res, err := c.DeleteServices(ctx, provider.DeleteServicesRequest{
		Name:      serviceSet.Name,
		Namespace: serviceSet.Namespace,
	})
	if err != nil {
		logProviderError(ctx, err)
		return false, fmt.Errorf("failed to delete services with provider %s: %w", entry.Name, err)
	}
	return res.Done, nil
}

func logProviderError(ctx context.Context, err error) {
	if pe, ok := errors.AsType[*providerclient.ProblemError](err); ok {
		ctrl.LoggerFrom(ctx).Info("Provider call failed", "type", pe.Type, "status", pe.Status, "detail", pe.Detail)
	}
}

// requestsForProvider maps a provider registry transition to the provider's ServiceSets.
func (r *ServiceSetReconciler) requestsForProvider(ctx context.Context, o client.Object) ([]ctrl.Request, error) {
	selector := fields.OneTermEqualSelector(kcmv1.ServiceSetProviderIndexKey, o.GetName())
	serviceSets := new(kcmv1.ServiceSetList)
	if err := r.List(ctx, serviceSets, client.MatchingFieldsSelector{Selector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list ServiceSets for provider %s: %w", o.GetName(), err)
	}
	requests := make([]ctrl.Request, 0, len(serviceSets.Items))
	for _, serviceSet := range serviceSets.Items {
		requests = append(requests, ctrl.Request{
			Name:      serviceSet.Name,
			Namespace: serviceSet.Namespace,
		})
	}
	return requests, nil
}

// requestForEvent maps a provider state-change notification to its ServiceSet.
func requestForEvent(_ context.Context, o client.Object) []ctrl.Request {
	ev, ok := o.(*corev1.Event)
	if !ok {
		return nil
	}
	nn, ok := notify.RequestFor(ev)
	if !ok {
		return nil
	}
	return []ctrl.Request{{NamespacedName: nn}}
}
