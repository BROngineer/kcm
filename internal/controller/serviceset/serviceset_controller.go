// Copyright 2025
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

	registryserver "github.com/brongineer/ksm-provider-api/registry/server"
	libsveltosv1beta1 "github.com/projectsveltos/libsveltos/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
	"github.com/K0rdent/kcm/internal/record"
	"github.com/K0rdent/kcm/internal/serviceset"
	kubeutil "github.com/K0rdent/kcm/internal/util/kube"
	ratelimitutil "github.com/K0rdent/kcm/internal/util/ratelimit"
	schemeutil "github.com/K0rdent/kcm/internal/util/scheme"
)

// ServiceSetReconciler reconciles a ServiceSet object by delegating
// deployment of its services to the state management provider.
type ServiceSetReconciler struct {
	client.Client

	Registry          ProviderRegistry
	NewProviderClient ProviderClientFactory
	// Transitions carries provider registry transitions; the object name is the provider name.
	Transitions <-chan event.GenericEvent

	timeFunc func() time.Time

	SystemNamespace string

	MaxConcurrentReconciles int
	requeueInterval         time.Duration
}

func (r *ServiceSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	start := time.Now()
	l := ctrl.LoggerFrom(ctx)
	l.Info("Reconciling ServiceSet")

	serviceSet := new(kcmv1.ServiceSet)
	err = r.Get(ctx, req.NamespacedName, serviceSet)
	if apierrors.IsNotFound(err) {
		l.Info("ServiceSet not found, skipping")
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	rgnClient, err := getRegionalClient(ctx, r.Client, serviceSet, r.SystemNamespace)
	if err != nil {
		l.Error(err, "failed to get regional client")
		return ctrl.Result{}, err
	}

	if !serviceSet.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, serviceSet)
	}

	if controllerutil.AddFinalizer(serviceSet, kcmv1.ServiceSetFinalizer) {
		return ctrl.Result{}, r.Update(ctx, serviceSet)
	}

	providerName := serviceSet.Spec.Provider.Name
	entry, registered := r.Registry.Lookup(providerName)

	clone := serviceSet.DeepCopy()
	defer func() {
		if err == nil {
			// fillNotDeployedServices relies on the service type having
			// already been resolved for every service reachable from spec,
			// which only holds once the profile was built successfully.
			fillNotDeployedServices(serviceSet, r.timeFunc)
		}
		// we'll update serviceSet status even in case of an error occurred
		// during reconciliation, so that the failure is surfaced on the
		// object instead of being visible only in the reconciler logs.
		// By returning the original error, the object being reconciled
		// will still be requeued with respect of rate limits.
		if !equality.Semantic.DeepEqual(clone.Status, serviceSet.Status) {
			if updateErr := r.Status().Update(ctx, serviceSet); updateErr != nil {
				err = errors.Join(err, updateErr)
			}
		}
		l.Info("Finished reconciling ServiceSet", "duration", time.Since(start), "err", err)
	}()

	serviceSet.Status.Cluster = clusterReference(serviceSet)
	serviceSet.Status.Provider = kcmv1.ProviderState{
		Ready:     registered && entry.State == registryserver.StateActive,
		Suspended: registered && entry.State == registryserver.StateSuspended,
	}
	// the condition was owned by the in-tree Sveltos adapter, ProviderApplied replaces it.
	apimeta.RemoveStatusCondition(&serviceSet.Status.Conditions, kcmv1.ServiceSetProfileCondition)
	if !registered {
		// we'll emit StateManagementProviderNotReadyEvent
		// only in case the previous observed state was "ready".
		if clone.Status.Provider.Ready {
			record.Eventf(serviceSet, nil, kcmv1.StateManagementProviderNotReadyEvent, kcmv1.ServiceSetReconcileEventAction,
				"StateManagementProvider %s not ready, skipping ServiceSet %s reconciliation", providerName, serviceSet.Name)
		}
		l.Info("StateManagementProvider is not registered, skipping", "provider", serviceSet.Spec.Provider)
		return ctrl.Result{}, nil
	}
	if serviceSet.Status.Provider.Suspended {
		// we'll emit StateManagementProviderSuspendedEvent
		// only in case the previous observed state was not "suspended".
		if !clone.Status.Provider.Suspended {
			record.Eventf(serviceSet, nil, kcmv1.StateManagementProviderSuspendedEvent, kcmv1.ServiceSetReconcileEventAction,
				"StateManagementProvider %s suspended, skipping ServiceSet %s reconciliation", providerName, serviceSet.Name)
		}
		l.Info("StateManagementProvider is suspended, skipping", "provider", serviceSet.Spec.Provider)
		return ctrl.Result{}, nil
	}

	serviceHashes, err := r.deployServices(ctx, rgnClient, entry, serviceSet)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Verify on-cluster state for services that sveltos reports as
	// Deployed. The helm SDK can return success before pods are actually
	// ready, so we cross-check by listing labeled resources on the target
	// cluster and evaluating their state against the embedded rules.
	// Verification is best-effort: a failure here is logged but does not
	// fail the reconcile — keep the sveltos-reported state for that round.
	pendingStamp, verifyErr := r.verifyServiceStates(ctx, rgnClient, serviceSet, serviceHashes)
	if verifyErr != nil {
		ctrl.LoggerFrom(ctx).Error(verifyErr, "service state verification failed; keeping sveltos-reported state")
	}

	// A service ended up Deployed but we couldn't stamp its
	// LastDeployedHash because sveltos-side inputs weren't yet
	// available. Requeue so we can stamp on a subsequent round.
	if pendingStamp {
		return ctrl.Result{RequeueAfter: r.requeueInterval}, nil
	}

	return ctrl.Result{}, nil
}

// verifyServiceStates cross-checks sveltos's per-service state verdict
// against the actual state of resources on the target cluster. For each
// service in Status.Services whose Type is Helm, it (a) computes a stable
// fingerprint over chart identity + values + patches from the sveltos-side
// artifacts (ClusterSummary + ClusterConfiguration), (b) lists labeled
// resources on the target cluster and evaluates them against the loaded
// rules, and (c) decides the final state by combining sveltos's verdict
// with the on-cluster verdict and the hash gate.
//
// Decision table (sveltos verdict, verifier verdict, hash) → final state.
// State moves to Deployed only when BOTH signals agree; the stamp block
// below then advances LastDeployedHash+Version only when they agree AND
// the hash has actually moved:
//
//   - sveltos=*,            verifier!=Deployed            → Provisioning
//     (downgrade —
//     unconditional;
//     works around
//     helm SDK's
//     "deployed too
//     early" bug)
//   - sveltos=Provisioning, verifier=Deployed, hashMatch  → Deployed
//     (PROMOTE —
//     sveltos-side
//     pessimism from
//     aggregate Helm
//     feature status;
//     fingerprint
//     unchanged means
//     healthy resource are
//     the confirmed
//     version)
//   - sveltos=Provisioning, verifier=Deployed, hash!=     → Provisioning
//     (KEEP — real
//     apply in progress
//     for our own
//     service; healthy
//     resources may be stale
//     pre-rollout resources)
//   - sveltos=Deployed,     verifier=Deployed             → Deployed
//     (both agree; the
//     stamp block below
//     advances hash+
//     version if the
//     hash moved)
//   - sveltos=Failed/etc                                  → skipped upstream
//     (guard at s.State
//     check — verifier
//     never overrides
//     terminal states)
//
// Rules are loaded from three tiers of ConfigMaps (cluster-global,
// namespace-global, ServiceSet-targeted) — see rulesFromConfigMaps.
// Per-rule compile errors are surfaced on the ServiceSet's top-level
// condition ServiceSetHealthRules.
// The second return, pendingStamp, is true when at least one Helm
// service ended up in Deployed state without a stamped LastDeployedHash
// — typically because the fingerprint inputs weren't yet available on
// sveltos's side (ClusterConfiguration section not populated with the
// chart entry, or ClusterSummary missing the HelmReleaseSummary). The
// caller uses this to requeue and try stamping on a subsequent round
// rather than leaving the service with a permanent empty hash.
func (r *ServiceSetReconciler) verifyServiceStates(
	ctx context.Context,
	rgnClient client.Client,
	serviceSet *kcmv1.ServiceSet,
	serviceHashes map[client.ObjectKey]string,
) (bool, error) {
	if rgnClient == nil || len(serviceSet.Status.Services) == 0 {
		return false, nil
	}

	rules, loadErrs, err := rulesFromConfigMaps(ctx, r.Client, r.SystemNamespace, serviceSet)
	if err != nil {
		return false, fmt.Errorf("load health rules: %w", err)
	}
	applyRuleLoadErrorCondition(serviceSet, loadErrs)

	if len(rules) == 0 {
		// No rules — keep sveltos-reported state.
		return false, nil
	}

	// Index spec-side versions so the verifier can stamp Status.Version
	// with the spec value at the moment it confirms a deploy. The Helm
	// path makes Status.Version mean "verified on cluster" (not "what spec
	// says") — Kustomize / Resource still mirror spec eagerly in state.go.
	specVersions := make(map[client.ObjectKey]string, len(serviceSet.Spec.Services))
	for i := range serviceSet.Spec.Services {
		svc := &serviceSet.Spec.Services[i]
		specVersions[client.ObjectKey{Namespace: svc.Namespace, Name: svc.Name}] = svc.Version
	}

	childClient, err := getChildClient(ctx, r.Client, rgnClient, serviceSet)
	if err != nil {
		return false, fmt.Errorf("get child cluster client: %w", err)
	}

	l := ctrl.LoggerFrom(ctx)
	var errs error
	stateChanged := false
	pendingStamp := false

	for i := range serviceSet.Status.Services {
		s := &serviceSet.Status.Services[i]
		// Kustomize/Resource paths are not yet supported by the verifier:
		// sveltos does not stamp per-service identity on those.
		if s.Type != kcmv1.ServiceTypeHelm {
			continue
		}
		// Never override Failed / NotDeployed / Deleting / Deleted.
		if s.State != kcmv1.ServiceStateDeployed && s.State != kcmv1.ServiceStateProvisioning {
			continue
		}

		serviceKey := client.ObjectKey{Namespace: s.Namespace, Name: s.Name}
		currentHash := serviceHashes[serviceKey]

		// Combine sveltos's verdict with the verifier's on-cluster verdict:
		//
		//   1. Verifier says NOT healthy (unhealthy resources, or none found
		//      under the release labels) → downgrade to Provisioning and
		//      attach conditions. Unconditional — this is the whole point of
		//      the verifier, catching sveltos's premature "Deployed" from
		//      helm SDK's known "success too early" bug.
		//   2. Verifier says healthy AND sveltos says Provisioning AND the
		//      current fingerprint matches the last confirmed-Deployed one
		//      → promote to Deployed. Sveltos is only Provisioning because
		//      featureHelm's aggregate status was demoted by unrelated
		//      activity in the same ClusterProfile; our hash hasn't moved,
		//      so the pods the verifier sees ARE the confirmed version.
		//   3. Otherwise → keep sveltos's verdict. Critically, when the
		//      verifier says healthy but our hash HAS advanced, we do NOT
		//      promote from sveltos-Provisioning: the healthy pods may be
		//      stale previous-version pods still running before a rollout
		//      starts. We wait until sveltos itself confirms Deployed.
		verifierState, conds, err := verifyHelmServiceOnCluster(ctx, childClient, rules, s.Name, s.Namespace)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("verify service %s/%s: %w", s.Namespace, s.Name, err))
			continue
		}

		newState := s.State
		var newConds []metav1.Condition
		switch {
		case verifierState != kcmv1.ServiceStateDeployed:
			newState = kcmv1.ServiceStateProvisioning
			newConds = conds
		case s.State == kcmv1.ServiceStateProvisioning &&
			currentHash != "" && currentHash == s.LastDeployedHash:
			newState = kcmv1.ServiceStateDeployed
		}

		if newState == kcmv1.ServiceStateDeployed && currentHash == "" {
			newState = kcmv1.ServiceStateProvisioning
			pendingStamp = true
		}

		if newState != s.State {
			l.V(1).Info("verifier transitioning service state",
				"service", serviceKey,
				"from", s.State, "to", newState,
				"currentHash", currentHash, "lastDeployedHash", s.LastDeployedHash)
			s.State = newState
			now := metav1.NewTime(r.timeFunc())
			s.LastStateTransitionTime = &now
			stateChanged = true
		}
		applyVerifyConditions(s, newConds)

		// Stamp Status.Version and Status.LastDeployedHash on a hash
		// transition while Deployed. Under the switch above, `newState ==
		// Deployed` with `currentHash != LastDeployedHash` is only
		// reachable via the "sveltos=Deployed AND verifier=Deployed" path
		// — the promotion arm requires `currentHash == LastDeployedHash`,
		// which makes the hash-!= guard here false. So this stamp fires
		// only when BOTH sveltos and the verifier confirm Deployed AND
		// the fingerprint has actually advanced: the strongest agreement
		// we can express with the signals sveltos exposes.
		if newState == kcmv1.ServiceStateDeployed && currentHash != "" && currentHash != s.LastDeployedHash {
			s.LastDeployedHash = currentHash
			s.Version = specVersions[serviceKey]
		}
	}

	if stateChanged {
		serviceSet.Status.Deployed = !slices.ContainsFunc(serviceSet.Status.Services, func(s kcmv1.ServiceState) bool {
			return s.State != kcmv1.ServiceStateDeployed
		})
	}
	// updateServicesInReadyStateCondition ran earlier (from
	// collectServiceStatuses) over sveltos-reported states. After the
	// verifier mutates per-service states, that aggregate is stale —
	// recompute so the ServicesInReadyState condition agrees with
	// .status.services[*].state. Runs unconditionally: even when this round
	// transitioned nothing, the previous round may have left a stale count.
	r.updateServicesInReadyStateCondition(serviceSet)
	return pendingStamp, errs
}

func (r *ServiceSetReconciler) reconcileDelete(ctx context.Context, serviceSet *kcmv1.ServiceSet) (ctrl.Result, error) {
	l := ctrl.LoggerFrom(ctx)
	l.Info("Reconciling ServiceSet deletion")

	// we'll update the ServiceSet status to reflect the deletion of the services
	if slices.ContainsFunc(serviceSet.Status.Services, func(state kcmv1.ServiceState) bool {
		return state.State != kcmv1.ServiceStateDeleting
	}) {
		serviceStates := make([]kcmv1.ServiceState, 0, len(serviceSet.Status.Services))
		for _, state := range serviceSet.Status.Services {
			if state.State == kcmv1.ServiceStateDeleting {
				continue
			}
			newState := state.DeepCopy()
			newState.State = kcmv1.ServiceStateDeleting
			newState.LastStateTransitionTime = new(metav1.NewTime(r.timeFunc()))
			serviceStates = append(serviceStates, *newState)
		}
		serviceSet.Status.Services = serviceStates
		return ctrl.Result{}, r.Status().Update(ctx, serviceSet)
	}

	done, err := r.deleteServices(ctx, serviceSet)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !done {
		l.V(1).Info("Waiting for provider to delete services")
		return ctrl.Result{RequeueAfter: r.requeueInterval}, nil
	}

	// otherwise the provider has removed the services, so we can remove the finalizer
	if controllerutil.RemoveFinalizer(serviceSet, kcmv1.ServiceSetFinalizer) {
		if err := r.Update(ctx, serviceSet); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to remove finalizer: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ServiceSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Client = mgr.GetClient()
	if r.timeFunc == nil {
		r.timeFunc = time.Now
	}
	r.requeueInterval = 10 * time.Second

	// Events are not generation-aware, so no generation predicate: a repeated
	// notification only updates the count of the existing Event.
	b := ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.TypedOptions[ctrl.Request]{
			MaxConcurrentReconciles: 10,
			RateLimiter:             ratelimitutil.DefaultFastSlow(),
		}).
		Named("serviceset").
		For(&kcmv1.ServiceSet{}).
		Watches(&corev1.Event{}, handler.EnqueueRequestsFromMapFunc(requestForEvent))
	if r.Transitions != nil {
		b = b.WatchesRawSource(source.Channel(r.Transitions, kubeutil.EnqueueRequestsFromMapFunc(r.requestsForProvider)))
	}
	return b.Complete(r)
}

// updateServicesInReadyStateCondition updates the "ServicesInReadyState" condition.
func (r *ServiceSetReconciler) updateServicesInReadyStateCondition(serviceSet *kcmv1.ServiceSet) {
	deployedCount := 0
	for _, svc := range serviceSet.Status.Services {
		if svc.State == kcmv1.ServiceStateDeployed {
			deployedCount++
		}
	}
	totalCount := len(serviceSet.Spec.Services)
	svcReadyCond := findCondition(serviceSet, kcmv1.ServicesInReadyStateCondition)
	svcReadyStatus := metav1.ConditionTrue
	if deployedCount != totalCount {
		svcReadyStatus = metav1.ConditionFalse
	}

	// update condition only if deployed <= total to avoid scenarios where
	// a service is removed from spec but is still present in the status.
	if deployedCount <= totalCount {
		updateCondition(serviceSet, svcReadyCond, svcReadyStatus, kcmv1.ServicesInReadyStateCondition,
			fmt.Sprintf("%d/%d", deployedCount, totalCount), r.timeFunc())
	}
}

// fillNotDeployedServices fills [github.com/K0rdent/kcm/api/v1beta1.ServiceSet] status for
// services that are not deployed.
func fillNotDeployedServices(serviceSet *kcmv1.ServiceSet, now func() time.Time) {
	deployedServicesMap := make(map[client.ObjectKey]struct{})
	for _, service := range serviceSet.Status.Services {
		key := client.ObjectKey{
			Namespace: service.Namespace,
			Name:      service.Name,
		}
		deployedServicesMap[key] = struct{}{}
	}

	for _, service := range serviceSet.Spec.Services {
		key := client.ObjectKey{
			Namespace: service.Namespace,
			Name:      service.Name,
		}
		if _, ok := deployedServicesMap[key]; ok {
			continue
		}
		serviceSet.Status.Services = append(serviceSet.Status.Services, kcmv1.ServiceState{
			Name:                    service.Name,
			Namespace:               service.Namespace,
			Template:                service.Template,
			State:                   kcmv1.ServiceStateNotDeployed,
			LastStateTransitionTime: new(metav1.NewTime(now())),
		})
	}
}

// conditionStatusChangedToFalse returns true if the condition status changed to False
// while previous observed condition status was equal to True or was not defined.
func conditionStatusChangedToFalse(conditionOldState, conditionNewState *metav1.Condition) bool {
	if conditionOldState == nil && conditionNewState == nil {
		return false
	}
	if conditionOldState == nil && conditionNewState.Status == metav1.ConditionFalse {
		return true
	}
	return conditionOldState != nil && conditionNewState != nil && conditionOldState.Status != conditionNewState.Status &&
		conditionNewState.Status == metav1.ConditionFalse
}

func conditionReasonChanged(conditionOldState, conditionNewState *metav1.Condition) bool {
	if conditionOldState == nil && conditionNewState == nil {
		return false
	}
	if conditionNewState == nil {
		return false
	}
	if conditionOldState == nil {
		return true
	}
	return conditionOldState.Reason != conditionNewState.Reason
}

// findCondition finds the condition of the given type in the ServiceSet.
// If no condition is found, a new condition of given type is created.
func findCondition(serviceSet *kcmv1.ServiceSet, conditionType string) metav1.Condition {
	condition := apimeta.FindStatusCondition(serviceSet.Status.Conditions, conditionType)
	// if the condition is not found, create a new one with given type and status "False"
	if condition == nil {
		condition = &metav1.Condition{
			Type:               conditionType,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: serviceSet.Generation,
		}
	}
	return *condition
}

// updateCondition updates the given condition of the ServiceSet.
func updateCondition(
	serviceSet *kcmv1.ServiceSet,
	condition metav1.Condition,
	status metav1.ConditionStatus,
	reason string,
	message string,
	transitionTime time.Time,
) bool {
	if condition.Status != status || condition.Reason != reason || condition.Message != message {
		condition.LastTransitionTime = metav1.NewTime(transitionTime)
	}
	condition.ObservedGeneration = serviceSet.Generation
	condition.Status = status
	condition.Reason = reason
	condition.Message = message
	return apimeta.SetStatusCondition(&serviceSet.Status.Conditions, condition)
}

// getRegionalClient returns the kubernetes client for the cluster where
// serviceSet's workload runs: the local client cl for self-managed (empty
// .spec.cluster) ServiceSets, or the child (in relation to mothership)
// client otherwise.
func getRegionalClient(ctx context.Context, cl client.Client, serviceSet *kcmv1.ServiceSet, systemNamespace string) (client.Client, error) {
	return resolveRegionalClient(ctx, cl, serviceSet, systemNamespace, nil)
}

// resolveRegionalClient is the cache-aware variant of [getRegionalClient].
// Pass a non-nil cache (keyed by serviceSet's cluster object key) to dedupe
// ClusterDeployment/Credential reads and regional kubeconfig fetches within
// a single caller, e.g. one poller tick.
func resolveRegionalClient(
	ctx context.Context,
	cl client.Client,
	serviceSet *kcmv1.ServiceSet,
	systemNamespace string,
	cache map[client.ObjectKey]client.Client,
) (client.Client, error) {
	if serviceSet.Spec.Cluster == "" {
		// ServiceSet that self-manages the management cluster has no
		// matching ClusterDeployment, so the local client is the answer
		return cl, nil
	}

	clusterKey := client.ObjectKey{Namespace: serviceSet.Namespace, Name: serviceSet.Spec.Cluster}
	if cache != nil {
		if c, ok := cache[clusterKey]; ok {
			return c, nil
		}
	}

	cd := new(kcmv1.ClusterDeployment)
	if err := cl.Get(ctx, clusterKey, cd); err != nil {
		return nil, fmt.Errorf("failed to get ClusterDeployment %s: %w", clusterKey, err)
	}

	cred := new(kcmv1.Credential)
	credKey := client.ObjectKey{Namespace: cd.Namespace, Name: cd.Spec.Credential}
	if err := cl.Get(ctx, credKey, cred); err != nil {
		return nil, fmt.Errorf("failed to get Credential %s: %w", credKey, err)
	}

	rgn := cl
	if cred.Spec.Region != "" {
		var err error
		rgn, err = kubeutil.GetRegionalClientByRegionName(ctx, cl, systemNamespace, cred.Spec.Region, schemeutil.GetRegionalSchemeWithSveltos)
		if err != nil {
			return nil, fmt.Errorf("failed to get regional client for region %s: %w", cred.Spec.Region, err)
		}
	}

	if cache != nil {
		cache[clusterKey] = rgn
	}

	return rgn, nil
}

// getChildClient returns the kubernetes client for the cluster where
// serviceSet's workload runs: the local client cl for self-managed (empty
// .spec.cluster) ServiceSets, the regional (child in relation to mothership)
// client or the child (in relation to regional cluster).
func getChildClient(ctx context.Context, cl, rgnClient client.Client, serviceSet *kcmv1.ServiceSet) (client.Client, error) {
	return resolveChildClient(ctx, cl, rgnClient, serviceSet)
}

// resolveChildClient returns the kubernetes client for the cluster where
// serviceSet's workloads actually run. Three schemes are handled:
//
//   - self-management: no CAPI child exists; return mgmt (cl).
//   - flat (mgmt hosts CAPI) or regional (Region cluster hosts CAPI):
//     build the client using GetChildClient function.
func resolveChildClient(
	ctx context.Context,
	cl client.Client,
	rgnClient client.Client,
	serviceSet *kcmv1.ServiceSet,
) (client.Client, error) {
	if serviceSet.Spec.Provider.SelfManagement {
		return cl, nil
	}

	clusterKey := client.ObjectKey{Namespace: serviceSet.Namespace, Name: serviceSet.Spec.Cluster}
	secretRef := kubeutil.GetKubeconfigSecretKey(clusterKey)
	childCl, err := kubeutil.GetChildClient(ctx, rgnClient, secretRef, "value", rgnClient.Scheme(), kubeutil.DefaultClientFactory)
	if err == nil {
		return childCl, nil
	}
	// NotFound error could be returned in the only case - when the Secret containing
	// the kubeconfig is not found. This might happen if the cluster is an adopted cluster
	// and backed by the SveltosCluster object. In this case we'll need to lookup for the
	// corresponding SveltosCluster and get the secret key from it.
	// Hence we'll return error in case the error is not the NotFound error.
	if !apierrors.IsNotFound(err) {
		return nil, err
	}
	// Otherwise we'll fallback to sveltos cluster identity.
	sveltosSecretRef, key, err := resolveSveltosClusterIdentityRef(ctx, rgnClient, serviceSet)
	if err != nil {
		return nil, err
	}
	return kubeutil.GetChildClient(ctx, rgnClient, sveltosSecretRef, key, rgnClient.Scheme(), kubeutil.DefaultClientFactory)
}

func clusterReference(serviceSet *kcmv1.ServiceSet) *corev1.ObjectReference {
	if serviceSet.Spec.Provider.SelfManagement {
		return serviceset.SelfManagementClusterReference()
	}
	return &corev1.ObjectReference{
		Kind:       kcmv1.ClusterDeploymentKind,
		Name:       serviceSet.Spec.Cluster,
		Namespace:  serviceSet.Namespace,
		APIVersion: kcmv1.GroupVersion.WithKind(kcmv1.ClusterDeploymentKind).GroupVersion().String(),
	}
}

func resolveSveltosClusterIdentityRef(ctx context.Context, cl client.Client, serviceSet *kcmv1.ServiceSet) (client.ObjectKey, string, error) {
	var (
		secretRef client.ObjectKey
		secretKey string
	)
	sveltosClusterKey := client.ObjectKey{Namespace: serviceSet.Namespace, Name: serviceSet.Spec.Cluster}
	sveltosCluster := new(libsveltosv1beta1.SveltosCluster)
	if err := cl.Get(ctx, sveltosClusterKey, sveltosCluster); err != nil {
		return secretRef, "", fmt.Errorf("failed to get Sveltos cluster %s: %w", sveltosClusterKey, err)
	}

	secretRef = client.ObjectKey{Namespace: serviceSet.Namespace, Name: sveltosCluster.Name + "-sveltos-kubeconfig"}
	if sveltosCluster.Spec.KubeconfigName != "" {
		secretRef = client.ObjectKey{Namespace: serviceSet.Namespace, Name: sveltosCluster.Spec.KubeconfigName}
	}
	secretKey = sveltosCluster.Spec.KubeconfigKeyName
	// if the key containing kubeconfig is defined, we're good to return
	// otherwise we'll try to discover the key.
	if secretKey != "" {
		return secretRef, secretKey, nil
	}
	secret := new(corev1.Secret)
	if err := cl.Get(ctx, secretRef, secret); err != nil {
		return secretRef, "", fmt.Errorf("failed to get secret %s: %w", secretRef, err)
	}
	// since iteration over map keys is non-deterministic in Go, we can't rely on
	// Sveltos implementation to "get the first key in the Secret" (sveltoscluster_type.go, L229-232)
	// instead, we'll reject secrets where there are more than 1 key defined.
	if len(secret.Data) == 0 {
		return secretRef, secretKey, fmt.Errorf("secret %s has no data", secretRef)
	}
	if len(secret.Data) > 1 {
		return secretRef, secretKey, fmt.Errorf("secret %s has more than one key", secretRef)
	}
	for k := range secret.Data {
		secretKey = k
	}
	return secretRef, secretKey, nil
}
