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
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	addoncontrollerv1beta1 "github.com/projectsveltos/addon-controller/api/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clusterapiv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
)

const (
	adapterAPIVersion = "sample-version/v1"
	adapterKind       = "SampleAdapter"
	adapterName       = "sample-adapter"
	adapterNamespace  = "sample-namespace"

	provisionerAPIVersion = "sample-version/v1"
	provisionerKind       = "SampleProvisioner"
	provisionerName       = "sample-provisioner"
	provisionerNamespace  = "sample-namespace"

	provisionerCRDGroup    = "sample-crd-group"
	provisionerCRDResource = "sample-crd-resources"
)

var testLabel = map[string]string{"integration-test": "true"}

var _ = Describe("ServiceSet Controller integration tests", Ordered, func() {
	var (
		reconciler ServiceSetReconciler

		namespace               corev1.Namespace
		credential              kcmv1.Credential
		clusterDeployment       kcmv1.ClusterDeployment
		serviceSet              kcmv1.ServiceSet
		stateManagementProvider kcmv1.StateManagementProvider

		cluster clusterapiv1.Cluster

		profile addoncontrollerv1beta1.Profile
	)

	BeforeEach(func() {
		By("creating a namespace", func() {
			namespace = corev1.Namespace{GenerateName: "namespace-test-"}
			Expect(cl.Create(ctx, &namespace)).To(Succeed())
		})

		By("creating a StateManagementProvider", func() {
			stateManagementProvider = prepareStateManagementProvider()
			Expect(cl.Create(ctx, &stateManagementProvider)).To(Succeed())
		})

		By("creating a Credential", func() {
			credential = prepareCredential(namespace.Name)
			Expect(cl.Create(ctx, &credential)).To(Succeed())
		})

		By("creating a ClusterDeployment", func() {
			clusterDeployment = prepareClusterDeployment(namespace.Name, credential.Name)
			Expect(cl.Create(ctx, &clusterDeployment)).To(Succeed())
		})

		By("creating a CAPI Cluster", func() {
			cluster = prepareCAPICluster(clusterDeployment.Name, clusterDeployment.Namespace)
			Expect(cl.Create(ctx, &cluster)).To(Succeed())
		})

		By("creating a ServiceSet", func() {
			serviceSet = prepareServiceSet(namespace.Name, stateManagementProvider.Name, clusterDeployment.Name)
			Expect(cl.Create(ctx, &serviceSet)).To(Succeed())
		})

		By("creating reconciler", func() {
			reconciler = ServiceSetReconciler{
				Client:          cl,
				timeFunc:        func() time.Time { return time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC) },
				requeueInterval: 1 * time.Second,
			}
		})
	})

	AfterEach(func() {
		Expect(client.IgnoreNotFound(cl.Delete(ctx, &serviceSet))).To(Succeed())
		Expect(client.IgnoreNotFound(cl.Delete(ctx, &clusterDeployment))).To(Succeed())
		Expect(client.IgnoreNotFound(cl.Delete(ctx, &credential))).To(Succeed())
		Expect(client.IgnoreNotFound(cl.Delete(ctx, &stateManagementProvider))).To(Succeed())
		Expect(client.IgnoreNotFound(cl.Delete(ctx, &namespace))).To(Succeed())
	})

	Context("When StateManagementProvider is not ready", func() {
		It("should only update the status of the ServiceSet", func() {
			By("checking the StateManagementProvider is not ready", func() {
				Expect(Object(&stateManagementProvider)()).Should(SatisfyAll(
					HaveField("Status.Ready", BeFalse()),
				))
			})

			By("reconciling the ServiceSet", func() {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&serviceSet)})
				Expect(err).To(Succeed())
				Expect(Object(&serviceSet)()).Should(SatisfyAll(
					HaveField("Status.Provider.Ready", BeFalse()),
					HaveField("Status.Provider.Suspended", BeFalse()),
					HaveField("Status.Cluster.APIVersion", kcmv1.GroupVersion.WithKind(kcmv1.ClusterDeploymentKind).GroupVersion().String()),
					HaveField("Status.Cluster.Kind", kcmv1.ClusterDeploymentKind),
					HaveField("Status.Cluster.Name", clusterDeployment.Name),
					HaveField("Status.Cluster.Namespace", clusterDeployment.Namespace),
				))

				profile = addoncontrollerv1beta1.Profile{}
				err = cl.Get(ctx, client.ObjectKeyFromObject(&serviceSet), &profile)
				Expect(err).To(HaveOccurred())
				Expect(apierrors.IsNotFound(err)).To(BeTrue())
			})
		})
	})

	Context("When StateManagementProvider is suspended", func() {
		It("should only update the status of the ServiceSet", func() {
			By("updating the StateManagementProvider to be ready and suspended", func() {
				stateManagementProvider.Spec.Suspend = true
				Expect(cl.Update(ctx, &stateManagementProvider)).To(Succeed())
				stateManagementProvider.Status.Ready = true
				Expect(cl.Status().Update(ctx, &stateManagementProvider)).To(Succeed())
				Expect(Object(&stateManagementProvider)()).Should(SatisfyAll(
					HaveField("Spec.Suspend", BeTrue()),
					HaveField("Status.Ready", BeTrue()),
				))
			})

			By("reconciling the ServiceSet", func() {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&serviceSet)})
				Expect(err).To(Succeed())
				Expect(Object(&serviceSet)()).Should(SatisfyAll(
					HaveField("Status.Provider.Ready", BeTrue()),
					HaveField("Status.Provider.Suspended", BeTrue()),
					HaveField("Status.Cluster.APIVersion", kcmv1.GroupVersion.WithKind(kcmv1.ClusterDeploymentKind).GroupVersion().String()),
					HaveField("Status.Cluster.Kind", kcmv1.ClusterDeploymentKind),
					HaveField("Status.Cluster.Name", clusterDeployment.Name),
					HaveField("Status.Cluster.Namespace", clusterDeployment.Namespace),
				))

				profile = addoncontrollerv1beta1.Profile{}
				err = cl.Get(ctx, client.ObjectKeyFromObject(&serviceSet), &profile)
				Expect(err).To(HaveOccurred())
				Expect(apierrors.IsNotFound(err)).To(BeTrue())
			})
		})
	})

	Context("When ServiceSet does not exist in the cluster", func() {
		It("should return nil without error", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				Name: "non-existent-ss", Namespace: namespace.Name,
			})
			Expect(err).To(Succeed())
		})
	})

	Context("When ServiceSet has no finalizer on first reconcile", func() {
		It("should add the finalizer and return without further processing", func() {
			noFinalizerSS := kcmv1.ServiceSet{
				GenerateName: "ss-no-fin-",
				Namespace:    namespace.Name,
				Labels:       testLabel,
				Spec: kcmv1.ServiceSetSpec{
					Cluster: clusterDeployment.Name,
					Provider: kcmv1.StateManagementProviderConfig{
						Name: stateManagementProvider.Name,
					},
				},
			}
			Expect(cl.Create(ctx, &noFinalizerSS)).To(Succeed())

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&noFinalizerSS)})
			Expect(err).To(Succeed())

			Expect(Object(&noFinalizerSS)()).Should(
				HaveField("Finalizers", ContainElement(kcmv1.ServiceSetFinalizer)),
			)

			noFinalizerSS.Finalizers = nil
			Expect(cl.Update(ctx, &noFinalizerSS)).To(Succeed())
			Expect(cl.Delete(ctx, &noFinalizerSS)).To(Succeed())
		})
	})

	Context("When ServiceSet labels do not match the StateManagementProvider selector", func() {
		It("should skip reconciliation without creating a Profile", func() {
			By("marking StateManagementProvider as ready", func() {
				stateManagementProvider.Status.Ready = true
				Expect(cl.Status().Update(ctx, &stateManagementProvider)).To(Succeed())
			})

			By("clearing ServiceSet labels so they no longer match the provider selector", func() {
				serviceSet.Labels = nil
				Expect(cl.Update(ctx, &serviceSet)).To(Succeed())
			})

			By("reconciling the ServiceSet", func() {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&serviceSet)})
				Expect(err).To(Succeed())
			})

			By("verifying no Profile was created", func() {
				prof := addoncontrollerv1beta1.Profile{}
				err := cl.Get(ctx, client.ObjectKeyFromObject(&serviceSet), &prof)
				Expect(apierrors.IsNotFound(err)).To(BeTrue())
			})
		})
	})

	Context("When ServiceSet is deleted with no Profile and empty service list", func() {
		PIt("should remove the finalizer and allow object deletion", func() {
			By("deleting the ServiceSet to trigger reconcileDelete", func() {
				Expect(cl.Delete(ctx, &serviceSet)).To(Succeed())
			})

			By("reconciling the deleted ServiceSet", func() {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&serviceSet)})
				Expect(err).To(Succeed())
			})

			By("verifying the ServiceSet is eventually deleted", func() {
				Eventually(func() bool {
					ss := &kcmv1.ServiceSet{}
					return apierrors.IsNotFound(cl.Get(ctx, client.ObjectKeyFromObject(&serviceSet), ss))
				}).Should(BeTrue())
			})
		})
	})

	Context("When ServiceSet is deleted but has services in non-Deleting state", func() {
		It("should transition service states to Deleting and requeue", func() {
			now := metav1.Now()
			By("seeding a service status on the ServiceSet", func() {
				serviceSet.Status.Services = []kcmv1.ServiceState{
					{
						Name:                    "test-service",
						Type:                    kcmv1.ServiceTypeHelm,
						State:                   kcmv1.ServiceStateProvisioning,
						Template:                "test-template",
						LastStateTransitionTime: &now,
					},
				}
				Expect(cl.Status().Update(ctx, &serviceSet)).To(Succeed())
			})

			By("deleting the ServiceSet", func() {
				Expect(cl.Delete(ctx, &serviceSet)).To(Succeed())
			})

			By("reconciling the deleted ServiceSet", func() {
				result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&serviceSet)})
				Expect(err).To(Succeed())
				// reconcileDelete returns immediately after updating statuses
				Expect(result.RequeueAfter).To(BeZero())
			})

			By("verifying services transitioned to Deleting state", func() {
				ss := &kcmv1.ServiceSet{}
				Expect(cl.Get(ctx, client.ObjectKeyFromObject(&serviceSet), ss)).To(Succeed())
				Expect(ss.Status.Services).To(HaveLen(1))
				Expect(ss.Status.Services[0].State).To(Equal(kcmv1.ServiceStateDeleting))
			})

			By("cleaning up: remove finalizer so AfterEach can delete the ServiceSet", func() {
				ss := &kcmv1.ServiceSet{}
				Expect(cl.Get(ctx, client.ObjectKeyFromObject(&serviceSet), ss)).To(Succeed())
				ss.Finalizers = nil
				Expect(cl.Update(ctx, ss)).To(Succeed())
			})
		})
	})

	// Regression coverage for reviewer comment
	// https://github.com/k0rdent/kcm/pull/2891#discussion_r3596677308 — the
	// verifier must never promote a Provisioning service to Deployed when
	// the sveltos-side fingerprint has advanced past what we last confirmed
	// on cluster, because the healthy pods the verifier just observed may
	// be stale pre-rollout pods.
	Context("Verifier state decision — hash-gated promotion", func() {
		const (
			releaseName  = "verifier-hashgate"
			specVersion  = "1.1.0"
			providerHash = "sha256:confirmed-hash"
		)

		// releaseNs is set per-It to the ServiceSet's own namespace so the
		// helper cleans up alongside the outer AfterEach. Envtest cannot
		// actually finish namespace teardown (no ns-lifecycle controller),
		// so reusing a shared release namespace would leak state across
		// test cases.
		var releaseNs string

		// prepareVerifierArtifacts wires up everything verifyServiceStates
		// needs: rules ConfigMap, healthy target Deployment and the child
		// kubeconfig. Returns the service hash the provider reports —
		// callers pre-seed LastDeployedHash to make the hash match or
		// mismatch as required by the test case.
		prepareVerifierArtifacts := func() string {
			releaseNs = namespace.Name

			By("targeting the reconciler at the ServiceSet's namespace for tier-1 rules", func() {
				reconciler.SystemNamespace = namespace.Name
			})

			By("creating a healthy Deployment labeled with the release name", func() {
				var replicas int32 = 1
				d := &appsv1.Deployment{
					Name:       releaseName,
					Namespace:  releaseNs,
					Generation: 1,
					Labels:     map[string]string{"release": releaseName},
					Spec: appsv1.DeploymentSpec{
						Replicas: &replicas,
						Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": releaseName}},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": releaseName}},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
							},
						},
					},
				}
				Expect(cl.Create(ctx, d)).To(Succeed())
				d.Status = appsv1.DeploymentStatus{
					ObservedGeneration: 1,
					Replicas:           1,
					ReadyReplicas:      1,
					UpdatedReplicas:    1,
				}
				Expect(cl.Status().Update(ctx, d)).To(Succeed())
				DeferCleanup(cl.Delete, d)
			})

			By("creating a namespace-global rules ConfigMap", func() {
				cm := &corev1.ConfigMap{
					Name:      "verifier-hashgate-rules",
					Namespace: namespace.Name,
					Labels:    map[string]string{healthRuleTargetLabel: healthRuleTargetGlobal},
					Data:      map[string]string{healthRuleConfigMapDataKey: testRulesYAML},
				}
				Expect(cl.Create(ctx, cm)).To(Succeed())
				DeferCleanup(cl.Delete, cm)
			})

			By("creating the CAPI kubeconfig Secret that resolveChildClient reads", func() {
				// resolveChildClient always dereferences the "<cluster>-kubeconfig"
				// Secret on rgnClient (which in this test is the envtest apiserver).
				// Point the child kubeconfig back at the same apiserver so
				// DefaultClientFactory builds a working client — collapsing all
				// three logical clusters (mgmt / regional / child) onto one
				// envtest is the standard shape for this test suite.
				kc := &corev1.Secret{
					Name:      clusterDeployment.Name + "-kubeconfig",
					Namespace: namespace.Name,
					Data:      map[string][]byte{"value": kubeconfigForRestConfig(config)},
				}
				Expect(cl.Create(ctx, kc)).To(Succeed())
				DeferCleanup(cl.Delete, kc)
			})

			return providerHash
		}

		providerHashes := func() map[client.ObjectKey]string {
			return map[client.ObjectKey]string{{Namespace: releaseNs, Name: releaseName}: providerHash}
		}

		// seedServiceStatus prepares the in-memory ServiceSet with a Helm
		// service in the given state, so verifyServiceStates has a target
		// to iterate. Spec.Services carries the "next intended version"
		// used by the stamp block.
		seedServiceStatus := func(state, lastDeployedHash, currentVersion string) {
			serviceSet.Spec.Services = []kcmv1.ServiceWithValues{{
				Name:      releaseName,
				Namespace: releaseNs,
				Version:   specVersion,
			}}
			curVer := currentVersion
			serviceSet.Status.Services = []kcmv1.ServiceState{{
				Type:             kcmv1.ServiceTypeHelm,
				Name:             releaseName,
				Namespace:        releaseNs,
				State:            state,
				LastDeployedHash: lastDeployedHash,
				Version:          curVer,
			}}
		}

		It("Case A: promotes on hash match (unrelated apply demoted us)", func() {
			expectedHash := prepareVerifierArtifacts()
			// Sveltos-side hash equals what we last confirmed → the pods
			// the verifier sees ARE the confirmed version → safe to
			// promote back from the aggregate-demoted Provisioning.
			seedServiceStatus(kcmv1.ServiceStateProvisioning, expectedHash, "1.0.0")

			pendingStamp, err := reconciler.verifyServiceStates(ctx, cl, &serviceSet, providerHashes())
			Expect(err).To(Succeed())
			Expect(pendingStamp).To(BeFalse(), "hash already stamped — no requeue needed")

			svc := serviceSet.Status.Services[0]
			Expect(svc.State).To(Equal(kcmv1.ServiceStateDeployed), "should promote to Deployed on hash match")
			Expect(svc.LastDeployedHash).To(Equal(expectedHash), "hash unchanged")
			Expect(svc.Version).To(Equal("1.0.0"), "version unchanged — no stamp on hash match")
		})

		It("Case B: does NOT promote when hash advanced (our real apply in progress)", func() {
			_ = prepareVerifierArtifacts()
			// Sveltos-side hash has moved past what we last confirmed →
			// sveltos is applying a new version of THIS service. The
			// healthy pods the verifier just observed could be stale
			// previous-version pods. Refuse to promote.
			staleHash := "sha256:previous-confirmed-hash"
			seedServiceStatus(kcmv1.ServiceStateProvisioning, staleHash, "1.0.0")

			pendingStamp, err := reconciler.verifyServiceStates(ctx, cl, &serviceSet, providerHashes())
			Expect(err).To(Succeed())
			Expect(pendingStamp).To(BeFalse(), "service stays Provisioning; pendingStamp only fires on Deployed with empty hash")

			svc := serviceSet.Status.Services[0]
			Expect(svc.State).To(Equal(kcmv1.ServiceStateProvisioning), "must stay Provisioning when hash advanced")
			Expect(svc.LastDeployedHash).To(Equal(staleHash), "hash must NOT advance without sveltos-side confirmation")
			Expect(svc.Version).To(Equal("1.0.0"), "version must not advance to spec value prematurely")
		})

		It("Case C: stamps hash+version when both sveltos and verifier agree on new hash", func() {
			expectedHash := prepareVerifierArtifacts()
			// Sveltos already says Deployed, verifier confirms healthy,
			// hash has advanced since last confirmed. This is the "both
			// authoritative signals agree" intersection — safe to record
			// the new version as landed.
			staleHash := "sha256:previous-confirmed-hash"
			seedServiceStatus(kcmv1.ServiceStateDeployed, staleHash, "1.0.0")

			pendingStamp, err := reconciler.verifyServiceStates(ctx, cl, &serviceSet, providerHashes())
			Expect(err).To(Succeed())
			Expect(pendingStamp).To(BeFalse(), "stamp fired this round — no requeue needed")

			svc := serviceSet.Status.Services[0]
			Expect(svc.State).To(Equal(kcmv1.ServiceStateDeployed), "both agreed → stays Deployed")
			Expect(svc.LastDeployedHash).To(Equal(expectedHash), "hash advances to sveltos-side fingerprint")
			Expect(svc.Version).To(Equal(specVersion), "version stamps to Spec.Services[i].Version")
		})
	})
})

func prepareStateManagementProvider() kcmv1.StateManagementProvider {
	return kcmv1.StateManagementProvider{
		GenerateName: "state-management-provider-",
		Spec: kcmv1.StateManagementProviderSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: testLabel,
			},
			Adapter: kcmv1.ResourceReference{
				APIVersion: adapterAPIVersion,
				Kind:       adapterKind,
				Name:       adapterName,
				Namespace:  adapterNamespace,
				ReadinessRule: `self.status.availableReplicas == self.status.replicas &&
self.status.availableReplicas == self.status.updatedReplicas &&
self.status.availableReplicas == self.status.readyReplicas`,
			},
			Provisioner: []kcmv1.ResourceReference{
				{
					APIVersion: provisionerAPIVersion,
					Kind:       provisionerKind,
					Name:       provisionerName,
					Namespace:  provisionerNamespace,
					ReadinessRule: `self.status.availableReplicas == self.status.replicas &&
self.status.availableReplicas == self.status.updatedReplicas &&
self.status.availableReplicas == self.status.readyReplicas`,
				},
			},
			ProvisionerCRDs: []kcmv1.ProvisionerCRD{
				{
					Group:   provisionerCRDGroup,
					Version: "v1",
					Resources: []string{
						provisionerCRDResource,
					},
				},
			},
		},
	}
}

func prepareServiceSet(namespace, providerName, clusterName string) kcmv1.ServiceSet {
	return kcmv1.ServiceSet{
		GenerateName: "service-set-",
		Namespace:    namespace,
		Labels:       testLabel,
		Finalizers:   []string{kcmv1.ServiceSetFinalizer},
		Spec: kcmv1.ServiceSetSpec{
			Cluster: clusterName,
			Provider: kcmv1.StateManagementProviderConfig{
				Name: providerName,
			},
		},
	}
}

func prepareCredential(namespace string) kcmv1.Credential {
	return kcmv1.Credential{
		GenerateName: "test-credential-aws-",
		Namespace:    namespace,
		Spec: kcmv1.CredentialSpec{
			IdentityRef: &corev1.ObjectReference{
				APIVersion: "infrastructure.cluster.x-k8s.io/v1beta2",
				Kind:       "AWSClusterStaticIdentity",
				Name:       "foo",
			},
		},
	}
}

func prepareClusterDeployment(namespace, credentialName string) kcmv1.ClusterDeployment {
	return kcmv1.ClusterDeployment{
		GenerateName: "cluster-deployment-",
		Namespace:    namespace,
		Spec: kcmv1.ClusterDeploymentSpec{
			Template:   "sample-template",
			Credential: credentialName,
			Config: &apiextv1.JSON{
				Raw: []byte(`{"foo":"bar"}`),
			},
		},
	}
}

func prepareCAPICluster(name, namespace string) clusterapiv1.Cluster {
	return clusterapiv1.Cluster{
		Name:      name,
		Namespace: namespace,
		Spec:      clusterapiv1.ClusterSpec{Paused: new(false)},
	}
}

// kubeconfigForRestConfig synthesises a kubeconfig YAML pointing at the
// given rest.Config. Used by verifier tests to seed the CAPI
// "<cluster>-kubeconfig" Secret that resolveChildClient reads —
// pointing back at the same envtest apiserver so DefaultClientFactory
// builds a working "child" client that queries the same fake used by
// the mgmt path.
func kubeconfigForRestConfig(cfg *rest.Config) []byte {
	const ctxName = "envtest"
	apiCfg := clientcmdapi.NewConfig()
	apiCfg.Clusters[ctxName] = &clientcmdapi.Cluster{
		Server:                   cfg.Host,
		CertificateAuthorityData: cfg.CAData,
		InsecureSkipTLSVerify:    cfg.Insecure,
	}
	apiCfg.AuthInfos[ctxName] = &clientcmdapi.AuthInfo{
		ClientCertificateData: cfg.CertData,
		ClientKeyData:         cfg.KeyData,
		Token:                 cfg.BearerToken,
	}
	apiCfg.Contexts[ctxName] = &clientcmdapi.Context{
		Cluster:  ctxName,
		AuthInfo: ctxName,
	}
	apiCfg.CurrentContext = ctxName
	out, err := clientcmd.Write(*apiCfg)
	if err != nil {
		panic(fmt.Errorf("serialize kubeconfig: %w", err))
	}
	return out
}
