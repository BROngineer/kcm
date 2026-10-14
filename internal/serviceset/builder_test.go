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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
)

func TestBuilderSelectorLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		existing map[string]string
		selector *metav1.LabelSelector
		want     map[string]string
	}{
		{
			name:     "selector labels are added",
			existing: map[string]string{"keep": "me"},
			selector: &metav1.LabelSelector{MatchLabels: map[string]string{"ksm.k0rdent.mirantis.com/adapter": "kcm-controller-manager"}},
			want:     map[string]string{"keep": "me", "ksm.k0rdent.mirantis.com/adapter": "kcm-controller-manager"},
		},
		{
			name:     "no selector keeps the existing labels",
			existing: map[string]string{"keep": "me"},
			want:     map[string]string{"keep": "me"},
		},
		{
			name: "no selector and no labels",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cd := &kcmv1.ClusterDeployment{Name: "cd", Namespace: "ns"}
			ss := &kcmv1.ServiceSet{Name: "ss", Namespace: "ns", Labels: tt.existing}

			got, err := NewBuilder(cd, ss, tt.selector).Build()
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Labels)
		})
	}
}
