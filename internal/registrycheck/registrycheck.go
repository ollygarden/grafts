// Package registrycheck holds the registry checks that cannot be Rego
// policies.
//
// A Weaver policy is handed the resolved registry and nothing else, so a rule
// that compares the registry against a component's component.yaml has no way to
// see half of what it needs. Those checks live here instead, run from the
// toolchain with both files in hand.
package registrycheck

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	yaml "go.yaml.in/yaml/v3"
)

// resolved is the part of Weaver's resolved output these checks read.
type resolved struct {
	SchemaURL   string `json:"schema_url"`
	Refinements struct {
		Metrics []metric `json:"metrics"`
	} `json:"refinements"`
	Registry struct {
		Metrics []metric `json:"metrics"`
	} `json:"registry"`
}

type metric struct {
	Name       string      `json:"name"`
	Stability  string      `json:"stability"`
	Attributes []attribute `json:"attributes"`
	Provenance provenance  `json:"provenance"`
}

type attribute struct {
	Key        string     `json:"key"`
	Stability  string     `json:"stability"`
	Provenance provenance `json:"provenance"`
}

type provenance struct {
	Source string `json:"source"`
	Path   string `json:"path"`
}

// component is the part of component.yaml these checks read.
type component struct {
	Component string `yaml:"component"`
	Stability struct {
		SemconvVersion   string   `yaml:"semconv_version"`
		Stable           []string `yaml:"stable"`
		ReleaseCandidate []string `yaml:"release_candidate"`
		Development      []string `yaml:"development"`
	} `yaml:"stability"`
}

// StabilityDisclosure reports every upstream convention a component depends on
// whose stability is below stable and which its component.yaml does not list.
//
// The point is not bookkeeping. Four of the ten components in this program
// target conventions that can still be renamed, and a component leaning on one
// inherits its churn: its README must not present it as settled, and a semconv
// bump has to put it back on the review list. An undisclosed dependency is one
// nobody re-reviews.
//
// Returns nil when everything is disclosed.
func StabilityDisclosure(resolvedJSON, componentYAML []byte) error {
	var r resolved
	if err := json.Unmarshal(resolvedJSON, &r); err != nil {
		return fmt.Errorf("reading the resolved registry: %w", err)
	}
	var c component
	if err := yaml.Unmarshal(componentYAML, &c); err != nil {
		return fmt.Errorf("reading component.yaml: %w", err)
	}

	disclosed := map[string]bool{}
	for _, level := range [][]string{c.Stability.Stable, c.Stability.ReleaseCandidate, c.Stability.Development} {
		for _, name := range level {
			disclosed[name] = true
		}
	}

	undisclosed := map[string]string{}
	for _, list := range [][]metric{r.Registry.Metrics, r.Refinements.Metrics} {
		for _, m := range list {
			if !ours(m.Provenance) {
				continue
			}
			// An upstream convention this registry refines: the metric name is
			// upstream's, so its stability is upstream's too.
			if isUpstreamName(m.Name, c.Component) && needsDisclosure(m.Stability) && !disclosed[m.Name] {
				undisclosed[m.Name] = m.Stability
			}
			for _, a := range m.Attributes {
				if a.Provenance.Source == "" || !needsDisclosure(a.Stability) || disclosed[a.Key] {
					continue
				}
				undisclosed[a.Key] = a.Stability
			}
		}
	}

	if len(undisclosed) == 0 {
		return nil
	}

	names := make([]string, 0, len(undisclosed))
	for name := range undisclosed {
		names = append(names, name)
	}
	slices.Sort(names)

	var b strings.Builder
	fmt.Fprintf(&b, "component.yaml does not disclose %d upstream convention(s) below stable:\n", len(names))
	for _, name := range names {
		fmt.Fprintf(&b, "  %s (%s)\n", name, undisclosed[name])
	}
	fmt.Fprint(&b, "Add each under the matching stability level in component.yaml, and say so in the README:\n")
	fmt.Fprintf(&b, "a convention that can still be renamed must not be presented to users as settled.")
	return fmt.Errorf("%s", b.String())
}

// ours reports whether an entry was authored in the component's own registry
// rather than resolved from a dependency.
func ours(p provenance) bool {
	return p.Source == "" && strings.HasPrefix(p.Path, "/registry/")
}

// isUpstreamName reports whether a metric name belongs to upstream rather than
// to the component. A grafts registry declares only under the component's own
// prefix, so anything else it carries is a refinement of an upstream metric.
func isUpstreamName(name, componentName string) bool {
	system := strings.TrimSuffix(strings.TrimSuffix(componentName, "receiver"), "exporter")
	return !strings.HasPrefix(name, componentName+".") && !strings.HasPrefix(name, system+".")
}

func needsDisclosure(stability string) bool {
	switch stability {
	case "", "stable":
		return false
	default:
		return true
	}
}
