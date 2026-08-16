package conformance

import (
	"fmt"
	"os"

	yaml "go.yaml.in/yaml/v3"
)

// Component is the part of a component's `telemetry/component.yaml` that the
// conformance harness needs.
//
// Loading it rather than restating it in Go keeps one source of truth for the
// parity contract: a harness that hardcodes its own namespace, drop list or
// floor can disagree with the file the reviewer reads, and the reviewer would
// have no way to tell.
type Component struct {
	Component string `yaml:"component"`
	System    string `yaml:"system"`

	Upstream struct {
		Repository string `yaml:"repository"`
		Version    string `yaml:"version"`
		Image      string `yaml:"image"`
		Digest     string `yaml:"digest"`
	} `yaml:"upstream"`

	Fixture struct {
		Target  Image `yaml:"target"`
		Backend Image `yaml:"backend"`
	} `yaml:"fixture"`

	Parity struct {
		// Floor is the parity the component must not fall below. Zero means
		// unset, which is only correct before the first measurement.
		Floor float64 `yaml:"floor"`
		// Namespace scopes the comparison to the target's own metrics.
		Namespace string `yaml:"namespace"`
	} `yaml:"parity"`

	Dropped []struct {
		Metric string `yaml:"metric"`
		Reason string `yaml:"reason"`
	} `yaml:"dropped"`
}

// Image is a container pinned by digest, so a parity number is measured against
// something that cannot move underneath it.
type Image struct {
	Image   string `yaml:"image"`
	Version string `yaml:"version"`
	Digest  string `yaml:"digest"`
}

// Ref renders the image as a digest-pinned reference.
func (i Image) Ref() string {
	return fmt.Sprintf("%s:%s@%s", i.Image, i.Version, i.Digest)
}

// Ref renders the upstream exporter image as a digest-pinned reference.
func (c Component) UpstreamRef() string {
	return fmt.Sprintf("%s:%s@%s", c.Upstream.Image, c.Upstream.Version, c.Upstream.Digest)
}

// Options builds the diff options this component declares.
func (c Component) Options() Options {
	dropped := make(map[string]string, len(c.Dropped))
	for _, d := range c.Dropped {
		dropped[d.Metric] = d.Reason
	}
	return Options{Namespace: c.Parity.Namespace, Dropped: dropped}
}

// LoadComponent reads a component.yaml.
func LoadComponent(path string) (Component, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Component{}, err
	}
	var c Component
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return Component{}, fmt.Errorf("reading %s: %w", path, err)
	}
	return c, nil
}
