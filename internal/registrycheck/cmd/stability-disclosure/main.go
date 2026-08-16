// Command stability-disclosure fails when a component depends on an upstream
// convention below stable that its component.yaml does not list.
//
// It is the one registry policy that cannot be a Rego rule: Weaver hands a
// policy the resolved registry and nothing else, and this check needs
// component.yaml too. `make telemetry-check` runs it after the Rego pass.
//
//	stability-disclosure <registry-dir> <component.yaml>
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"go.olly.garden/grafts/internal/registrycheck"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: stability-disclosure <registry-dir> <component.yaml>")
		os.Exit(2)
	}
	registry, componentPath := os.Args[1], os.Args[2]

	if err := run(registry, componentPath); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", componentPath, err)
		os.Exit(1)
	}
}

func run(registry, componentPath string) error {
	weaver, err := filepath.Abs(filepath.Join("telemetry", "weaver.sh"))
	if err != nil {
		return err
	}

	cmd := exec.Command(weaver, "resolve", registry)
	cmd.Stderr = os.Stderr
	resolved, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("resolving %s: %w", registry, err)
	}

	component, err := os.ReadFile(componentPath)
	if err != nil {
		return err
	}
	return registrycheck.StabilityDisclosure(resolved, component)
}
