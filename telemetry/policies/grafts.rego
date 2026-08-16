# Policies every grafts component registry is checked against, on top of
# Weaver's own. Weaver natively fails on an unresolvable `ref`, a missing
# `stability` and a malformed definition; these add what it does not do.
#
# The resolved input is the v2 shape:
#
#   input.schema_url            this registry's schema URL
#   input.registry.metrics      declared metrics, plus imported upstream ones
#   input.registry.attributes   declared attributes
#   input.refinements.metrics   the effective metric set, including refinements
#
# Entries we authored carry `provenance.path` under /registry/; entries that
# came from upstream semconv carry `provenance.source`. That is what separates
# "ours" from "theirs" in both collections.
package after_resolution

import rego.v1

# ---- what belongs to this registry ---------------------------------------

# Every metric this registry is responsible for, declared or refined. A local
# metric appears in both collections; identical copies collapse because `deny`
# is a set and both produce the same finding.
owned_metrics contains metric if {
	some collection in [input.registry.metrics, input.refinements.metrics]
	metric := collection[_]
	is_ours(metric)
}

is_ours(entry) if {
	not entry.provenance.source
	startswith(entry.provenance.path, "/registry/")
}

# Upstream conventions this registry pulled in with `imports`, keyed by name.
# Refining one of these is the whole point of a grafts registry, so entries
# under an upstream name are `local_prefix`'s business only when the name was
# never imported; when it was, `instrument_match` takes over.
imported_metrics[metric.name] := metric if {
	metric := input.registry.metrics[_]
	metric.provenance.source
}

# The namespaces this registry may declare in, derived from its schema URL:
# `.../schemas/grafts/<component>/<version>` yields `<component>` for
# self-telemetry and, for a `<system>receiver` or `<system>exporter`, the bare
# `<system>` for the telemetry it scrapes. Deriving them rather than taking a
# parameter keeps the per-component authoring surface to registry files alone.
component := parts[count(parts) - 2] if {
	parts := split(input.schema_url, "/")
}

allowed_roots contains component

allowed_roots contains trim_suffix(component, "receiver")

allowed_roots contains trim_suffix(component, "exporter")

# ---- no_deprecated_refs --------------------------------------------------
#
# Verified gap: `ref: db.user` resolves silently under `weaver registry check`
# despite being deprecated as obsoleted, "Removed, no replacement at this
# time". A deprecated key counts as no key at all.

deny contains finding("no_deprecated_refs", msg, name) if {
	metric := owned_metrics[_]
	name := metric.name
	attr := metric.attributes[_]
	attr.deprecated
	msg := sprintf(
		"metric `%s` references `%s`, which upstream deprecated (%s: %s). A deprecated key counts as no key -- declare a local `<system>.*` attribute with a note stating why.",
		[name, attr.key, object.get(attr.deprecated, "reason", "deprecated"), object.get(attr.deprecated, "note", "")],
	)
}

deny contains finding("no_deprecated_refs", msg, "registry.attributes") if {
	attr := input.registry.attributes[_]
	is_ours(attr)
	attr.deprecated
	msg := sprintf("attribute `%s` is deprecated and still declared here.", [attr.key])
}

# ---- local_prefix --------------------------------------------------------
#
# A registry that redeclares an upstream key is worse than no registry, because
# it blesses the duplication. Declarations live under the component's own
# namespace; everything else is a `ref`.

deny contains finding("local_prefix", msg, name) if {
	metric := owned_metrics[_]
	name := metric.name
	not in_allowed_namespace(name)
	not imported_metrics[name]
	msg := sprintf(
		"metric `%s` is declared outside this registry's namespaces %v. Reference the upstream convention instead of redeclaring it, or move the declaration under the component prefix.",
		[name, sort(allowed_roots)],
	)
}

deny contains finding("local_prefix", msg, attr.key) if {
	attr := input.registry.attributes[_]
	is_ours(attr)
	not in_allowed_namespace(attr.key)
	msg := sprintf(
		"attribute `%s` is declared outside this registry's namespaces %v.",
		[attr.key, sort(allowed_roots)],
	)
}

in_allowed_namespace(name) if {
	root := allowed_roots[_]
	root != ""
	startswith(name, concat("", [root, "."]))
}

# ---- instrument_match ----------------------------------------------------
#
# A referenced convention must match on instrument type, not just on name.
# `db.client.operation.duration` is stable and semantically exact for PgBouncer
# query time, but it is a histogram while PgBouncer exposes only a cumulative
# counter -- and a single-bucket histogram under the released name passes review
# because the name looks right.
#
# The two namespace rules divide the work: `local_prefix` rejects a name in a
# namespace this registry neither owns nor imported, and this one rejects an
# imported name whose instrument the registry disagrees with.
#
# What neither can decide is whether a local metric *should* have used a
# convention it does not name -- `pgbouncer.query.time` versus
# `db.client.operation.duration` share no text. That judgement is what the
# playbook's registry review is for, and why the review is by someone who did
# not write the registry.

deny contains finding("instrument_match", msg, local.name) if {
	local := owned_metrics[_]
	upstream := imported_metrics[local.name]
	upstream.instrument != local.instrument
	msg := sprintf(
		"metric `%s` is declared as a %s but the upstream convention it shadows is a %s. Match the convention's instrument type or emit a local `<system>.*` metric and record why.",
		[local.name, local.instrument, upstream.instrument],
	)
}

# ---- prom_annotation -----------------------------------------------------
#
# The `annotations.prometheus` block is the parity spec: it is what makes "how
# complete are we" a number we compute rather than a claim we make. An entry
# without one is invisible to the parity report.

dispositions := {"map", "rename", "merge", "split", "resource", "drop", "extra"}

deny contains finding("prom_annotation", msg, name) if {
	metric := owned_metrics[_]
	name := metric.name
	not metric.annotations.prometheus
	msg := sprintf("metric `%s` has no `annotations.prometheus` block, so it cannot appear in the parity report.", [name])
}

deny contains finding("prom_annotation", msg, name) if {
	metric := owned_metrics[_]
	name := metric.name
	prom := metric.annotations.prometheus
	not prom.compat_source
	msg := sprintf("metric `%s` does not declare `compat_source`; it must be `derived` or `native`.", [name])
}

deny contains finding("prom_annotation", msg, name) if {
	metric := owned_metrics[_]
	name := metric.name
	prom := metric.annotations.prometheus
	not prom.compat_source in {"derived", "native"}
	msg := sprintf("metric `%s` declares `compat_source: %v`; it must be `derived` or `native`.", [name, prom.compat_source])
}

deny contains finding("prom_annotation", msg, name) if {
	metric := owned_metrics[_]
	name := metric.name
	prom := metric.annotations.prometheus
	not prom.disposition in dispositions
	msg := sprintf("metric `%s` declares `disposition: %v`; it must be one of %v.", [name, object.get(prom, "disposition", "<unset>"), sort(dispositions)])
}

# A `derived` entry promises the compat series is a pure function of the OTel
# output. Dropping a label destroys the information that promise depends on.
deny contains finding("prom_annotation", msg, name) if {
	metric := owned_metrics[_]
	name := metric.name
	prom := metric.annotations.prometheus
	prom.compat_source == "derived"
	count(prom.dropped_labels) > 0
	msg := sprintf(
		"metric `%s` claims `compat_source: derived` while dropping %v. The compat series cannot be reconstructed from output that no longer carries those labels -- mark it `native` so the scraper populates the compat scope directly.",
		[name, prom.dropped_labels],
	)
}

# Anything still emitted in the compat scope needs a source to be diffed
# against. `extra` is the exception: it is surface upstream does not have.
deny contains finding("prom_annotation", msg, name) if {
	metric := owned_metrics[_]
	name := metric.name
	prom := metric.annotations.prometheus
	prom.disposition != "extra"
	not prom.source_metric
	msg := sprintf("metric `%s` has no `source_metric`, so the conformance diff has nothing to match it against.", [name])
}

# ---- helpers -------------------------------------------------------------

finding(id, message, signal) := {
	"id": id,
	"level": "violation",
	"message": message,
	"signal_type": "metric",
	"signal_name": signal,
}
