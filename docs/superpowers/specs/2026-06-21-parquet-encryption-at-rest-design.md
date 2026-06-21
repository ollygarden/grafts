# Parquet Exporter — Encryption at Rest

Date: 2026-06-21
Component: `exporter/parquetexporter`

## Goal

Add optional encryption at rest for the Parquet files the exporter writes, using
Parquet Modular Encryption (AES-GCM) so that DuckDB can still read the files
natively (with the key) via `read_parquet(...)`. Encryption is opt-in and fully
backward compatible: when unconfigured, the exporter behaves exactly as today.

## Non-goals (v1)

- Key management beyond a static key supplied in config (no KMS, no envelope
  encryption, no key files). These remain possible future iterations.
- Column-level / selective encryption. v1 encrypts the whole file.
- Configurable algorithm. v1 fixes AES-GCM (the Parquet default).
- Automatic key rotation.

## Background

The write path lives in `writer.go`:

```go
fw, err := pqarrow.NewFileWriter(w.schema, writeOnlyFile{f}, w.props, pqarrow.DefaultWriterProps())
```

`w.props` is built in `newWriterProperties(compression string)`. Arrow Go
v18.6.0 supports Parquet Modular Encryption via
`parquet.WithEncryptionProperties(parquet.NewFileEncryptionProperties(footerKey, opts...))`,
which slots directly into the existing `parquet.NewWriterProperties(...)` call.
No change to the rotate/rename/fsync lifecycle is required — an encrypted file
rotates `.part -> .parquet` exactly like a plaintext one.

## Design

### 1. Configuration

Add an optional `encryption` block to `Config` (`config.go`). A pointer so that
`nil` means "off" and preserves current behavior.

```yaml
exporters:
  parquet:
    directory: /data/parquet
    compression: zstd
    encryption:
      key: ${env:PARQUET_ENCRYPTION_KEY}   # base64-encoded raw key (16/24/32 bytes -> AES-128/192/256)
      key_id: "key1"                        # optional label, written as footer-key metadata
```

```go
type EncryptionConfig struct {
    // Key is the base64-encoded raw AES key (16, 24, or 32 bytes).
    Key string `mapstructure:"key"`
    // KeyID is an optional label written as footer-key metadata so a reader
    // (e.g. DuckDB) can select the matching key by name.
    KeyID string `mapstructure:"key_id"`
}
```

`Config` gains: `Encryption *EncryptionConfig `mapstructure:"encryption"``.

**Validation** (extend `Config.Validate()`):
- If `Encryption == nil`, skip (encryption disabled).
- Otherwise `Key` is required; base64-decode it and require the result to be
  exactly 16, 24, or 32 bytes. Any other length or a decode failure is a config
  error returned at startup.
- `KeyID` is free-form and optional.

Decoding happens once in `Validate()` (fail fast). The decoded raw-key bytes are
re-derived where needed in the writer; the key is held only in memory.

### 2. Writer wiring

`newWriterProperties` takes the `*Config` (or the decoded key + key_id) instead
of just the compression string, and conditionally appends encryption:

```go
func newWriterProperties(cfg *Config) *parquet.WriterProperties {
    opts := []parquet.WriterProperty{parquet.WithCompression(codecFor(cfg.Compression))}
    if cfg.Encryption != nil {
        key, _ := base64.StdEncoding.DecodeString(cfg.Encryption.Key) // already validated
        encOpts := []parquet.EncryptOption{}
        if cfg.Encryption.KeyID != "" {
            encOpts = append(encOpts, parquet.WithFooterKeyMetadata(cfg.Encryption.KeyID))
        }
        fileEnc := parquet.NewFileEncryptionProperties(string(key), encOpts...)
        opts = append(opts, parquet.WithEncryptionProperties(fileEnc))
    }
    return parquet.NewWriterProperties(opts...)
}
```

Defaults give whole-file AES-GCM (footer + all columns encrypted, no plaintext
footer, no per-column overrides) — which matches the chosen scope. The exact
Arrow Go option names (`WithFooterKeyMetadata`, `EncryptOption`) are verified
against v18.6.0 during implementation.

On startup the exporter logs whether encryption is enabled (and the `key_id` if
set) — never the key material.

### 3. Testing

- **Round-trip test**: configure a writer with encryption, write a record batch,
  rotate. Then (a) open the resulting `.parquet` with a plain
  `file.NewParquetReader` / `pqarrow` reader and assert it fails without
  decryption properties, and (b) open it with `FileDecryptionProperties` built
  from the same key and assert the rows read back correctly.
- **Config validation tests**: missing key when block present; invalid base64;
  wrong key length (e.g. 20 bytes); valid 16/24/32-byte keys; `key_id` optional.
- **Backward-compat test**: `Encryption == nil` still produces a readable
  plaintext file (existing writer tests continue to pass unmodified).
- Run `make lint` and `make test` for the component (CI gates on lint;
  `errcheck`/`staticcheck` are the source of truth per CLAUDE.md).

### 4. Documentation (README + DuckDB)

Document the `encryption` config and how to read encrypted files from DuckDB:

```sql
PRAGMA add_parquet_key('key1', '<key>');
SELECT * FROM read_parquet('traces/part-*.parquet',
                           encryption_config = {footer_key: 'key1'});
```

**Compatibility caveat to verify during implementation (the one real risk):**
DuckDB's `add_parquet_key` expects the key in a specific encoding/length, and we
must confirm the handshake between the raw bytes Arrow Go writes and what DuckDB
expects matches end-to-end (ideally an actual DuckDB read of a file produced by
the exporter). The README must state the exact key format DuckDB needs so a 16/24/32-byte
key produced here is usable there without guesswork. If a mismatch surfaces, the
fix is in how the README instructs users to pass the key to DuckDB, not in the
Parquet format itself.

## Error handling

- Bad key configuration is caught at startup in `Validate()`, not per file.
- No new per-write failure modes: encryption is internal to the Parquet writer;
  existing `opCreate`/`opWrite`/`opSync`/`opRename` telemetry and error paths are
  unchanged.
- Key material exists only in memory; it is never logged or written to the file
  in plaintext (only the optional `key_id` label is, by design).

## Implementation workflow

Per request, implementation follows:

1. Create a Linear issue on the **Engineering** team for this work.
2. Create a feature branch off `main`.
3. Implement (TDD where practical), run `make lint` + `make test`.
4. Self-review the diff.
5. Open a PR referencing the Linear issue.
6. Wait for Code Rabbit comments and address the relevant ones
7. Once all CI and review steps are done, a human will review and merge.