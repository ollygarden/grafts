# Parquet Exporter Encryption at Rest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional, opt-in encryption at rest to the Parquet exporter using Parquet Modular Encryption (AES-GCM), so DuckDB can still read files with the key.

**Architecture:** A new optional `encryption` config block carries a base64 AES key plus an optional `key_id`. `Config.Validate()` fails fast on a bad key. The writer threads the key into the Parquet `WriterProperties` via `parquet.WithEncryptionProperties(...)`, encrypting the whole file (footer + all columns) with AES-GCM. When the block is absent, behavior is byte-for-byte the current behavior.

**Tech Stack:** Go, `github.com/apache/arrow-go/v18` (parquet + pqarrow), OpenTelemetry Collector exporter framework.

## Global Constraints

- Component module: `exporter/parquetexporter` (its own `go.mod`).
- Arrow Go version: `v18.6.0` (already a dependency; do not bump for this work).
- `make lint` is the source of truth — it runs `errcheck` + `staticcheck` beyond `go vet`/`go test`. Run `golangci-lint run ./...` from the component dir before each commit. Never leave an unchecked error return.
- Encryption is opt-in: `Encryption == nil` MUST preserve current output exactly. Existing tests must pass unmodified.
- Key material is held only in memory; never logged, never written in plaintext. Only the optional `key_id` label may be persisted (as footer-key metadata).
- Algorithm is fixed to AES-GCM (the Arrow Go default — no algorithm config knob in v1).
- Spec: `docs/superpowers/specs/2026-06-21-parquet-encryption-at-rest-design.md`.

## File Structure

- `config.go` (modify): add `EncryptionConfig` type, `Encryption *EncryptionConfig` field, and validation. Holds the decoded-key helper.
- `config_test.go` (modify): validation test cases for the encryption block.
- `writer.go` (modify): change `newWriterProperties` to take `*Config` and conditionally add encryption properties.
- `writer_test.go` (modify): encrypted round-trip test (read fails without key, succeeds with key) + plaintext backward-compat already covered by `TestWriterRoundTrip`.
- `exporter.go` (modify): one startup `Info` log line stating whether encryption is enabled.
- `README.md` (modify): document the `encryption` config and the DuckDB read recipe.

## Confirmed Arrow Go v18.6.0 API (verified against the module cache)

- `parquet.NewFileEncryptionProperties(footerKey string, opts ...parquet.EncryptOption) *parquet.FileEncryptionProperties`
- `parquet.WithFooterKeyMetadata(keyMeta string) parquet.EncryptOption`
- `parquet.WithEncryptionProperties(props *parquet.FileEncryptionProperties) parquet.WriterProperty`
- `parquet.NewFileDecryptionProperties(opts ...parquet.FileDecryptionOption) *parquet.FileDecryptionProperties`
- `parquet.WithFooterKey(key string) parquet.FileDecryptionOption`
- `parquet.NewReaderProperties(alloc memory.Allocator) *parquet.ReaderProperties` — set field `.FileDecryptProps`
- `file.NewParquetReader(r, file.WithReadProps(props))`

The footer key is passed as a Go `string` whose bytes are the raw AES key (16/24/32 bytes), i.e. `string(decodedKeyBytes)`.

---

### Task 1: Config — encryption block and validation

**Files:**
- Modify: `exporter/parquetexporter/config.go`
- Test: `exporter/parquetexporter/config_test.go`

**Interfaces:**
- Produces:
  - `type EncryptionConfig struct { Key string; KeyID string }` with mapstructure tags `key`, `key_id`.
  - `Config.Encryption *EncryptionConfig` (mapstructure `encryption`).
  - `func (c *EncryptionConfig) decodedKey() ([]byte, error)` — base64-decodes `Key` and validates length ∈ {16,24,32}. Used by both `Validate()` and the writer (Task 2).

- [ ] **Step 1: Write the failing tests**

Add these cases to `config_test.go` (keep the existing `TestValidate`; add a new function):

```go
func TestValidateEncryption(t *testing.T) {
	base := func() *Config {
		c := createDefaultConfig().(*Config)
		c.Directory = "/tmp/x"
		return c
	}
	// 32 raw bytes -> AES-256, valid.
	key32 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	key16 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 16))
	key24 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 24))
	key20 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 20))

	t.Run("nil block is valid", func(t *testing.T) {
		require.NoError(t, base().Validate())
	})
	t.Run("valid 16/24/32 byte keys", func(t *testing.T) {
		for _, k := range []string{key16, key24, key32} {
			c := base()
			c.Encryption = &EncryptionConfig{Key: k}
			require.NoError(t, c.Validate())
		}
	})
	t.Run("key_id optional and allowed", func(t *testing.T) {
		c := base()
		c.Encryption = &EncryptionConfig{Key: key32, KeyID: "key1"}
		require.NoError(t, c.Validate())
	})
	t.Run("missing key", func(t *testing.T) {
		c := base()
		c.Encryption = &EncryptionConfig{}
		require.Error(t, c.Validate())
	})
	t.Run("invalid base64", func(t *testing.T) {
		c := base()
		c.Encryption = &EncryptionConfig{Key: "not!base64!!"}
		require.Error(t, c.Validate())
	})
	t.Run("wrong key length", func(t *testing.T) {
		c := base()
		c.Encryption = &EncryptionConfig{Key: key20}
		require.Error(t, c.Validate())
	})
}
```

Add imports to `config_test.go` if missing: `"bytes"`, `"encoding/base64"`, and `"github.com/stretchr/testify/require"`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd exporter/parquetexporter && go test ./... -run TestValidateEncryption -v`
Expected: FAIL — `EncryptionConfig` / `Encryption` undefined (compile error).

- [ ] **Step 3: Implement the config type and validation**

In `config.go`, add the imports `"encoding/base64"` and `"fmt"` (keep `"errors"`). Add the type and field:

```go
// EncryptionConfig enables Parquet Modular Encryption (AES-GCM) for written files.
type EncryptionConfig struct {
	// Key is the base64-encoded raw AES key (16, 24, or 32 bytes -> AES-128/192/256).
	Key string `mapstructure:"key"`
	// KeyID is an optional label written as footer-key metadata so a reader
	// (e.g. DuckDB) can select the matching key by name. Never the key itself.
	KeyID string `mapstructure:"key_id"`
}

// decodedKey returns the raw AES key bytes, validating base64 and length.
func (e *EncryptionConfig) decodedKey() ([]byte, error) {
	if e.Key == "" {
		return nil, errors.New("encryption.key is required when encryption is configured")
	}
	raw, err := base64.StdEncoding.DecodeString(e.Key)
	if err != nil {
		return nil, fmt.Errorf("encryption.key is not valid base64: %w", err)
	}
	switch len(raw) {
	case 16, 24, 32:
		return raw, nil
	default:
		return nil, fmt.Errorf("encryption.key must decode to 16, 24, or 32 bytes, got %d", len(raw))
	}
}
```

Add the field to `Config` (after `Compression`):

```go
	// Encryption, when set, enables Parquet Modular Encryption (AES-GCM).
	Encryption *EncryptionConfig `mapstructure:"encryption"`
```

In `Config.Validate()`, before the final `return nil`, add:

```go
	if cfg.Encryption != nil {
		if _, err := cfg.Encryption.decodedKey(); err != nil {
			return err
		}
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd exporter/parquetexporter && go test ./... -run 'TestValidate' -v`
Expected: PASS (both `TestValidate` and `TestValidateEncryption`).

- [ ] **Step 5: Lint**

Run: `cd exporter/parquetexporter && golangci-lint run ./...`
Expected: no findings.

- [ ] **Step 6: Commit**

```bash
git add exporter/parquetexporter/config.go exporter/parquetexporter/config_test.go
git commit -m "feat(parquetexporter): encryption config block with key validation"
```

---

### Task 2: Writer — thread the key into Parquet WriterProperties

**Files:**
- Modify: `exporter/parquetexporter/writer.go`
- Test: `exporter/parquetexporter/writer_test.go`

**Interfaces:**
- Consumes: `Config.Encryption` and `(*EncryptionConfig).decodedKey()` from Task 1.
- Produces: `func newWriterProperties(cfg *Config) *parquet.WriterProperties` (signature changed from `(compression string)`). Whole-file AES-GCM when `cfg.Encryption != nil`.

- [ ] **Step 1: Write the failing test**

Add to `writer_test.go`. This writes an encrypted file, asserts a plain reader fails, then asserts a reader configured with the key reads the row back. Add imports `"bytes"`, `"encoding/base64"`, `"github.com/apache/arrow-go/v18/parquet"`.

```go
func TestWriterEncryptedRoundTrip(t *testing.T) {
	dir := t.TempDir()
	rawKey := bytes.Repeat([]byte{7}, 32)
	cfg := createDefaultConfig().(*Config)
	cfg.Directory = dir
	cfg.Encryption = &EncryptionConfig{
		Key:   base64.StdEncoding.EncodeToString(rawKey),
		KeyID: "key1",
	}

	w, err := newSignalWriter("test", dir, testSchema(), cfg, testTelemetry(t), zap.NewNop())
	require.NoError(t, err)
	rec := oneRowRecord(t, testSchema(), "secret")
	require.NoError(t, w.write(rec))
	rec.Release()
	require.NoError(t, w.close())

	matches, _ := filepath.Glob(filepath.Join(dir, "*.parquet"))
	require.Len(t, matches, 1)

	// Without the key, opening must fail.
	f1, err := os.Open(matches[0])
	require.NoError(t, err)
	defer func() { _ = f1.Close() }()
	_, err = file.NewParquetReader(f1)
	require.Error(t, err, "encrypted file must not open without a key")

	// With the key, the row reads back.
	f2, err := os.Open(matches[0])
	require.NoError(t, err)
	defer func() { _ = f2.Close() }()
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.FileDecryptProps = parquet.NewFileDecryptionProperties(
		parquet.WithFooterKey(string(rawKey)),
	)
	rdr, err := file.NewParquetReader(f2, file.WithReadProps(props))
	require.NoError(t, err)
	defer func() { require.NoError(t, rdr.Close()) }()
	assert.Equal(t, int64(1), rdr.NumRows())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd exporter/parquetexporter && go test ./... -run TestWriterEncryptedRoundTrip -v`
Expected: FAIL — `newWriterProperties` still ignores encryption, so the plain `file.NewParquetReader(f1)` succeeds and `require.Error` fails (or a compile error if you changed the signature first).

- [ ] **Step 3: Change `newWriterProperties` to take `*Config` and add encryption**

In `writer.go`, add import `"encoding/base64"`. Replace the existing `newWriterProperties`:

```go
func newWriterProperties(cfg *Config) *parquet.WriterProperties {
	codec := compress.Codecs.Zstd
	switch cfg.Compression {
	case compressionSnappy:
		codec = compress.Codecs.Snappy
	case compressionNone:
		codec = compress.Codecs.Uncompressed
	}
	opts := []parquet.WriterProperty{parquet.WithCompression(codec)}
	if cfg.Encryption != nil {
		// Key validity is enforced by Config.Validate at startup; decode again here.
		key, _ := cfg.Encryption.decodedKey()
		var encOpts []parquet.EncryptOption
		if cfg.Encryption.KeyID != "" {
			encOpts = append(encOpts, parquet.WithFooterKeyMetadata(cfg.Encryption.KeyID))
		}
		fileEnc := parquet.NewFileEncryptionProperties(string(key), encOpts...)
		opts = append(opts, parquet.WithEncryptionProperties(fileEnc))
	}
	return parquet.NewWriterProperties(opts...)
}
```

Update the one caller in `newSignalWriter`:

```go
		props:  newWriterProperties(cfg),
```

- [ ] **Step 4: Run the writer tests to verify they pass**

Run: `cd exporter/parquetexporter && go test ./... -run TestWriter -v`
Expected: PASS — `TestWriterEncryptedRoundTrip`, `TestWriterRoundTrip` (plaintext backward-compat), `TestWriterRotatesOnMaxRows`, `TestWriterRotatesOnAge` all pass.

- [ ] **Step 5: Run the full component test suite + lint**

Run: `cd exporter/parquetexporter && go test ./... && golangci-lint run ./...`
Expected: all PASS, no lint findings.

- [ ] **Step 6: Commit**

```bash
git add exporter/parquetexporter/writer.go exporter/parquetexporter/writer_test.go
git commit -m "feat(parquetexporter): encrypt Parquet files via modular encryption"
```

---

### Task 3: Startup log line

**Files:**
- Modify: `exporter/parquetexporter/exporter.go`

**Interfaces:**
- Consumes: `Config.Encryption` from Task 1, `e.logger` (already present).

- [ ] **Step 1: Add the log line in `Start`**

In `exporter.go`, inside `Start`, immediately after `var err error` (before creating the traces writer), add:

```go
	if e.cfg.Encryption != nil {
		e.logger.Info("parquet: encryption at rest enabled",
			zap.String("algorithm", "AES-GCM"),
			zap.String("key_id", e.cfg.Encryption.KeyID))
	}
```

(`zap` is already imported in `exporter.go`.)

- [ ] **Step 2: Build and run the suite**

Run: `cd exporter/parquetexporter && go build ./... && go test ./... && golangci-lint run ./...`
Expected: all PASS, no lint findings.

- [ ] **Step 3: Commit**

```bash
git add exporter/parquetexporter/exporter.go
git commit -m "feat(parquetexporter): log when encryption at rest is enabled"
```

---

### Task 4: README + DuckDB read recipe

**Files:**
- Modify: `exporter/parquetexporter/README.md`

- [ ] **Step 1: Document the config**

Add an `encryption` entry to the configuration section of `README.md`:

```markdown
### Encryption at rest (optional)

When the `encryption` block is set, files are written with Parquet Modular
Encryption (AES-GCM) — footer and all columns are encrypted. Omit the block to
write plaintext Parquet (the default).

```yaml
exporters:
  parquet:
    directory: /data/parquet
    encryption:
      key: ${env:PARQUET_ENCRYPTION_KEY}  # base64-encoded raw AES key, 16/24/32 bytes (AES-128/192/256)
      key_id: "key1"                       # optional label, stored as footer-key metadata (never the key)
```

The key is read from config at startup and held only in memory; it is never
written to the Parquet files. A bad key (not base64, or not 16/24/32 bytes) fails
collector startup.
```

- [ ] **Step 2: Document the DuckDB read recipe with the encoding caveat**

Add below the config block:

```markdown
#### Reading encrypted files from DuckDB

DuckDB reads Parquet Modular Encryption natively. Register the key, then pass an
`encryption_config`:

```sql
PRAGMA add_parquet_key('key1', '<key>');
SELECT * FROM read_parquet('traces/*.parquet',
                           encryption_config = {footer_key: 'key1'});
```

**Key encoding:** the value passed to `add_parquet_key` must be the *same raw key
bytes* the exporter was given (the bytes your base64 `key` decodes to), supplied
in the form DuckDB's current release expects. Verify the round trip on your DuckDB
version before relying on it in production — see the verification step below.
```

- [ ] **Step 3: Verify the DuckDB handshake (manual, the one real risk)**

This is the compatibility caveat from the spec. If `duckdb` is available locally, run an actual end-to-end read. If it is not available, note in the PR description that this verification is pending and must be done before merge.

```bash
# Produce an encrypted file via a short-lived collector run or the writer test fixture,
# then from DuckDB:
#   PRAGMA add_parquet_key('key1', '<raw-key>');
#   SELECT count(*) FROM read_parquet('traces/part-*.parquet', encryption_config = {footer_key: 'key1'});
# Confirm the count matches and the README instructs the exact key form that worked.
```

Record the working key form in the README (replace `<key>` guidance with the verified form). Expected: DuckDB returns the row count without an "invalid key" / "not encrypted" error.

- [ ] **Step 4: Commit**

```bash
git add exporter/parquetexporter/README.md
git commit -m "docs(parquetexporter): document encryption at rest and DuckDB reads"
```

---

## Self-Review

**Spec coverage:**
- Config block + validation (16/24/32, base64, key_id optional, nil=off) → Task 1. ✓
- Writer wiring, whole-file AES-GCM, key in memory only → Task 2. ✓
- Round-trip test (fails without key, succeeds with key), config validation tests, backward-compat → Tasks 1 & 2 (plaintext compat is existing `TestWriterRoundTrip`, unmodified). ✓
- Startup log (enabled + key_id, never key) → Task 3. ✓
- README + DuckDB recipe + the encoding-handshake caveat verification → Task 4. ✓
- Error handling: bad key at startup → Task 1; no new per-write paths → Task 2 leaves telemetry untouched. ✓

**Placeholder scan:** No TBD/TODO. The only deliberately-manual step is the DuckDB handshake verification (Task 4 Step 3), which the spec itself flags as verify-during-implementation; it has a concrete command and a recorded outcome.

**Type consistency:** `EncryptionConfig{Key, KeyID}`, `decodedKey()`, and `newWriterProperties(cfg *Config)` are used identically across Tasks 1–3. Decrypt-side reader (`NewFileDecryptionProperties` + `WithFooterKey` + `props.FileDecryptProps` + `file.WithReadProps`) matches the verified API list.

## Workflow reminder (from the spec)

This plan covers the code. The surrounding workflow the user specified:
1. Linear issue on the **Engineering** team. 2. Feature branch off `main`. 3. Implement (above) + `make lint`/`make test`. 4. Self-review the diff. 5. Open PR referencing the Linear issue. 6. Address relevant CodeRabbit comments. 7. Human reviews and merges.
