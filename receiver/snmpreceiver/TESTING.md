# Testing the SNMP Receiver Locally

This runbook walks through smoke-testing the SNMP receiver end-to-end against [snmpsim](https://github.com/lextudio/snmpsim), a pure-Python SNMP agent simulator.

Runtime: ~5 minutes. Requires Docker and a built `grafts` binary.

## 1. Start snmpsim

```bash
docker run --rm -d --name snmpsim \
  -p 1161:161/udp \
  tandrup/snmpsim
```

The container listens on UDP 161 internally; the command above maps it to host port 1161. The image ships with a default recording under community `public` covering standard MIB-II OIDs (`sysUpTime.0`, `ifTable`, `sysName.0`). No fixture files to author.

Verify it's up:

```bash
docker logs snmpsim | tail -10
```

You should see a line like `Listening at UDP/IPv4 endpoint 0.0.0.0:161`.

## 2. Build the collector

```bash
cd distributions/grafts && make build
```

Produces `distributions/grafts/build/grafts`.

## 3. Run the collector

From the repo root:

```bash
./distributions/grafts/build/grafts \
  --config=./receiver/snmpreceiver/testdata/local/config.yaml
```

## 4. Verify output

Within ~15 seconds you should see the `debug` exporter log metric records. Look for:

- `snmp.system.uptime` — gauge, single data point, resource attribute `snmp.host=127.0.0.1`
- `snmp.interface.in_octets` — counter, multiple data points (one per interface in snmpsim's `ifTable`), each with an `interfaces_index` attribute

Example line (abbreviated):

```
Metric #0
Descriptor:
     -> Name: snmp.system.uptime
     -> Unit: cs
     -> DataType: Gauge
NumberDataPoints #0
Timestamp: ...
Value: 12345
```

The v3 target polls the same `system` group, so you should see `snmp.system.uptime` emitted for **both** targets each cycle (identical resource attributes — this is expected for a single snmpsim instance).

## 5. Cleanup

```bash
# Ctrl-C the collector
docker stop snmpsim
```

## SNMPv3 credentials

The config uses snmpsim's built-in `simulator` user profile. These are **simulator defaults, not production credentials**:

| Field | Value |
|---|---|
| username | `simulator` |
| auth_protocol | `MD5` |
| auth_passphrase | `auctoritas` |
| privacy_protocol | `DES` |
| privacy_passphrase | `privatus` |

## Troubleshooting

**Port 1161 already in use.** Another SNMP simulator or an earlier `snmpsim` container may be running. Run `docker ps` and stop the conflicting container, or change the port mapping (e.g. `-p 11161:1161/udp`) and update `targets[].port` in the config accordingly.

**Collector starts but no metrics appear.** Check `docker logs snmpsim` for errors. Verify connectivity from the host:

```bash
docker run --rm --network host alpine/socat - UDP:127.0.0.1:1161 </dev/null
```

**SNMPv3 auth failures in collector logs.** Most often a typo in `auth_passphrase` or `privacy_passphrase`. Values must be exactly `auctoritas` and `privatus`.

**macOS UDP flakiness.** Docker Desktop on macOS has historically had quirks with UDP port forwarding. If metrics are intermittent, try `colima` or `orbstack` as an alternative runtime, or run snmpsim directly on the host via `pip install snmpsim`.

## Automated integration test

For a headless version of this flow, see `integration_test.go` in this package. Run it with:

```bash
make test-integration
```
