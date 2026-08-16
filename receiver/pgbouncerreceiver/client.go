package pgbouncerreceiver

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Command is a PgBouncer admin console command.
type Command string

// The commands one scrape issues. `SHOW SERVERS` is deliberately absent: every
// row is one connection, so it needs an aggregation decision this receiver has
// not made, and the exporter it replaces does not read it either.
const (
	CommandStats     Command = "SHOW STATS"
	CommandPools     Command = "SHOW POOLS"
	CommandDatabases Command = "SHOW DATABASES"
	CommandLists     Command = "SHOW LISTS"
	CommandClients   Command = "SHOW CLIENTS"
	CommandConfig    Command = "SHOW CONFIG"
	CommandVersion   Command = "SHOW VERSION"
)

// Row is one admin console row, keyed by column name.
//
// Read as strings rather than into a struct per command, because PgBouncer adds
// columns between releases: 1.24 added `total_server_assignment_count` to
// SHOW STATS and 1.25 added `load_balance_hosts` to SHOW POOLS. A struct scan
// breaks on a column it has never heard of; a map ignores it.
type Row map[string]string

// Int returns a column as an integer, or zero when it is absent or unparseable.
// A column PgBouncer has renamed reads as zero rather than failing the whole
// scrape, which matches the partial-scrape behaviour users rely on.
func (r Row) Int(column string) int64 {
	v, err := strconv.ParseInt(strings.TrimSpace(r[column]), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// Float returns a column as a float, or zero when it is absent or unparseable.
func (r Row) Float(column string) float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(r[column]), 64)
	if err != nil {
		return 0
	}
	return v
}

// client reads PgBouncer's admin console.
//
// An interface so the scraper's tests never open a connection: everything the
// receiver decides -- the joins, the aggregation, the attribute mapping -- is
// worth testing without Docker.
type client interface {
	Query(ctx context.Context, cmd Command) ([]Row, error)
	Close(ctx context.Context) error
}

// pgxClient talks to the admin console over the Postgres wire protocol.
type pgxClient struct {
	conn *pgx.Conn
}

// newClient connects to PgBouncer's admin database.
func newClient(ctx context.Context, connString string) (*pgxClient, error) {
	cfg, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parsing connection string: %w", err)
	}

	// PgBouncer's admin console does not implement the extended query protocol,
	// which pgx uses by default. Getting this wrong is not a per-query failure:
	// the first SHOW returns "extended query protocol not supported by admin
	// console", the failed statement stays in pgx's cache, and the deallocation
	// attempt closes the connection -- so every later command fails too, with
	// an error naming a statement rather than the protocol.
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to the pgbouncer admin console: %w", err)
	}
	return &pgxClient{conn: conn}, nil
}

// Query runs one admin command and returns its rows keyed by column name.
func (c *pgxClient) Query(ctx context.Context, cmd Command) ([]Row, error) {
	rows, err := c.conn.Query(ctx, string(cmd))
	if err != nil {
		return nil, fmt.Errorf("running %s: %w", cmd, err)
	}
	defer rows.Close()

	columns := make([]string, 0, len(rows.FieldDescriptions()))
	for _, fd := range rows.FieldDescriptions() {
		columns = append(columns, fd.Name)
	}

	var out []Row
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("reading a %s row: %w", cmd, err)
		}
		row := make(Row, len(columns))
		for i, column := range columns {
			if i < len(values) && values[i] != nil {
				row[column] = fmt.Sprint(values[i])
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading %s: %w", cmd, err)
	}
	return out, nil
}

// Close releases the admin connection.
func (c *pgxClient) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
}
