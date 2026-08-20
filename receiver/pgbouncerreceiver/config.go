package pgbouncerreceiver

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"

	"go.olly.garden/grafts/internal/promcompat"

	"go.olly.garden/grafts/receiver/pgbouncerreceiver/internal/telemetry"
)

// Config configures the PgBouncer receiver.
type Config struct {
	// Endpoint is PgBouncer's listen address, as host:port.
	Endpoint string `mapstructure:"endpoint"`
	// Username is a role listed in PgBouncer's `stats_users` or `admin_users`.
	// A role in neither can connect but sees nothing.
	Username string `mapstructure:"username"`
	// Password for Username.
	Password configopaque.String `mapstructure:"password"`
	// Database is PgBouncer's admin database.
	Database string `mapstructure:"database"`
	// TLS selects libpq's sslmode. PgBouncer's admin console is usually reached
	// over a loopback or a private network, so this defaults to disable rather
	// than failing closed on a setup that has no certificate to verify.
	TLS string `mapstructure:"tls"`

	// CollectionInterval is how often PgBouncer is scraped.
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	// Timeout bounds one scrape, across all commands.
	Timeout time.Duration `mapstructure:"timeout"`

	// Emit selects the output shapes. Defaults to OTel only. Adding
	// "prometheus" also emits the series pgbouncer_exporter produced, in their
	// own instrumentation scope so one filter processor removes them.
	Emit promcompat.Emit `mapstructure:"emit"`
	// Metrics gates individual metrics.
	Metrics telemetry.MetricsConfig `mapstructure:"metrics"`
}

// Validate reports every problem with the configuration at once, so a user
// fixing one is not sent back for the next.
func (c *Config) Validate() error {
	var errs []error

	if c.Endpoint == "" {
		errs = append(errs, errors.New("endpoint is required"))
	}
	if c.Username == "" {
		errs = append(errs, errors.New("username is required"))
	}
	if c.Database == "" {
		errs = append(errs, errors.New("database is required"))
	}
	if c.CollectionInterval <= 0 {
		errs = append(errs, fmt.Errorf("collection_interval must be positive, got %s", c.CollectionInterval))
	}
	if c.Timeout <= 0 {
		errs = append(errs, fmt.Errorf("timeout must be positive, got %s", c.Timeout))
	}
	// A timeout at or above the interval means a slow scrape is still running
	// when the next one is due, which reads as a hung receiver rather than as
	// a misconfiguration.
	if c.Timeout > 0 && c.CollectionInterval > 0 && c.Timeout >= c.CollectionInterval {
		errs = append(errs, fmt.Errorf("timeout (%s) must be shorter than collection_interval (%s)", c.Timeout, c.CollectionInterval))
	}

	errs = append(errs, c.Emit.Validate())

	return errors.Join(errs...)
}

// connString builds the libpq connection string for the admin console.
//
// Assembled through net/url rather than by formatting: a password containing
// `@`, `/` or `?` would otherwise be read as connection-string syntax, and the
// resulting failure looks like a wrong host rather than a quoting bug.
func (c *Config) connString() string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.Username, string(c.Password)),
		Host:     c.Endpoint,
		Path:     "/" + c.Database,
		RawQuery: url.Values{"sslmode": []string{c.TLS}}.Encode(),
	}
	return u.String()
}
