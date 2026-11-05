// basalt-secret sets a secret in the control plane's file secret store (BASALT_SECRETS_DIR).
// Usage: printf %s "$value" | basalt-secret set <name>
// A secret that exists is not overwritten. It exists for the few secrets an operator provides (the local backup store's key); the control
// plane generates all the others itself.
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/avilashs/clickhouse_as_service/internal/config"
	"github.com/avilashs/clickhouse_as_service/internal/secrets"
)

func main() {
	if len(os.Args) != 3 || os.Args[1] != "set" {
		fmt.Fprintln(os.Stderr, "usage: printf $value | basalt-secret set <name>")
		os.Exit(2)
	}
	cfg, err := config.Load()
	if err == nil {
		var st *secrets.FileStore
		if st, err = secrets.NewFileStore(cfg.SecretsDir); err == nil {
			var v []byte
			if v, err = io.ReadAll(io.LimitReader(os.Stdin, 64<<10)); err == nil {
				ctx := context.Background()
				name := os.Args[2]
				var got string
				if got, err = st.GetOrCreate(ctx, name, func() (string, error) { return string(v), nil }); err == nil && got != string(v) {
					err = fmt.Errorf("%s is already set to a different value; remove its file under BASALT_SECRETS_DIR to rotate it", name)
				}
			}
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "basalt-secret:", err)
		os.Exit(1)
	}
}
