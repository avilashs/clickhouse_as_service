// Package openapi embeds the API contract so the server can serve it and tests can check it.
package openapi

import _ "embed"

//go:embed openapi.yaml
var Spec []byte
