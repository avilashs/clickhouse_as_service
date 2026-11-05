package agent

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func hs(host string, up time.Duration, dbs []string, objs ...schemaObject) hostSchema {
	m := map[string]schemaObject{}
	for _, o := range objs {
		m[o.key()] = o
	}
	return hostSchema{host: host, uptime: up, databases: dbs, objects: m, functions: map[string]string{}}
}

func obj(db, name, engine string) schemaObject {
	return schemaObject{Database: db, Name: name, Engine: engine, Create: "CREATE TABLE " + db + "." + name + " UUID 'u' (x UInt8) ENGINE = " + engine}
}

func TestPlanCopiesOnlyWhatEveryOtherReplicaHas(t *testing.T) {
	a := hs("a", 5*time.Hour, []string{"default", "shop"}, obj("default", "t1", "ReplicatedMergeTree"), obj("shop", "orders", "ReplicatedMergeTree"), obj("shop", "gone", "ReplicatedMergeTree"))
	// "gone" was dropped ON CLUSTER and has already disappeared from b: it must not come back
	b := hs("b", 5*time.Hour, []string{"default", "shop"}, obj("default", "t1", "ReplicatedMergeTree"), obj("shop", "orders", "ReplicatedMergeTree"))
	fresh := hs("c", time.Minute, []string{"default"})
	dbs, objs, _ := planSchemaSync(fresh, []hostSchema{a, b})
	if !reflect.DeepEqual(dbs, []string{"shop"}) {
		t.Errorf("databases %v", dbs)
	}
	var names []string
	for _, o := range objs {
		names = append(names, o.key())
	}
	if !reflect.DeepEqual(names, []string{"default.t1", "shop.orders"}) {
		t.Errorf("objects %v: a table missing from one healthy replica must not be recreated", names)
	}
}

func TestPlanCreatesTablesBeforeWhatReadsThem(t *testing.T) {
	src := hs("a", time.Hour*3, []string{"default"},
		obj("default", "mv", "MaterializedView"), obj("default", "v", "View"), obj("default", "dict", "Dictionary"),
		obj("default", "dist", "Distributed"), obj("default", "t", "ReplicatedReplacingMergeTree"))
	_, objs, _ := planSchemaSync(hs("c", time.Minute, []string{"default"}), []hostSchema{src})
	var order []string
	for _, o := range objs {
		order = append(order, o.Name)
	}
	if !reflect.DeepEqual(order, []string{"t", "dist", "dict", "v", "mv"}) {
		t.Errorf("order %v", order)
	}
}

func TestPlanDoesNothingWhenTheReplicaHasEverything(t *testing.T) {
	a := hs("a", time.Hour, []string{"default"}, obj("default", "t", "ReplicatedMergeTree"))
	c := hs("c", time.Minute, []string{"default"}, obj("default", "t", "ReplicatedMergeTree"))
	dbs, objs, funcs := planSchemaSync(c, []hostSchema{a})
	if len(dbs)+len(objs)+len(funcs) != 0 {
		t.Errorf("%v %v %v", dbs, objs, funcs)
	}
}

func TestPlanCopiesFunctionsEveryOtherReplicaHas(t *testing.T) {
	a := hs("a", time.Hour, nil)
	a.functions["f"] = "CREATE FUNCTION f AS x -> x + 1"
	b := hs("b", time.Hour, nil)
	b.functions["f"] = a.functions["f"]
	b.functions["g"] = "CREATE FUNCTION g AS x -> x"
	_, _, funcs := planSchemaSync(hs("c", time.Minute, nil), []hostSchema{a, b})
	if !reflect.DeepEqual(funcs, []string{"f"}) {
		t.Errorf("functions %v", funcs)
	}
}

func TestIdempotent(t *testing.T) {
	for in, want := range map[string]string{
		"CREATE TABLE d.t UUID 'u' (x UInt8)":               "CREATE TABLE IF NOT EXISTS d.t UUID 'u' (x UInt8)",
		"CREATE MATERIALIZED VIEW d.mv TO d.t AS SELECT 1":  "CREATE MATERIALIZED VIEW IF NOT EXISTS d.mv TO d.t AS SELECT 1",
		"CREATE FUNCTION f AS x -> x":                       "CREATE FUNCTION IF NOT EXISTS f AS x -> x",
		"CREATE DICTIONARY d.dict (k UInt64) PRIMARY KEY k": "CREATE DICTIONARY IF NOT EXISTS d.dict (k UInt64) PRIMARY KEY k",
	} {
		if got := idempotent(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
	if got := idempotent("ATTACH TABLE x"); !strings.HasPrefix(got, "ATTACH") {
		t.Errorf("unrelated statements stay as they are: %q", got)
	}
}
