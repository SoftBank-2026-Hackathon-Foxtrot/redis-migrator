package main

import (
	"statemig/internal/redisx"
	"strings"
	"testing"
)

func TestCommandContractIgnoresProviderExtrasButRequiresApplicationCommands(t *testing.T) {
	allowed := set("get", "set")
	ct := CmdTable{"get": false, "set": true, "json.set": true}
	kt := KeyTable{"get": {1, 1, 1}, "set": {1, 1, 1}, "json.set": {1, 1, 1}}
	commands, keys, err := constrainCommands(ct, kt, allowed)
	if err != nil || len(commands) != 2 || !commands["set"] || keys["set"] != kt["set"] {
		t.Fatal("application command metadata was not preserved", commands, keys, err)
	}
	delete(ct, "set")
	if _, _, err := constrainCommands(ct, kt, allowed); err == nil {
		t.Fatal("a backend missing an application command was accepted")
	}
}

func TestCommandOutsideContractNeverReachesBackend(t *testing.T) {
	backend := newEchoRedis(t, "source", 0)
	p, addr := startGatedProxy(t, backend.ep)
	p.allowedCommands = set("ping")
	c, err := redisx.ParseEndpoint("client", "redis://"+addr)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := c.Dial(0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	value, err := conn.DoStr("JSON.SET", "cache:item", "$", "{}")
	if err == nil || !strings.Contains(err.Error(), "NOPERM") || !value.IsErr() {
		t.Fatal("outside command was not refused", value, err)
	}
}
