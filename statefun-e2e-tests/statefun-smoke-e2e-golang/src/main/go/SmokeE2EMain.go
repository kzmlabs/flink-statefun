// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package main

import (
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
	"log"
	"net/http"
)

var (
	appNamespace              = "statefun.smoke.e2e"
	commandFn, _              = statefun.TypeNameFromParts(appNamespace, "command-interpreter-fn")
	discardEgress, _          = statefun.TypeNameFromParts(appNamespace, "discard-sink")
	verificationEgress, _     = statefun.TypeNameFromParts(appNamespace, "verification-sink")
	commandsTypeName, _       = statefun.TypeNameFromParts(appNamespace, "commands")
	sourceCommandsTypeName, _ = statefun.TypeNameFromParts(appNamespace, "source-command")
	verificationTypeName, _   = statefun.TypeNameFromParts(appNamespace, "verification-result")
	commandsType              = statefun.MakeProtobufTypeWithTypeName(commandsTypeName)
	sourceCommandsType        = statefun.MakeProtobufTypeWithTypeName(sourceCommandsTypeName)
	verificationType          = statefun.MakeProtobufTypeWithTypeName(verificationTypeName)
)

func main() {
	spec := statefun.StatefulFunctionSpec{
		FunctionType: commandFn,
		States:       []statefun.ValueSpec{State},
		Function:     statefun.StatefulFunctionPointer(CommandInterpreterFn),
	}

	builder := statefun.StatefulFunctionsBuilder()
	_ = builder.WithSpec(spec)

	http.Handle("/", builder.AsHandler())
	log.Fatal(http.ListenAndServeTLS(":443", "/app/v3/test/smoketest/server.crt", "/app/v3/test/smoketest/server.key", nil))
}
