---
title: "Kafka record headers in Stateful Functions: closing a five-year-old gap"
description: StateFun Actors 3.4.0-KZM-3.4 adds end-to-end Kafka record header support to the Stateful Functions programming model - trace propagation, typed values, Kafka-exact null semantics, and per-topic opt-in.
---

# Kafka record headers in Stateful Functions: closing a five-year-old gap

*Published 2026-07-24 · by the kzmlabs maintainers · [Read on dev.to](https://dev.to/okazimirov/kafka-record-headers-in-stateful-functions-closing-a-five-year-old-gap-5bon) · [Read on Medium](https://medium.com/@kazimirov.oleksandr/kafka-record-headers-in-stateful-functions-3f24c82b473f)*

Apache Stateful Functions never supported Kafka record headers. Not on the way in: a function had no way to see the headers of the record that invoked it. Not on the way out: the egress builder had topic, key, and value, and nothing else. If your platform standardized on header-borne trace context, tenant tags, or schema hints, StateFun was a black hole in the middle of your pipeline. Headers went in, and they never came out.

[StateFun Actors 3.4.0-KZM-3.4](https://github.com/kzmlabs/flink-statefun/releases/tag/v3.4.0-KZM-3.4) finally closes that gap for remote functions built with the Java SDK. Here's what shipped, a couple of design decisions we sweated over more than expected (null handling took three iterations), and how to adopt it in a running deployment.

## What can StateFun functions do with Kafka headers now?

Both directions, symmetric API.

**Reading**: a function invoked by a Kafka record sees that record's headers.

```java
public CompletableFuture<Void> apply(Context context, Message message) {
    for (MessageHeader header : message.headers()) {
        switch (header.key()) {
            case "traceparent" -> span.setParent(header.valueAsUtf8String());
            case "retry-count" -> handleRetry(header.valueAsInt());
        }
    }
    ...
}
```

**Writing**: the egress builder attaches headers to the produced record.

```java
context.send(
    KafkaEgressMessage.forEgress(NOTIFICATIONS)
        .withTopic("notifications")
        .withUtf8Key(orderId)
        .withValue(payload)
        .withUtf8Header("traceparent", traceContext)  // UTF-8 text
        .withHeader("payload-hash", hashBytes)        // raw bytes
        .withHeader("retry-count", 10)                // binary int, 10, not "10"
        .build());
```

The obvious first use case is distributed tracing: a W3C `traceparent` header can now ride a record from your edge producer, through a StateFun function graph, and out to downstream consumers. That was previously impossible without smuggling trace context inside every payload schema.

## Why is 10, not "10", worth a bullet point?

Kafka headers are raw bytes on the wire; Kafka has no typed headers. Most frameworks make you pick between hand-stringifying numbers or hand-packing byte buffers. The SDK gives you three explicit encodings and never guesses:

- `withUtf8Header(key, text)`: plain text, readable by any Kafka consumer, `kcat`, or Connect.
- `withHeader(key, bytes)`: raw bytes, you own the format.
- `withHeader(key, 10)` / `withHeader(key, type, value)`: the SDK's binary type encodings, decoded losslessly on the read side by `valueAsInt()`, `valueAsLong()`, `valueAsFloat()`, `valueAsDouble()`, `valueAsBoolean()`, or `valueAs(Type<T>)`.

Every read accessor shares one production-safety contract: a null or undecodable value returns `null`, never an exception inside a live function invocation.

## What happens to null header values?

They stay null. This detail is easy to get wrong: Kafka's wire format distinguishes a header whose value is **null** from one whose value is **empty** (a null value encodes as length `-1`), and `Header#value()` in the Kafka client legitimately returns `null`. Protobuf, which carries StateFun's remote-function protocol, cannot represent null bytes.

Our first cut coerced null to empty bytes. It worked, and it was wrong. We only caught it after actually checking what `kafka-clients` does (null header values are legal and preserved; null *keys* are rejected). The fix borrows the idiom StateFun's protocol already uses for exactly this problem: an explicit `has_value` flag, same as `TypedValue.has_value`. A null-valued header now goes in as null and comes out as null, which also means echoing headers is lossless:

```java
message.headers().forEach(h -> egress.withHeader(h.key(), h.value()));
```

## How did this land without touching the runtime's hot path?

The interesting design constraint: an ingress record's payload crosses the whole Flink runtime (router, internal message envelope, feedback loop, request-reply dispatcher) as a single `TypedValue` protobuf, which the dispatcher copies verbatim into the remote function invocation. There is no side channel.

So the headers ride the `TypedValue` itself, as a new repeated `metadata` field. Additive protobuf, preserved by every serialization boundary in between, delivered to the SDK with **zero changes to flink-core**. Old SDKs parse and ignore the field; new SDKs against old runtimes simply see no metadata. No lockstep upgrade, no savepoint migration, no protocol version bump.

A pleasant side effect: function-to-function messages can carry headers too. `MessageBuilder` has the same header API, and forwarding via `MessageBuilder.fromMessage(...)` preserves them.

## Doesn't this bloat every record?

Only if you ask for it. Ingress header forwarding is **opt-in per topic**, because captured headers cost bytes on every hop: the routable envelope, the Flink shuffle, the HTTP request to your function.

```yaml
kind: io.statefun.kafka.v1/ingress
spec:
  id: example/orders-in
  forwardHeaders: true          # ingress-level default for all topics
  topics:
    - topic: orders
      valueType: example/Order
      targets: [example/order-fn]
    - topic: firehose
      forwardHeaders: false     # per-topic override wins
      valueType: example/Event
      targets: [example/event-fn]
```

Topics that have not opted in never capture headers. Nothing enters the runtime, and `Message#headers()` returns an empty list. Records on opted-in topics that happen to carry no headers add zero overhead: the hot path guards every allocation behind a header-count check. Egress headers are always available since they are explicit builder calls.

## Can I unit-test functions that use headers?

Yes, without touching protobuf. The SDK testing toolkit grew alongside the feature:

```java
Message message =
    MessageBuilder.forAddress(SELF)
        .withValue("payload")
        .withUtf8Header("traceparent", "00-abc...")
        .withHeader("attempt", 3)
        .build();
TestContext context = TestContext.forTarget(SELF);

functionUnderTest.apply(context, message).get();

KafkaEgressCapture record =
    KafkaEgressCapture.of(context.getSentEgressMessages().get(0).message());
assertThat(record.lastHeader("traceparent").orElseThrow().valueAsUtf8String())
    .startsWith("00-");
```

`KafkaEgressCapture` unpacks a captured egress message into topic, key, value, and headers, with `lastHeader` following Kafka's own last-wins semantics.

## How is this tested?

Around sixty unit tests pin the contract at each seam: duplicate keys, ordering, all six value encodings, null-vs-empty at every boundary, garbage bytes decoding to `null` instead of throwing. On top of that sits the Kubernetes end-to-end gate every release has to pass anyway. It now produces a record with UTF-8, binary, *and* null-valued headers into a kind cluster running the Flink Operator, has a remote function echo them, and asserts byte-exact results on the output topic.

## How do I get it?

`3.4.0-KZM-3.4` on [Maven Central](https://central.sonatype.com/artifact/io.github.kzmlabs.flinkstatefun/statefun-sdk-java) and [GHCR](https://github.com/kzmlabs/flink-statefun/pkgs/container/flink-statefun):

```xml
<dependency>
    <groupId>io.github.kzmlabs.flinkstatefun</groupId>
    <artifactId>statefun-sdk-java</artifactId>
    <version>3.4.0-KZM-3.4</version>
</dependency>
```

Existing deployments upgrade in place. The protocol change is strictly additive, and header forwarding stays off until a topic opts in, so nothing changes behavior until you say so.

Full reference: **[the Kafka record headers guide](../guides/kafka-headers.md)**. Background on the fork itself: **[We forked Apache Stateful Functions for Flink 2.x](forking-statefun.md)**.
