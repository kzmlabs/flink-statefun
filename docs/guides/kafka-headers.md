# Kafka record headers

Kafka records carry headers — key/value metadata pairs alongside the payload, commonly used for
trace IDs, correlation IDs, schema hints, retry counters, or routing tags. StateFun Actors
supports headers in **both directions** for remote functions built with `statefun-sdk-java`:

- **Ingress → function**: a function invoked by a record from a Kafka ingress
  (`io.statefun.kafka.v1/ingress`) reads the record's headers through `Message#headers()`.
- **Function → egress**: a function attaches headers when building a `KafkaEgressMessage`; the
  runtime writes them onto the produced Kafka record (`io.statefun.kafka.v1/egress`).

Semantics match Kafka's own contract exactly:

| Kafka rule | StateFun behavior |
|---|---|
| Duplicate header keys are allowed | Preserved, in original order |
| Header **value** may be `null` (distinct from empty) | Preserved as `null` end-to-end |
| Header **key** must not be `null` | A null key degrades to an empty key instead of failing |
| Headers are raw bytes on the wire | Choose an encoding — see [Choosing an encoding](#choosing-an-encoding) |

Egress headers work out of the box. Ingress header **forwarding is opt-in**: headers cost payload
bytes on every hop between the ingress and the remote function, so a topic only pays for them
after opting in — see [Enabling header forwarding](#enabling-header-forwarding).

## Enabling header forwarding

Ingress headers are forwarded to remote functions only for topics that enable
`forwardHeaders` (default `false`). The flag exists at two levels of the
`io.statefun.kafka.v1/ingress` spec:

```yaml
kind: io.statefun.kafka.v1/ingress
spec:
  id: example/orders-in
  address: kafka:9092
  consumerGroupId: my-group
  forwardHeaders: true        # ingress-level default for all topics below
  topics:
    - topic: orders
      valueType: example/Order
      targets:
        - example/order-fn
    - topic: audit-log
      forwardHeaders: false   # per-topic override wins over the ingress-level value
      valueType: example/AuditEntry
      targets:
        - example/audit-fn
```

The effective value per topic is resolved as: per-topic `forwardHeaders` if present, otherwise
the ingress-level `forwardHeaders`, otherwise `false`. With a 20-topic ingress you set the policy
once at ingress level and override individual topics in either direction.

For a topic that has not opted in, record headers are never captured — no header bytes enter the
runtime, the Flink shuffle, or the remote-function request, and `Message#headers()` returns an
empty list. Records on opted-in topics without headers still add zero overhead.

## Reading headers in a function

```java
public CompletableFuture<Void> apply(Context context, Message message) {
    for (MessageHeader header : message.headers()) {
        switch (header.key()) {
            case "trace-id" -> log.info("trace={}", header.valueAsUtf8String());
            case "retry-count" -> handleRetry(header.valueAsInt());
        }
    }
    // or stream-style:
    Optional<String> traceId =
        message.headers().stream()
            .filter(h -> h.key().equals("trace-id"))
            .map(MessageHeader::valueAsUtf8String)
            .findFirst();
    ...
}
```

`Message#headers()` returns an immutable `List<MessageHeader>`:

- headers appear in the order they were set on the Kafka record, duplicates included;
- messages from a topic that did not enable
  [`forwardHeaders`](#enabling-header-forwarding), and messages that did not originate from a
  Kafka ingress, return an empty list (unless the sender attached headers explicitly — see
  [Forwarding headers](#forwarding-headers-between-functions));
- the list is computed lazily and cached — untouched headers cost nothing.

### `MessageHeader` accessors

| Accessor | Returns | For values written with |
|---|---|---|
| `key()` | `String` (never null) | — |
| `hasValue()` | `false` when the Kafka header value was null | — |
| `value()` | `Slice` (zero-copy), or `null` for a null-valued header | `withHeader(key, byte[]/Slice)` |
| `valueAsUtf8String()` | `String` | `withUtf8Header(key, text)` |
| `valueAsInt()` / `valueAsLong()` / `valueAsFloat()` / `valueAsDouble()` / `valueAsBoolean()` | boxed primitive | the matching primitive `withHeader` overload |
| `valueAs(Type<T>)` | `T` | `withHeader(key, type, value)` |

!!! note "Never throws"

    Every accessor is production-safe: a null or undecodable value returns `null` — an exception
    is never thrown inside a live function invocation because of malformed header bytes.

## Writing headers to a Kafka egress

```java
context.send(
    KafkaEgressMessage.forEgress(TypeName.typeNameFromString("example/notifications"))
        .withTopic("example.notifications")
        .withUtf8Key(orderId)
        .withValue(payload)
        .withUtf8Header("trace-id", traceId)     // UTF-8 text
        .withHeader("payload-hash", hashBytes)   // raw bytes
        .withHeader("retry-count", 10)           // binary int
        .withHeader("replayed", true)            // binary boolean
        .build());
```

### Builder overloads

| Overload | Wire encoding |
|---|---|
| `withUtf8Header(String key, String value)` | UTF-8 text |
| `withHeader(String key, byte[] value)` | raw bytes as given |
| `withHeader(String key, Slice value)` | raw bytes as given (zero-copy) |
| `withHeader(String key, Type<T> type, T value)` | the SDK type's serializer |
| `withHeader(String key, int / long / float / double / boolean value)` | binary SDK `Types` encoding — shorthand for the `Type<T>` overload |

Headers may repeat keys; they are written in call order.

## Choosing an encoding

Kafka headers are always raw bytes on the wire — there is no native "number" header type — so
the choice is which encoding the *reader* expects:

- **Binary (`withHeader(key, 10)` / `Type<T>` overload)** — a real binary number, decoded
  losslessly by any StateFun SDK reader via `valueAsInt()` etc. Best between StateFun functions.
- **UTF-8 text (`withUtf8Header(key, "10")`)** — readable by any Kafka consumer, `kcat`, Kafka
  Connect, or UI tooling. Best when non-StateFun systems consume the topic.
- **Raw bytes (`byte[]`/`Slice`)** — you own the encoding on both sides (e.g. hashes, protobuf).

## Null semantics

Kafka distinguishes a header with a **null value** from one with an **empty value**, and so does
StateFun — a null value survives the full ingress → function → egress round-trip as null:

```java
// echoing headers preserves null-ness:
message.headers().forEach(h -> egress.withHeader(h.key(), h.value())); // null value() → null header

// reading:
header.hasValue();   // false for a null-valued header
header.value();      // null   — same contract as Kafka's own Header#value()
header.valueAsInt(); // null   — no exception
```

Write-side null handling (never fails a send):

| Input | Result on the produced record |
|---|---|
| null header *value* (any overload) | header present with a **null** value |
| explicitly empty value (`new byte[0]`) | header present with an **empty** value |
| null header *key* | key degrades to `""` (Kafka forbids null keys — failing inside the running job would be worse) |
| null `Type<T>` object | treated as a null value |

## Forwarding headers between functions

Function-to-function messages can carry headers too — `MessageBuilder` has the same overload
family, and the receiving function reads them with the same `Message#headers()` API:

```java
context.send(
    MessageBuilder.forAddress(TARGET_FN, id)
        .withValue("payload")
        .withUtf8Header("trace-id", traceId)
        .build());
```

`MessageBuilder.fromMessage(message)` preserves the original message's headers when re-targeting
or forwarding, including null-valued ones.

## Testing header-aware functions

The SDK testing toolkit covers headers end-to-end — construct incoming messages with headers via
`MessageBuilder`, and assert egress headers with `KafkaEgressCapture` (no protobuf hand-parsing):

```java
@Test
void echoesTraceHeader() throws Exception {
    Message message =
        MessageBuilder.forAddress(SELF)
            .withValue("payload")
            .withUtf8Header("trace-id", "abc-123")
            .withHeader("attempt", 3)
            .build();
    TestContext context = TestContext.forTarget(SELF);

    functionUnderTest.apply(context, message).get();

    KafkaEgressCapture record =
        KafkaEgressCapture.of(context.getSentEgressMessages().get(0).message());
    assertThat(record.topic()).isEqualTo("out");
    assertThat(record.lastHeader("trace-id").orElseThrow().valueAsUtf8String())
        .isEqualTo("abc-123");
    assertThat(record.lastHeader("attempt").orElseThrow().valueAsInt()).isEqualTo(3);
}
```

`KafkaEgressCapture` exposes `targetEgressId()`, `topic()`, `utf8Key()`, `value()`,
`utf8Value()`, `headers()`, and `lastHeader(key)` (Kafka's last-wins semantics). Unlike the
production paths it fails loudly: passing a non-Kafka egress message throws
`IllegalArgumentException`, so a wiring mistake surfaces in the test, not in production.

## Protocol and compatibility

Header support is strictly additive at the protocol level:

- `KafkaProducerRecord` (egress), `TypedValue.Metadata` (request-reply), and the internal ingress
  envelope each gained a repeated header field with a `has_value` flag — the same idiom
  `TypedValue` itself uses to distinguish empty from absent.
- Remote functions built against **older SDKs** keep working against a runtime with header
  support — they simply do not see headers.
- A **new SDK** talking to an **older runtime** also keeps working — headers set on egress
  messages are ignored by the old runtime, nothing fails.
- `forwardHeaders` is an additive YAML property — existing `module.yaml` files parse unchanged
  and keep the default (off).
- Records without headers — and all records on topics that did not opt in — have unchanged wire
  size and no extra allocations on any path.

!!! info "Kafka only"

    Kinesis has no record-header concept, so `io.statefun.kinesis.v1` ingresses and egresses are
    unaffected by this feature.
