# Shading layer

StateFun's runtime needs Protobuf 3.25.5 internally. A user's job may already pull a different Protobuf version (3.21, 4.x, or whatever Flink/Kafka transitively needs). Without isolation, both copies of `com.google.protobuf.Message` would land on the classpath, and the JVM would pick one — possibly breaking either StateFun or the user's code.

## How the isolation works

The `statefun-shaded/` parent module holds two children:

- **`statefun-protobuf-shaded`** — a copy of `protobuf-java` 3.25.5 with every package rewritten from `com.google.protobuf` to `org.apache.flink.statefun.sdk.shaded.com.google.protobuf`.
- **`statefun-protocol-shaded`** — StateFun's own generated protocol classes (`Address`, `FromFunction`, `ToFunction`) relocated under the same `org.apache.flink.statefun.sdk.shaded` prefix.

Internally StateFun imports the relocated names. The user's code imports the regular `com.google.protobuf.*`. Two distinct types from the JVM's view — no collision regardless of the user's Protobuf version.

## Implementation note

Unusually, the relocation happens at **source-generation time** (via `replacer-plugin`), not at JAR-creation time (the more common `maven-shade-plugin <relocations>` pattern). The reason: `statefun-sdk-java` source code itself imports the relocated names. JAR-time relocation would mean those class names only exist after packaging — too late for compile.

The trade-off: one extra build step per shaded module, generated `.java` files with a `@javax.annotation.Generated("proto")` marker, and a slightly noisier shade-plugin overlap warning when uber JARs include both the relocated and original Protobuf JARs (which is intentional).

## What stays unrelocated

- The public SDK API (`statefun-sdk-java`) — so users can `import org.apache.flink.statefun.sdk.java.*` directly
- User function code, user proto types — your imports are unchanged

## Net result

A user can put StateFun on their classpath alongside any Protobuf version they want. StateFun internally uses its private relocated copy; the user's `com.google.protobuf.Message` is whatever they brought.
