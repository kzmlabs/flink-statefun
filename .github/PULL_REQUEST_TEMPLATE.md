## Summary

<!-- What does this PR do and why? 1–3 bullets. -->

## Changes

<!-- Brief list of user-visible changes. -->

## Test plan

- [ ] `mvn spotless:apply` clean
- [ ] `mvn install -Dskip.k8s.e2e -B` passes
- [ ] New or updated tests cover the change
- [ ] K8s E2E run (if touching runtime/runner): `mvn verify -pl :statefun-e2e-k8s-native -am`

## Related issues

<!-- Fixes #123, Refs #456 -->
