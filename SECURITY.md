# Security Policy

## Supported Versions

The latest `3.4.x-KZM-2.x` release line receives security fixes. Older versions are not maintained.

| Version        | Supported |
| -------------- | --------- |
| 3.4.0-KZM-2.x  | Yes       |
| earlier        | No        |

## Reporting a Vulnerability

**Please do not open public GitHub issues for security vulnerabilities.**

Report suspected vulnerabilities privately via GitHub's [security advisory form](https://github.com/kzmlabs/flink-statefun/security/advisories/new).

Include:

- A description of the vulnerability and its impact
- Affected versions
- Steps to reproduce (proof of concept if possible)
- Any mitigations you have identified

You can expect an acknowledgement within 7 days. A fix target date will be shared after the initial triage.

## Disclosure Policy

We follow coordinated disclosure:

1. Reporter submits a private advisory
2. We triage, confirm, and develop a fix
3. A CVE is requested if warranted
4. Fix is released on Maven Central and GHCR
5. Public advisory is published alongside the release notes

## Dependency Vulnerabilities

We track transitive dependency CVEs via Dependabot. Reports of vulnerable dependencies (that are actually reachable in StateFun's code paths) are welcome through the same private channel.
