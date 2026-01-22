---
layout: default
title: Security
nav_order: 3
---

# Security
{: .no_toc }

Threat model and security implementation details.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Design Principles

1. **Memory safety** - `#![forbid(unsafe_code)]`
2. **AWS transport security** - All SSM traffic encrypted via TLS (WSS)
3. **Input validation** - Defensive parsing throughout
4. **Rate limiting** - Built-in DoS protection

---

## Threat Model

| Threat | Mitigation |
|:-------|:-----------|
| Message tampering | SHA-256 digest validation |
| Credential leakage | Redacted Debug impls, URL sanitization |
| Replay attacks | Sequence number tracking |
| DoS (large messages) | 10MB max payload |
| DoS (message flood) | Rate limiting (configurable) |
| SSRF | Strict AWS endpoint validation |
| MITM | TLS required (WSS only) |

---

## Credential Handling

Credentials are automatically sanitized:
- Session tokens skipped in tracing spans
- URLs sanitized to remove query parameters in logs
- Custom `Debug` impls redact sensitive fields

---

## Rate Limiting

Token bucket rate limiting:

```rust
use aws_ssm_bridge::{RateLimiter, RateLimitConfig};

let limiter = RateLimiter::new(RateLimitConfig {
    tokens_per_second: 100.0,
    bucket_size: 50,
    ..Default::default()
});
```

### Default Limits

| Limit | Value |
|:------|:------|
| Max payload size | 10 MB |
| Max decompressed size | 100 MB |
| Buffer capacity | 10,000 messages |

---

## SSRF Protection

URL validation ensures only AWS SSM endpoints are allowed:

```
wss://*.ssmmessages.{region}.amazonaws.com
wss://*.ssmmessages.{region}.amazonaws.com.cn
```

Prevents SSRF attacks where a malicious server might redirect connections.

---

## Deployment Checklist

- [ ] Use environment variables or secrets manager for credentials
- [ ] Enable CloudTrail logging for SSM API calls
- [ ] Use VPC endpoints where possible
- [ ] Set appropriate `idle_timeout` and `max_duration`
- [ ] Monitor metrics for anomalies
- [ ] Keep dependencies updated (`cargo audit`)
