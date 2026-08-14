+++
title = "Documentation"
description = "Guides for aws-ssm-bridge: getting started, architecture, the Session Manager wire protocol, the security model, and the Python API."
sort_by = "weight"
template = "section.html"
page_template = "page.html"
+++

Five pages, in the order most people need them.

| Page | What it covers |
|:---|:---|
| [Getting started](@/docs/getting-started.md) | Credentials, your first session, port forwarding, and the errors you will actually hit |
| [Architecture](@/docs/architecture.md) | How the crate is put together, and the reasoning behind the parts that are not obvious |
| [Wire protocol](@/docs/protocol.md) | What travels between a client and the SSM agent, and where the traps are |
| [Security](@/docs/security.md) | What is defended against, how, and what explicitly is not |
| [Python](@/docs/python.md) | The full async binding surface |

Upgrading from an earlier release? The
[changelog](https://github.com/hupe1980/aws-ssm-bridge/blob/main/CHANGELOG.md)
carries a migration table — 0.5.0 moved or renamed most of the public API.

For the item-by-item Rust API — every type, method and trait — see
[docs.rs/aws-ssm-bridge](https://docs.rs/aws-ssm-bridge). These pages explain
*why*; the API reference documents *what*.

## Is this the right library?

It is, if you are writing an application that needs to open Session Manager
sessions programmatically: a deployment tool, an SSH `ProxyCommand`, a database
tunnel, an incident-response bot, a fleet-wide command runner.

It is not, if you want a terminal command. Use `aws ssm start-session` and the
official [`session-manager-plugin`](https://github.com/aws/session-manager-plugin)
for that — this crate deliberately has no CLI.

## The one thing to read first

A session is either running or closed, and *every* way it can end resolves
[`Session::closed()`](https://docs.rs/aws-ssm-bridge/latest/aws_ssm_bridge/session/struct.Session.html)
and records a `CloseReason`. Build on that signal rather than polling, and the
rest of the API follows naturally. [Architecture](@/docs/architecture.md#session-lifetime)
explains why it is the load-bearing guarantee.
