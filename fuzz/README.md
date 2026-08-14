# Fuzz targets

Coverage-guided fuzzing for everything that parses data from the network.

```sh
cargo install cargo-fuzz
cargo fuzz run fuzz_binary_protocol
cargo fuzz run fuzz_handshake
cargo fuzz run fuzz_acknowledge
```

| Target | What it protects |
|---|---|
| `fuzz_binary_protocol` | The 120-byte header parser — the first code to touch bytes off the wire. Also checks that a message this crate produced always re-parses. |
| `fuzz_handshake` | The agent's handshake JSON, which is parsed before any session state exists. |
| `fuzz_acknowledge` | Acknowledgement payloads, which drive the retransmission buffer. |

A panic in any of these is a remotely triggerable denial of service, so the bar
is: arbitrary bytes in, `Result` out, never a panic.
