# CLI Output Formats

The CLI defaults to compact JSON for automation and backward compatibility:

~~~sh
hatrie-cli health
hatrie-cli -output json health
~~~

Use -output pretty before the subcommand when inspecting a response
interactively:

~~~sh
hatrie-cli -output pretty health
~~~

Pretty mode formats each complete newline-delimited JSON response with two-space
indentation. It preserves non-JSON lines, so SQL REPL prompts and diagnostics
remain usable. Watch-style commands continue to emit each record as soon as its
newline arrives.

Pretty mode is intentionally opt-in because formatting has measurable cost:

| Mode | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Compact JSON passthrough | 1.595 | 0 | 0 |
| Pretty JSON | 454.9 | 177 | 3 |
| Pretty-mode CPU cost | **285x higher** | **177 more** | **3 more** |

The benchmark measures the local output formatter only; network latency and
server execution are excluded. Run it with make benchmark-cli-output.
