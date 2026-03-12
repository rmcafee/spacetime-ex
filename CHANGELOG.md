# Changelog

## v0.2.0 — 2026-03-07

### Added

- `SpacetimeDB.CodeGen` — fetches schema from `GET /v1/database/{name}/schema` and
  generates `SpacetimeDB.BSATN.Schema` modules, one per table
- `mix spacetimedb.gen` Mix task for CLI code generation
- Handles SpacetimeDB v1 (`typespace` + `product_type_ref`), legacy inline `schema`,
  object-literal type notation (`{"U32": {}}`), `Builtin` wrappers, `Array`, `Option`,
  and `Ref` resolution — compatible across SpacetimeDB versions
- Generated modules include `primary_key/0` and `table_name/0` helpers for use
  with `SpacetimeDB.Table`
- 116 tests total

## v0.1.0 — 2026-03-06

Initial release.

### Added

- `SpacetimeDB.Connection` GenServer managing WebSocket lifecycle via Mint.WebSocket
- Full `v1.json.spacetimedb` protocol support (JSON text frames)
- Client→server: `Subscribe`, `SubscribeSingle`, `SubscribeMulti`, `Unsubscribe`,
  `CallReducer`, `OneOffQuery`
- Server→client: `IdentityToken`, `InitialSubscription`, `SubscribeApplied`,
  `SubscribeMultiApplied`, `UnsubscribeApplied`, `SubscriptionError`,
  `TransactionUpdate`, `TransactionUpdateLight`, `OneOffQueryResponse`
- `SpacetimeDB.Handler` behaviour with optional callbacks for all server messages
- `SpacetimeDB.Types` — typed structs for all protocol messages
- `SpacetimeDB.Protocol` — pure encode/decode layer (no process state)
- Automatic reconnection with exponential backoff
- TLS support via `tls: true` option
- Auth token threading across reconnects
- `child_spec/1` for supervised usage
- Handler map syntax for quick scripting (anonymous function map)
