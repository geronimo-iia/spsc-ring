# Backlog

## Medium

- `push_slice(&[])` — zero-len input should return 0, no side effects
- `pop_into_slice(&mut [])` — zero-len dst should return 0, no side effects
- `ring(1)` wrap — single-slot buffer: push/pop/push/pop, sequence math at mask=0

## Low

- `Display` for all error types: `RecvError`, `SendError`, `TryRecvError`, `TrySendError`, `InvalidCapacity`
- Loom: disconnect race — `closed` flag set concurrently with `is_disconnected` check
- Both halves drop simultaneously — double-drop race, should not double-set `closed` or panic
