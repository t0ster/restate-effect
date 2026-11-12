# Changelog

## 0.8.0

**Breaking:** semaphore limits are owned by the semaphore object, not sent with each request.

Before, `withPermit` sent `acquire({ holder, awakeable, permits, lease })`, so the limits were in every caller's journal: changing one in code broke the replay of invocations that had already sent it (journal mismatch, then a pause).

- `Service.layer` and `Service.layerRemote` are now functions of the semaphores they serve: `Semaphores.layer(gateway, ...others)`. The serving deployment's limits apply; `layerRemote` ignores them.
- `withPermit(s)` requires `Registered<Service, Name>` per semaphore: using a semaphore the served object was not given is a type error. `Semaphore<N, K>` gains its name as `K`; `Service.registered` is gone.
- Requests carry only who asks: `acquire({ holder, awakeable })`, `release({ holder })`; `expire` is unchanged. The object journals the limits it uses per operation (a `Restate.run`), so a replay across a deploy stays consistent. An unknown key is a defect.
- Changing a limit is safe on a live server: it applies on deploy, from the next grant. A raise frees slots at the object's next operation; a cut does not revoke current holders.

### Upgrading from 0.7

1. Give each semaphore to the object's layer in the serving endpoint: `Semaphores.layer` → `Semaphores.layer(a, b, …)`; in deployments that only use the object, `Semaphores.layerRemote` → `Semaphores.layerRemote(a, …)`.
2. Deploy when no permit is held or awaited: requests sent under 0.7 by invocations still running fail to replay under 0.8 (the body changed). Drain those invocations first, or kill them; their permits then come back when their leases pass.
3. The object's state carries over: 0.7 ledgers (their stored `permits` is ignored) and queued waiters (they keep their stored lease).
