# Sandbox Create Benchmark

## Goal

The target is **20,000 ready sandbox creates per second** at a reference capacity of **768 GiB RAM**, using **128 MiB sandboxes**.

A successful `Create` RPC currently means that placement and VM startup completed. The benchmark must therefore keep three rates distinct:

- **Offered rate:** requests dispatched by the load generator.
- **Accepted rate:** requests admitted by the scheduler and worker.
- **Ready rate:** successful `Create` RPC completions for started sandboxes.

Dispatching 20,000 requests per second is not sufficient evidence for 20,000 ready creates per second. The final result must report all three without relabeling offered traffic as completed work.

The benchmark is intentionally memory-bounded. It runs a short burst that fills only a configured fraction of available RAM, cleans up every sandbox, and leaves the worker fleet at its original size. This lets us prove the design on one inexpensive worker before scaling to a 768 GiB host.

## Capacity Envelope

Let:

- $M_r$ be reference memory in MiB.
- $R_r$ be the target rate at the reference memory.
- $M_l$ be live memory reported by the current scheduler.
- $M_u$ be memory already used by live sandboxes.
- $m$ be requested memory per sandbox.
- $h$ be the benchmark memory headroom in $(0, 1]$.

The scaled offered rate is proportional to live memory:

$$
R_l = R_r \times \frac{M_l}{M_r}
$$

The safe number of requests in one wave is:

$$
N = \left\lfloor \frac{\max(0, \lfloor hM_l \rfloor - M_u)}{m} \right\rfloor
$$

The timed dispatch window is:

$$
T = \frac{N}{R_l}
$$

Scaling both memory and offered rate proportionally keeps the experiment comparable across instance sizes. Increasing the machine size increases the request count, not the duration of the memory-bounded wave.

### Full-scale example

Assume 768 GiB means $768 \times 1024 = 786{,}432$ MiB:

$$
N_{100\%} = \frac{786{,}432}{128} = 6{,}144
$$

At 20,000 creates/s, the full-capacity wave lasts:

$$
T_{100\%} = \frac{6{,}144}{20{,}000} = 307.2\text{ ms}
$$

With 80% headroom:

- Safe requests: $\lfloor 786{,}432 \times 0.8 / 128 \rfloor = 4{,}915$
- Offered rate: 20,000/s
- Dispatch window: 245.75 ms

### Current small-scale example

The current worker advertises approximately 31,560 MiB. Relative to the 768 GiB reference machine:

$$
R_l = 20{,}000 \times \frac{31{,}560}{786{,}432} = 802.61\text{ requests/s}
$$

At 128 MiB per sandbox:

| Headroom | Requests | Offered rate | Dispatch window |
|---:|---:|---:|---:|
| 100% | 246 | 802.61/s | 306.50 ms |
| 90% | 221 | 802.61/s | 275.35 ms |
| 80% | 197 | 802.61/s | 245.45 ms |

The first affordable target is therefore **197 requests over about 245 ms on one current worker**, not 6,144 requests and not a second worker.

## Latency Constraint

Memory capacity also limits how much create latency the system can hide. By Little's law, a target rate $R$ with mean create latency $W$ needs approximately $R \times W$ requests in flight.

At full capacity with 6,144 sandbox slots:

$$
W \leq \frac{6{,}144}{20{,}000} = 307.2\text{ ms}
$$

At 80% headroom, the equivalent bound is about 245.75 ms. Because rate and memory scale together, the small worker has the same normalized latency requirement. A client can offer the scaled rate even when startup is slower, but the system has not reached the goal until ready completions also meet the scaled rate without exhausting the safe memory budget.

## Benchmark Modes

### Ordinary closed-loop mode

The existing mode remains useful for latency and baseline throughput:

- A fixed number of client tasks send requests.
- Each task sends its next request after the previous response.
- The result measures ready throughput under a fixed concurrency.

This mode cannot prove a requested offered rate because response latency throttles request generation.

### Memory-scaled open-loop mode

The scale test uses an open-loop schedule:

1. Query scheduler `SystemService.Info` for aggregate `capacity.mem_mib` and `used.mem_mib`.
2. Derive the scaled rate, request count, and dispatch window from the equations above.
3. Reject invalid configurations, zero capacity, or a wave with no safe sandbox slot.
4. Establish and prime every HTTP/2 connection before timing starts.
5. Warm the image/template path outside the measured window.
6. Assign each request an absolute dispatch deadline within the burst window.
7. Dispatch requests according to those deadlines regardless of earlier response latency.
8. Stop after the derived request count; never extend a wave past the memory budget.
9. Wait for all responses and report readiness separately from dispatch timing.
10. Remove every created sandbox outside the measured window.

Proposed invocation for the current fleet:

```bash
vmon bench \
  --server http://SCHEDULER:8100 \
  --target-rps 20000 \
  --reference-memory-mib 786432 \
  --memory 128 \
  --memory-headroom 0.8 \
  --json
```

The scheduler capacity is discovered at runtime. The command expresses the full-scale goal, while the benchmark derives the affordable local wave automatically.

## Required Output

Every run must report:

### Capacity

- Total live worker memory.
- Memory already used.
- Safe benchmark memory budget after headroom.
- Sandbox memory.
- Derived request count.

### Offered traffic

- Reference target rate and memory.
- Scaled target rate.
- Scheduled dispatch window.
- Actual first-to-last dispatch window.
- Actual offered rate.

### Accepted results

- Successfully admitted creates.
- Admission decision wall time, ending when the final request is accepted or rejected.
- Admitted creates per second over that wall time; readiness drain time is excluded.

### Ready results

- Successful and failed creates.
- Readiness window: schedule origin to the last ready completion.
- Ready creates per second over the readiness window; total wall — which also covers failed creates resolving and stream drain — is reported separately.
- Minimum, mean, p50, p90, p99, and maximum create latency.
- Placement count per worker.
- Stable error-code counts.

### Cleanup and health

- Number of sandboxes removed.
- Cleanup failures and timeouts.
- Peak running and in-flight sandboxes.
- Worker heartbeat continuity.
- Worker count before and after the run.

Machine-readable JSON must include the same distinctions. `creates_per_s` means ready completions; offered traffic belongs in a separate `offered` object.

## Cost Guards

The small-scale phase must not add compute capacity.

- Run with exactly one worker.
- Cap the ASG at one worker or disable scale-up for the duration of the experiment.
- Never use `--keep` in automated runs.
- Preserve the sandbox self-timeout as a cleanup backstop.
- Abort when the scheduler has unexpected pre-existing memory usage, or subtract it from the safe budget.
- Do not begin another wave until cleanup finishes and the scheduler reports zero benchmark sandboxes.
- Restore any temporary autoscaler setting after the run.

An 80% memory wave exceeds a 50% autoscaler target long enough to request a second worker if cleanup stalls. The one-worker ASG cap is therefore a required cost guard, not an optional optimization.

## Small-Scale Acceptance Gate

Before increasing instance size, the current worker must pass repeated 80%-headroom waves:

1. Derived target is approximately 802.61 offered requests/s.
2. Each wave contains 197 requests over approximately 245.45 ms.
3. Actual offered rate is within 5% of the derived target.
4. Every request succeeds; no `busy`, timeout, transport, or engine errors.
5. The worker remains live in the scheduler throughout the wave and cleanup.
6. No second worker launches.
7. Cleanup removes every sandbox and returns used memory to zero.
8. Ready throughput and latency are stable across at least ten fully cleaned waves.

Passing the offered-rate condition proves the client and scheduler can accept the normalized load. Passing the ready-rate condition proves the full create path meets the 20,000/s-per-768-GiB goal at small scale.

## Scale-Up Sequence

Capacity increases only after the previous stage passes with the same normalized rate:

1. Current worker, one ASG instance, memory-scaled short bursts.
2. Repeated waves to establish variance and detect leaks.
3. Larger single worker with the same requests/s per MiB.
4. Multiple workers to validate placement and scheduler fan-out.
5. Full 768 GiB test: up to 6,144 requests per wave, subject to headroom.

At every stage, the equations and result schema remain unchanged. Only live capacity, request count, and the number of workers change.

## Expected Bottlenecks

The benchmark is designed to expose the next limit without buying around it. Likely limits include:

- Synchronous VM readiness on the `Create` RPC.
- Image/template restoration and copy-on-write setup.
- Per-sandbox process, thread, file-descriptor, and eventfd creation.
- TAP, namespace, IP, and nftables setup.
- HTTP/2 stream and connection limits in the client and scheduler.
- Worker heartbeat publication coupled to route updates.
- Serial Redis route writes after a burst.
- Heartbeat sandbox digests capped below the 6,144-sandbox full-scale envelope.

Offered, accepted, ready, health, and cleanup measurements identify which subsystem failed first. That evidence determines the next optimization; instance size does not.
