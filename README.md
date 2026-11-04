# hetzner-deadman

A single-purpose dead man's switch for a Hetzner Cloud project, designed to run
**on the very server it will eventually delete**.

You check in through a Telegram bot. If you stop answering for
`GRACE_PERIOD_DAYS` (default **7 days**), the program:

1. notifies you that teardown has started,
2. deletes every Floating IP,
3. deletes every **other** server and every other Primary IP,
4. takes a **live snapshot** of the still-running main server and protects it,
5. runs the (optional) billing hook,
6. prepares the main server's Primary IP so it survives,
7. deletes the main server — which also terminates this program.

The **main** server is never powered off or has its Primary IP unassigned before
the snapshot. (Other servers may be powered off only if a reserved IP still
attached to them must be detached.) Nothing is deleted if the main server or its
Primary IP cannot be resolved. Every step is resumable, idempotent, and gives up
after a configurable retry window instead of wedging forever.

---

## The one thing that must not go wrong

The program runs on the main server, reached through the main server's Primary
IP. Therefore it must never:

* power off / shut down the main server before the teardown is finished, or
* unassign the main server's Primary IP before deleting the server.

Doing either would kill the process mid-teardown with no way to wake up.

Hetzner's constraints that shape the implementation:

| Constraint | Consequence |
|---|---|
| `DELETE /servers/{id}` is refused while an attached Primary IP has delete protection | Delete protection on the main IP must be **off** while the server is deleted |
| `auto_delete: true` deletes the Primary IP together with the server | `auto_delete` must be **false** before the server delete |
| `POST /primary_ips/{id}/actions/unassign` requires the server to be powered off | The main IP is **never unassigned** while the main server runs |
| Snapshots of a running server are supported (`create_image`) | No shutdown is needed; the snapshot is live |
| `snapshot` images survive server deletion; `backup` images do not | Only `type: snapshot` is used |

The main IP is therefore prepared as: `auto_delete=false`, delete protection
**disabled only while the server is still attached**, then the server is deleted
(Hetzner detaches the IP and keeps it), then protection is re-enabled by a
best-effort post-delete loop. Even if the process is killed the instant the VM
disappears, the IP is already `auto_delete=false` and unassigned — it cannot be
deleted by the server removal.

Every snapshot is protected with `change_protection {"delete": true}` and no
snapshot is ever deleted. Existing snapshots are untouched.

---

## Quick start (Docker, on the main server)

```bash
cd /home/ubuntu/hetzner-deadman
cp .env.example .env
$EDITOR .env        # HCLOUD_TOKEN, MAIN_SERVER_ID, TELEGRAM_*
docker compose up -d --build
docker compose logs -f
```

Check it:

```bash
docker compose exec deadman python -m deadman status
docker compose exec deadman python -m deadman dry-run
```

`dry-run` performs only `GET` requests and prints exactly which resources would
be preserved and deleted. Review that output before setting `DRY_RUN=false`.

> The container has no privileges and does not mount loop devices. State lives
> in a named volume as `state.json`, `state.db` and a 4 MiB checksummed
> `state.block`. See [Persistence](#persistence). If you switch to a host
> bind mount (`./data:/data`), `chown -R 10001:10001 ./data` first — the image
> runs as UID 10001.

### Telegram bot

1. Create a bot with [@BotFather](https://t.me/BotFather), copy the token.
2. Send the bot any message.
3. `curl "https://api.telegram.org/bot<TOKEN>/getUpdates"` and copy
   `message.chat.id` into `TELEGRAM_CHAT_ID`.
4. Start the daemon. It sends a check-in message immediately (if none was sent
   yet) and every `CHECKIN_INTERVAL_HOURS`.

Commands accepted from the authorised chat:

| Command | Effect |
|---|---|
| any text, `/alive`, `/ping` | confirms you are online, resets the deadline |
| `/status` | phase, last check-in, time left, current teardown step |
| `/dryrun` | read-only Hetzner pre-flight report |
| `/abort`, `/cancel` | stops a running teardown **before** the main delete is issued |
| `/help` | command list |

By default **any** message during teardown aborts it
(`ABORT_ON_MESSAGE_DURING_TEARDOWN=true`), because regaining connectivity is
exactly when you want the brake.

---

## Teardown sequence

`MONITORING` → deadline reached → `TEARDOWN`:

| # | Step | What it does |
|---|---|---|
| 1 | `NOTIFY_TEARDOWN_START` | Telegram notice with inventory and deadline; never blocks the flow |
| 2 | `DELETE_FLOATING_IPS` | unprotect → unassign → delete → verify 404 |
| 3 | `DELETE_OTHER_SERVERS` | unprotect → ensure attached IPs are safe → delete → verify gone |
| 4 | `DELETE_OTHER_PRIMARY_IPS` | hard-skip reserved IPs → unprotect → power off owning server if needed → unassign → delete |
| 5 | `SNAPSHOT_MAIN_SERVER` | **live** `create_image{type:snapshot}` → poll action → poll image `available` → protect image |
| 6 | `SETTLE_BILLING` | explanation + optional webhook/command; cannot fail |
| 7 | `PREPARE_MAIN_PRIMARY_IP` | `auto_delete=false`; disable protection while attached; verify |
| 8 | `DELETE_MAIN_SERVER` | clear server protection → final Telegram message → `DELETE` → persist `DONE` immediately → re-protect the IP as soon as it detaches |

Then `DONE`. If the process still exists (e.g. it was run from another host),
it keeps answering `/status` and re-checks the Primary IP invariant every six
hours.

### Retry semantics

* A failed step is retried every `RETRY_INTERVAL_SECONDS` (default **1 hour**).
* After `MAX_RETRY_HOURS` (default **6 h**) without success, the step is skipped
  and the next step runs. The flow can never stall.
* `404` is success for every delete. `422 must_be_unassigned` triggers an
  unassign-and-retry sub-step. `422 server_not_stopped` triggers
  shutdown/poweroff (never for the main server). Load-balancer target errors
  trigger `remove_target`.
* `429` is handled inside the HTTP client using `RateLimit-Reset`; it does not
  consume the step retry budget.

The retry deadline and the next attempt time are persisted, so restarting the
container does not reset the clock or cause a hot loop.

### Snapshot gate

By default `REQUIRE_SNAPSHOT_BEFORE_MAIN_DELETE=false`: if snapshot creation
fails for the entire retry window, the teardown continues without one. This
matches the explicit instruction not to be billed indefinitely because Hetzner's
image API is broken. Set it to `true` if you prefer keeping the server forever
over deleting it without a backup. `touch /data/force_proceed` overrides the gate
once, deliberately, from the host.

---

## Configuration

Environment variables (also readable from a JSON file via `--config`; YAML works only if PyYAML is installed, which the Docker image intentionally omits).
Bare names are shown; every one also accepts a `DEADMAN_` prefix.

| Variable | Default | Meaning |
|---|---|---|
| `HCLOUD_TOKEN` | — | project-scoped API token (required) |
| `MAIN_SERVER_ID` | auto | the server this runs on |
| `MAIN_PRIMARY_IP_ID` / `MAIN_PRIMARY_IP_IDS` | auto | explicit Primary IP reservation |
| `MAIN_PRIMARY_IP` | — | reserve by address instead of id |
| `RESERVED_PRIMARY_IPS` / `RESERVED_IPS` | — | extra IDs/addresses never to delete |
| `RESERVE_MAIN_IPV6` | `true` | also preserve the main IPv6 /64 (auto-detection only: an explicit `MAIN_PRIMARY_IP*` is authoritative) |
| `TELEGRAM_BOT_TOKEN` | — | bot token |
| `TELEGRAM_CHAT_ID` | — | authorised chat |
| `TELEGRAM_USER_ID` | — | optional second authorisation check |
| `CHECKIN_INTERVAL_HOURS` | `24` | how often to ask for a check-in |
| `GRACE_PERIOD_DAYS` | `7` | silence before teardown |
| `RETRY_INTERVAL_SECONDS` | `3600` | delay between step attempts |
| `MAX_RETRY_HOURS` | `6` | per-step retry budget before skipping |
| `SKIP_STEP_AFTER_MAX_RETRY_HOURS` | `true` | advance instead of stalling |
| `REQUIRE_SNAPSHOT_BEFORE_MAIN_DELETE` | `false` | gate the main delete on a snapshot |
| `ABORT_ON_MESSAGE_DURING_TEARDOWN` | `true` | any message aborts before main delete |
| `DRY_RUN` | `false` | simulate every mutating call |
| `STATE_DIR` | `/data` | persistence directory |
| `ENABLE_BLOCK_FILE` | `true` | write `state.block` |
| `BLOCK_FILE_SIZE_MB` | `4` | block file size |
| `BILLING_MODE` | `auto` | `none`, `webhook`, `command` |
| `BILLING_WEBHOOK_URL` | — | POST target for the billing hook |
| `BILLING_COMMAND` | — | argv string, executed without a shell |
| `HEARTBEAT_FILE` | — | touching it also resets the timer |
| `LABEL_SELECTOR` | — | scope all lists/deletes to matching servers |

Legacy aliases are accepted (`DEADMAN_DAYS`, `X_DAYS`,
`DAYS_BEFORE_SELF_DESTRUCT`, `INACTIVITY_DAYS`, `GRACE_PERIOD_DAYS`,
`X_RETRY_HOURS`, `RETRY_HOURS`, `MAIN_PRIMARY_IP_ID`, …). In a JSON/YAML file,
`lease.timeout_hours` is interpreted as **hours** and converted to days
(`72` → `grace_period_days: 3`).

A JSON config equivalent is parsed too; see `config.example.json`.

---

## CLI

```bash
python3 -m deadman run                 # daemon (normal)
python3 -m deadman status [--json]     # current phase, deadline, steps
python3 -m deadman dry-run             # read-only Hetzner pre-flight
python3 -m deadman trigger-now         # create /data/trigger; daemon starts now
python3 -m deadman reset-timer         # reset deadline (daemon must be stopped)
python3 -m deadman verify-storage      # JSON + block + SQLite diagnostics
python3 -m deadman config              # effective config, token redacted
```

`trigger-now` is the "I am about to lose connectivity, do it now" button. The
trigger file is removed when teardown begins.

---

## Persistence

Three independent copies of the state document in `STATE_DIR`:

| File | Format | Purpose |
|---|---|---|
| `state.json` | atomically replaced JSON (tmp + `fsync` + `rename` + dir fsync) | authoritative, human-readable |
| `state.json.bak` | previous good document | corruption fallback |
| `state.block` | 4 MiB preallocated binary, A/B slots at 4096 and 2 MiB | survives torn writes; each slot: magic, version, generation, length, CRC32 + SHA-256 of payload, CRC32 of header |
| `state.db` | SQLite WAL, `synchronous=FULL` | `kv['state']` mirror + append-only `events` audit log |

Load order: `state.json` → `state.json.bak` → newest valid block slot →
`kv['state']`. A step that was `running` when the process died is re-verified
against the live API on startup; retry deadlines are preserved.

`flock` on `STATE_DIR/lock` guarantees one daemon. A second instance logs
`already_running` and exits `0`.

### Optional ext4 loop image

`setup-block-storage.sh --loop` creates `/var/lib/hetzner-deadman/state.img`
(16 MiB ext4), mounts it, and can add an fstab entry. This requires root and is
**not** required: the built-in `state.block` needs no privileges and works in
any container. `setup-block-storage.sh` with no arguments prepares a plain
host directory.

### Running without Docker

```bash
sudo ./setup-block-storage.sh --dir /var/lib/hetzner-deadman --owner "$USER"
STATE_DIR=/var/lib/hetzner-deadman HCLOUD_TOKEN=... \
  python3 -m deadman run
```

A minimal systemd unit:

```ini
[Unit]
Description=hetzner-deadman
After=network-online.target

[Service]
EnvironmentFile=/etc/hetzner-deadman.env
ExecStart=/usr/bin/python3 -m deadman run
WorkingDirectory=/opt/hetzner-deadman
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

---

## Billing

Hetzner Cloud and Robot expose **no** invoice, payment, credit or billing
endpoints. Account credit is applied to future invoices automatically and
outstanding balances reconcile roughly daily. There is nothing to call.

The `SETTLE_BILLING` step therefore records that fact and optionally calls a
user-supplied webhook or command. Whatever happens — timeout, DNS failure,
non-zero exit, HTTP 500, a bug — the step is `succeeded`/`skipped` and the flow
continues. It is placed immediately before the main delete, because afterwards
this process no longer exists.

---

## Safety checklist before going live

- [ ] `MAIN_SERVER_ID` is correct and belongs to this token's project.
- [ ] `MAIN_PRIMARY_IP_ID` (or `MAIN_PRIMARY_IP`) is set explicitly, not only
      auto-detected.
- [ ] `dry-run` shows the main IP under **preserve** and no surprises under
      **delete**.
- [ ] The token is project-scoped; a token from another project sees an empty
      project and the program refuses to act.
- [ ] Telegram check-in works and `/status` answers.
- [ ] The volume is persistent (`docker volume ls`, `docker compose down` does
      not remove it).
- [ ] You have tested `trigger-now` on a throwaway project, including the
      snapshot and the main IP surviving with protection re-enabled.

### Known limits

* Snapshots are billed per GB; the program never deletes them.
* If the whole Hetzner account is suspended, no step can run; retries eventually
  skip and the state remains on disk for inspection.
* The token must remain valid for the entire grace period. An expired token is
  retried hourly like any other error, then the step is skipped; it cannot be
  fixed remotely by this software.
* Running the daemon anywhere other than the main server is supported for
  testing, but the production design is "on the main server, delete thyself".

---

## Development

No third-party dependencies; Python ≥ 3.11.

```bash
python3 -m compileall deadman
python3 -m deadman --help
python3 -m deadman verify-storage --state-dir /tmp/deadman-test
python3 -m deadman reset-timer  --state-dir /tmp/deadman-test
python3 -m deadman status       --state-dir /tmp/deadman-test
```

To exercise the full flow without touching Hetzner, use `DRY_RUN=true` against a
throwaway project: every mutation is logged with method/path/body and synthetic
success responses keep the state machine moving.
