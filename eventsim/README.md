# Eventsim

Eventsim is a **fake music web-site event generator** written in Scala. It produces realistic-looking streams of page-request events — think Spotify user behaviour — without containing any real user data. Events can be written to JSON files or streamed directly to Apache Kafka.

> **Attribution** — This Docker image is based on [viirya/eventsim](https://github.com/viirya/eventsim).

---

## Table of Contents

- [How it works](#how-it-works)
- [Directory layout](#directory-layout)
- [Data files](#data-files)
- [Running with Docker Compose](#running-with-docker-compose)
- [CLI reference](#cli-reference)
- [Config file deep-dive](#config-file-deep-dive)
  - [Top-level parameters](#top-level-parameters)
  - [User levels and auth states](#user-levels-and-auth-states)
  - [Session entry points (`new-session`)](#session-entry-points-new-session)
  - [State-machine transitions (`transitions`)](#state-machine-transitions-transitions)
  - [Visibility control (`show-user-details`)](#visibility-control-show-user-details)
- [Tuning the traffic shape](#tuning-the-traffic-shape)
- [A/B testing and parallel generation](#ab-testing-and-parallel-generation)
- [Output format](#output-format)

---

## How it works

Eventsim models each user as a state machine that traverses a configurable set of web-site pages. Internally:

1. **User generation** — On start-up a pool of N users is created. Each user is assigned random properties (name, location, engagement level) drawn from real-world statistical distributions.
2. **Priority queue** — All user sessions are placed in a min-heap ordered by the timestamp of their next event. The simulator pops the earliest session, emits the event, computes the *next* event for that session (or schedules the next session for the user), and re-inserts it.
3. **Session timing** — The inter-event gap follows a **log-normal distribution** (mean = `alpha` seconds). The gap between sessions follows an **exponential distribution** (mean = `beta` seconds), with a configurable minimum floor (`session-gap`).
4. **State transitions** — From any page the config file defines a set of `(dest, probability)` pairs. If the probabilities sum to less than 1.0, the remainder becomes the probability of ending the session.
5. **Song events** — `NextSong` transitions are special: the next event fires *after the song duration* rather than after a random log-normal delay.
6. **Redirect events** — Pages like `Login` (status 307) fire their follow-up at a fixed short delay to model form-submit redirects.
7. **Traffic shaping** — Optional damping factors reduce traffic at night or on weekends by scaling arrival probabilities with a sine curve (night) or a linear ramp around midnight (weekends/holidays).

---

## Directory layout

```
eventsim/
├── Dockerfile                # Builds the container image (Java 11 / OpenJDK)
├── eventsim.sh               # Entrypoint — launches the JAR with G1GC and -Xmx4G
├── examples/
│   └── example-config.json   # Full reference config used by docker-compose
├── data/                     # Static reference data loaded at runtime
│   ├── songs_analysis.txt.gz      # ~385 k tracks from the Million Song Dataset
│   ├── listen_counts.txt.gz       # Per-song popularity weights
│   ├── Top1000Surnames.csv        # US Census last-name distribution
│   ├── yob1990.txt                # SSA first-name distribution (1990 cohort)
│   ├── US.txt                     # GeoNames US place-name database
│   ├── Gaz_zcta_national.txt      # US ZIP-code centroid data
│   └── user agents.txt            # Common browser user-agent strings
└── target/
    └── eventsim-assembly-2.0.jar  # Fat JAR (must be present before building image)
```

---

## Data files

| File | Source | Purpose |
|------|--------|---------|
| `songs_analysis.txt.gz` | Million Song Dataset | Song titles, artists, durations |
| `listen_counts.txt.gz` | Derived | Popularity weights used for song selection |
| `Top1000Surnames.csv` | US Census Bureau | Random last-name generation |
| `yob1990.txt` | Social Security Administration | Random first-name generation |
| `US.txt` | GeoNames | City / state assignment |
| `Gaz_zcta_national.txt` | US Census Bureau | ZIP code coordinates |
| `user agents.txt` | willshouse.com survey | Realistic browser user-agent strings |

These files are baked into the Docker image (`COPY data /opt/eventsim/data`) and loaded at start-up. The song file alone contains ~385 k tracks and may take a few seconds to parse.

---

## Running with Docker Compose

The `docker-compose.yml` in the project root starts eventsim as part of the full stack. The relevant service snippet is:

```yaml
eventsim:
  build:
    context: ./eventsim
    dockerfile: Dockerfile
  container_name: eventsim
  depends_on:
    kafka:
      condition: service_healthy
  environment:
    - JAVA_OPTS=-Xmx4G
  command:
    - |
      /opt/eventsim/eventsim.sh \
        -c /opt/eventsim/examples/example-config.json \
        --from 730 \
        --to 0 \
        --nusers 10 \
        --growth-rate 0.001 \
        --userid 1 \
        --kafkaBrokerList kafka:9092 \
        --randomseed $$(shuf -i 1-10000 -n 1) \
        --continuous
  restart: unless-stopped
  mem_limit: 6g
```

**What this does:**
- Replays **2 years** of simulated history (`--from 730 --to 0`), then switches to **real-time continuous** streaming (`--continuous`).
- Starts with **10 users** growing at 0.1 % annually, generating a manageable but realistic flow.
- Outputs events to the **`kafka:9092`** broker. Topics are auto-created by Kafka.
- Uses a **random seed** so each container restart produces a different (but reproducible) run if you fix the seed.

### Start only eventsim

```bash
docker compose up eventsim --build
```

### Watch the Kafka topic

Use [Kafdrop](http://localhost:9002) (also in the stack) or:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic listen_events \
  --from-beginning
```

---

## CLI reference

```
eventsim.sh [options] [output-dir]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-c, --config <file>` | *(required)* | Path to the JSON config file |
| `-n, --nusers <N>` | `1` | Initial number of simulated users |
| `-u, --userid <N>` | `1` | User ID assigned to the first generated user (sequential from here) |
| `-r, --randomseed <N>` | random | Seed for the PRNG — fix this for reproducible data |
| `-g, --growth-rate <f>` | `0.0` | Annual user growth rate as a fraction (`0.01` = 1 %) |
| `-a, --attrition-rate <f>` | `0.0` | Annual user attrition rate as a fraction |
| `--from <days>` | `15` | Start time = N days ago |
| `--to <days>` | `1` | End time = N days ago |
| `-s, --start-time <ISO8601>` | derived | Explicit start timestamp |
| `-e, --end-time <ISO8601>` | derived | Explicit end timestamp |
| `--continuous` / `--nocontinuous` | `--nocontinuous` | Keep running in real-time after reaching end time |
| `-k, --kafkaBrokerList <host:port>` | *(none)* | Stream events to Kafka instead of files |
| `--useAvro` / `--nouseAvro` | `--nouseAvro` | Output Avro binary instead of JSON |
| `--tag <string>` | *(none)* | Tag appended to every event (useful for A/B test labelling) |
| `--generate-counts` | — | Pre-compute listen-counts file then exit |
| `--generate-similars` | — | Pre-compute similar-song file then exit |

Parameters are resolved in this order (highest wins): **CLI flag > config file > built-in default**.

---

## Config file deep-dive

The config file (`examples/example-config.json`) is a single JSON object that controls every aspect of the simulation. There is no schema validation — unknown keys are silently ignored.

### Top-level parameters

```jsonc
{
  "seed": 1,              // PRNG seed — change for different fake data
  "alpha": 90.0,          // Mean seconds between events within a session (log-normal)
  "beta": 604800.0,       // Mean seconds between sessions (exponential, ~1 week)
  "damping": 0.09375,     // Depth of the day/night traffic cycle (0 = flat)
  "weekend-damping": 0.5, // Traffic multiplier on weekends/holidays (0.5 = 50 % of weekday)
  "weekend-damping-offset": 180,  // Minutes before midnight that weekend starts
  "weekend-damping-scale": 360,   // Duration of the traffic ramp, in minutes
  "session-gap": 1800,    // Minimum seconds between two sessions for the same user
  "churned-state": "Cancelled"    // Auth state that marks a user as permanently churned
}
```

> **Tip — `alpha` and `beta`**: Lower `alpha` → users click faster. Lower `beta` → users come back more often. Both are means of their respective distributions; actual per-user values are sampled randomly.

> **Tip — `damping`**: Set to `0` for flat 24/7 traffic. Set to `0.09375` (the default) for a mild day/night cycle. Values above `~0.3` produce very pronounced peaks and near-zero night traffic.

---

### User levels and auth states

Levels and auth states are the two dimensions of a user's current state. They appear in every transition.

```jsonc
"levels": [
  {"level": "free", "weight": 10},
  {"level": "paid", "weight": 2}
],
"auths": [
  {"auth": "Guest",     "weight": 0},
  {"auth": "Logged In", "weight": 10},
  {"auth": "Logged Out","weight": 1}
],
"new-user-auth": "Guest",   // Auth state assigned when a brand-new user first arrives
"new-user-level": "free"    // Subscription level for new users
```

`weight` controls the relative frequency with which users are initialised into that state when starting a **new session** (not a new user). A weight of `0` means the state is never chosen as a session entry point directly.

---

### Session entry points (`new-session`)

`new-session` defines the distribution of pages that users can *start* a session on. Each entry is a fully-qualified state (page + method + status + auth + level) with a `weight`:

```jsonc
"new-session": [
  {"page":"Home",    "method":"GET","status":200,"auth":"Logged In","level":"free","weight":1000},
  {"page":"NextSong","method":"PUT","status":200,"auth":"Logged In","level":"free","weight":1500},
  {"page":"Home",    "method":"GET","status":200,"auth":"Guest",    "level":"free","weight":100},
  ...
]
```

Higher weight → more sessions start on that page. The example config is heavily weighted toward `NextSong` and `Home` for logged-in users, which means most sessions open mid-stream — a natural behaviour for a music app.

---

### State-machine transitions (`transitions`)

The `transitions` array is the core of the simulation. Each entry defines one directed edge in the state machine:

```jsonc
{
  "source": {"page":"Home","method":"GET","status":200,"auth":"Logged In","level":"free"},
  "dest":   {"page":"NextSong","method":"PUT","status":200,"auth":"Logged In","level":"free"},
  "p": 0.6
}
```

**Rules:**
- **`p`** is the probability of taking this transition *given the user is currently in `source`*.
- All `p` values for a given source state are summed. If the sum is `S < 1.0`, then `1.0 - S` is the probability that the session **ends** after this state.
- Setting `S = 1.0` for a state means the user never leaves voluntarily from there (this is intentional for redirect pages like `Logout`).
- A transition that **changes `auth`** (e.g. `Logged Out → Logged In` via the `Login` page) or **changes `level`** (e.g. `free → paid` via `Submit Upgrade`) models real account-state transitions.

**Example flow for a free logged-in user from `Home`:**

| Destination | p |
|-------------|---|
| `NextSong` | 0.600 |
| `Home` (refresh) | 0.010 |
| `Settings` | 0.020 |
| `Upgrade` | 0.010 |
| `Logout` | 0.010 |
| `Error` (404) | 0.001 |
| `About` | 0.002 |
| `Help` | 0.002 |
| **End session** | **0.345** |

**Monitoring session-end probability**: if the total `p` for a state seems lower than expected, sessions will terminate there more often. This is useful for modelling high-bounce landing pages.

---

### Visibility control (`show-user-details`)

Controls whether events for users in a given auth state appear in the output:

```jsonc
"show-user-details": [
  {"auth": "Guest",     "show": false},
  {"auth": "Logged In", "show": true},
  {"auth": "Logged Out","show": false},
  {"auth": "Cancelled", "show": true}
]
```

Setting `"show": false` for `Guest` and `"Logged Out"` means anonymous and logged-out page hits are **not emitted** — the output only contains events for identified users. Adjust this to include or exclude anonymous traffic in your data pipeline.

---

## Tuning the traffic shape

| Goal | What to change |
|------|---------------|
| More events per user session | Lower `alpha` (e.g. `60.0`) |
| Users return more frequently | Lower `beta` (e.g. `86400.0` = daily) |
| Stronger day/night pattern | Raise `damping` (e.g. `0.2`) |
| Suppress weekend traffic entirely | Set `weekend-damping` to `0` |
| Faster user growth | Set `--growth-rate 0.05` (5 % annually) |
| Scale data volume | Increase `--nusers` |
| Reproducible runs | Fix `--randomseed` |
| Longer historical backfill | Increase `--from` (e.g. `--from 1095` for 3 years) |

---

## A/B testing and parallel generation

Generate two parallel datasets covering the same time window with different config parameters, user ID ranges, seeds, and tags:

```bash
# Control group
eventsim.sh \
  --config examples/example-config.json \
  --tag control \
  --nusers 5000 \
  --start-time "2025-01-01T00:00:00" \
  --end-time   "2025-07-01T00:00:00" \
  --growth-rate 0.25 \
  --userid 1 \
  --randomseed 1 \
  control.json

# Test group (different seed, different user ID range, different params)
eventsim.sh \
  --config examples/alt-example-config.json \
  --tag test \
  --nusers 5000 \
  --start-time "2025-01-01T00:00:00" \
  --end-time   "2025-07-01T00:00:00" \
  --growth-rate 0.25 \
  --userid 5001 \
  --randomseed 2 \
  test.json
```

**Key rules for parallel runs:**
- Use **non-overlapping `--userid` ranges** — user IDs are assigned sequentially and must be unique across files.
- Use **different `--randomseed` values** — same seed produces identical data.
- Keep **start and end times identical** — the generator produces partial (incomplete) sessions at the boundaries; mixing time windows will produce inconsistent data.

---

## Output format

Each emitted event is a JSON object (one per line). Fields vary by page type, but a typical `NextSong` event looks like:

```json
{
  "artist": "The Beatles",
  "auth": "Logged In",
  "firstName": "Jane",
  "gender": "F",
  "itemInSession": 4,
  "lastName": "Doe",
  "length": 234.56,
  "level": "paid",
  "location": "Austin, TX",
  "method": "PUT",
  "page": "NextSong",
  "registration": 1609459200000,
  "sessionId": 42,
  "song": "Come Together",
  "status": 200,
  "ts": 1735689600000,
  "userAgent": "Mozilla/5.0 ...",
  "userId": 7
}
```

When streaming to Kafka, each event is published to a topic named after the `page` value (e.g. `NextSong`, `Home`, `Login`). Topic names are auto-created by the broker.
