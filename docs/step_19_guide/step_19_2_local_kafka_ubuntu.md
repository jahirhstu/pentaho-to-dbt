# Step 19.2 — Install local Apache Kafka on Ubuntu

This guide sets up a **local development broker** directly on Ubuntu. It does
not use Docker or a cloud service. The Python producer and consumer planned for
later steps will run on the Ubuntu host and connect to `localhost:9092`.

The broker is a single-node KRaft cluster. This is suitable for development and
learning, not production: it has no replication or high availability. The
listener is bound to loopback so it is not exposed to the LAN.

The commands below use the Apache Kafka `4.4.0` binary release shown by the
[official Kafka quickstart](https://kafka.apache.org/quickstart/) and Java 17.
If you already installed Kafka, first identify its version and installation
path. Do not overwrite that installation or format its existing data directory.
The fresh-install commands below assume `/opt/kafka` is not already in use.

## 1. Check prerequisites and existing Kafka

Run these commands on Ubuntu:

```bash
java -version
command -v kafka-server-start.sh || true
systemctl list-unit-files | grep -i kafka || true
sudo find /opt /usr/local -maxdepth 4 -type f -name kafka-server-start.sh -print 2>/dev/null
```

Kafka 4.4 requires Java 17 or newer. If Java is missing or older, install the
Ubuntu Java runtime:

```bash
sudo apt update
sudo apt install -y openjdk-17-jre-headless curl
java -version
```

If the checks show an existing Kafka installation, note its version and path.
You can follow the configuration, topic, and verification sections using that
installation's `bin` directory, but adapt paths and config keys to that version.
Do not run the storage-format command on a directory that already contains
Kafka data. If you are unsure whether a directory has been initialized, stop
and inspect it before proceeding.

## 2. Download and install Kafka (fresh installation only)

Download the pinned Kafka archive and its SHA-512 checksum from Apache:

```bash
cd /tmp
curl -fLO https://downloads.apache.org/kafka/4.4.0/kafka_2.13-4.4.0.tgz
curl -fLO https://downloads.apache.org/kafka/4.4.0/kafka_2.13-4.4.0.tgz.sha512
sha512sum --check kafka_2.13-4.4.0.tgz.sha512
```

Continue only if the checksum reports `OK`. Extract the release and create a
stable path for the service and client commands:

```bash
sudo tar -xzf /tmp/kafka_2.13-4.4.0.tgz -C /opt
sudo ln -s /opt/kafka_2.13-4.4.0 /opt/kafka
/opt/kafka/bin/kafka-topics.sh --version
```

If `/opt/kafka` already exists, do not replace it; choose the path for the
installation you intend to use.

## 3. Create a dedicated service account and persistent directories

The Kafka service will run without root privileges:

```bash
getent passwd kafka >/dev/null || \
  sudo useradd --system --home-dir /var/lib/kafka --create-home \
    --shell /usr/sbin/nologin kafka

sudo install -d -o kafka -g kafka /var/lib/kafka/data /var/log/kafka
sudo install -d -o root -g root -m 0755 /etc/kafka
```

The data directory is under `/var/lib`, not `/tmp`, so Kafka messages and
cluster metadata survive a reboot.

## 4. Configure a loopback-only KRaft broker

Start from the configuration shipped with this Kafka release:

```bash
sudo cp /opt/kafka/config/server.properties /etc/kafka/server.properties
sudo nano /etc/kafka/server.properties
```

In the file, set these properties. Edit the existing lines rather than adding
duplicates:

```properties
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
log.dirs=/var/lib/kafka/data
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
```

Keep the release's `listener.security.protocol.map` line, which must map both
`PLAINTEXT` and `CONTROLLER` to `PLAINTEXT`. These settings create one local
broker/controller, one partition by default, and seven-day default retention.
Each topic below also receives an explicit seven-day retention setting.

Confirm there are no duplicate active definitions for the properties you edited:

```bash
grep -nE '^(process.roles|node.id|controller.quorum.bootstrap.servers|listeners|inter.broker.listener.name|advertised.listeners|controller.listener.names|listener.security.protocol.map|log.dirs|num.partitions|offsets.topic.replication.factor|transaction.state.log.replication.factor|transaction.state.log.min.isr|log.retention.hours)=' \
  /etc/kafka/server.properties
```

Ensure the broker can read the configuration and write its data/log directories:

```bash
sudo chown root:kafka /etc/kafka/server.properties
sudo chmod 0640 /etc/kafka/server.properties
sudo chown -R kafka:kafka /var/lib/kafka/data /var/log/kafka
```

### Initialize KRaft storage once

Check that the data directory is empty before formatting:

```bash
sudo find /var/lib/kafka/data -mindepth 1 -maxdepth 1 -print
```

For a **new, empty** data directory, generate a cluster ID and format it:

```bash
KAFKA_CLUSTER_ID="$(sudo -u kafka /opt/kafka/bin/kafka-storage.sh random-uuid)"
sudo -u kafka /opt/kafka/bin/kafka-storage.sh format \
  --standalone \
  -t "$KAFKA_CLUSTER_ID" \
  -c /etc/kafka/server.properties
```

Format is a one-time initialization, not a start/restart command. Never repeat
it against existing Kafka data to troubleshoot a startup problem; it can
destroy or disconnect existing cluster state.

## 5. Run Kafka under systemd

Create a systemd unit:

```bash
sudo nano /etc/systemd/system/kafka.service
```

Add:

```ini
[Unit]
Description=Apache Kafka local KRaft broker
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=kafka
Group=kafka
WorkingDirectory=/opt/kafka
Environment=LOG_DIR=/var/log/kafka
ExecStart=/opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties
Restart=on-failure
RestartSec=5
TimeoutStopSec=180
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
```

Load the unit, start Kafka now, and configure it to start after future reboots:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now kafka
sudo systemctl status kafka --no-pager
```

The status should show `active (running)`. Follow startup logs if it does not:

```bash
sudo journalctl -u kafka -n 100 --no-pager
sudo journalctl -u kafka -f
```

Press `Ctrl-C` to stop following logs; this does not stop the service.

Common service commands:

```bash
sudo systemctl stop kafka
sudo systemctl start kafka
sudo systemctl restart kafka
sudo systemctl disable kafka
```

`disable` prevents startup at boot; it does not stop a currently running
broker. Stop it separately when needed.

## 6. Create the three project topics

Set the CLI path for this terminal:

```bash
export KAFKA_HOME=/opt/kafka
```

Create each Step 19.1 topic with one partition, replication factor one, and
seven-day retention. `--if-not-exists` makes these commands safe to repeat:

```bash
for topic in \
  pentaho.project1.sales.v1 \
  pentaho.project1.customer.v1 \
  pentaho.project1.product.v1
do
  "$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=604800000
done
```

List the topics and inspect their partition and retention settings:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list

for topic in \
  pentaho.project1.sales.v1 \
  pentaho.project1.customer.v1 \
  pentaho.project1.product.v1
do
  "$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server localhost:9092 \
    --describe --topic "$topic"
  "$KAFKA_HOME/bin/kafka-configs.sh" \
    --bootstrap-server localhost:9092 \
    --entity-type topics \
    --entity-name "$topic" \
    --describe
done
```

Each topic should report one partition, replication factor 1, and
`retention.ms=604800000`. The broker does not need user credentials for this
isolated local setup. Do not change the listeners to `0.0.0.0` or open port
9092 in the firewall for this exercise.

## 7. Verify host-side produce and consume

First, verify a local TCP listener:

```bash
ss -ltn | grep ':9092'
```

Create a temporary connectivity-test topic:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic step19_2_connectivity_check \
  --partitions 1 --replication-factor 1
```

In **Terminal 1**, start a consumer:

```bash
"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --bootstrap-server localhost:9092 \
  --topic step19_2_connectivity_check \
  --from-beginning
```

In **Terminal 2**, send a test message:

```bash
printf '%s\n' 'step19.2-host-client-check' \
  | "$KAFKA_HOME/bin/kafka-console-producer.sh" \
      --bootstrap-server localhost:9092 \
      --topic step19_2_connectivity_check
```

Terminal 1 should display `step19.2-host-client-check`. Stop the consumer with
`Ctrl-C`, then delete the temporary topic:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server localhost:9092 \
  --delete --topic step19_2_connectivity_check
```

This verifies the local Kafka CLI can produce and consume through the host
listener. The Python applications are implemented and tested in later Step 19
substeps.

## 8. Troubleshooting

| Symptom | Checks |
|---|---|
| `active (failed)` or service exits | Run `sudo journalctl -u kafka -n 100 --no-pager`; check Java version, config paths, data-directory ownership, and whether the configured ports are already in use. |
| `Address already in use` | Run `sudo ss -ltnp | grep -E ':9092|:9093'`; stop the conflicting service or choose unused local ports and update the advertised listener and client bootstrap address consistently. |
| `Connection to node ... could not be established` | Confirm `systemctl status kafka`, `ss -ltn`, and `advertised.listeners=PLAINTEXT://localhost:9092`; clients on this Ubuntu host should use exactly `localhost:9092`. |
| Storage formatting reports existing metadata | Do not format again. Check `sudo find /var/lib/kafka/data -maxdepth 2 -type f -print` and inspect the startup logs. |
| Topic creation says it already exists | This is expected on repeat runs. Use `--describe` to inspect its current configuration; `--if-not-exists` does not change an existing topic's settings. |

If you need to change a topic's retention after creation, use Kafka's
`kafka-configs.sh --alter` command for that topic; recreating a topic is not
required.

## 9. Step 19.2 completion checklist

Only mark Step 19.2 **Done** in [steps.txt](../steps.txt) after verifying on
Ubuntu that:

- Kafka is installed from the intended distribution and Java requirement is met.
- `kafka.service` starts successfully and reaches `active (running)`.
- The broker is listening on loopback at `localhost:9092`, not exposed to the LAN.
- All three project topics exist with one partition, replication factor 1, and
  seven-day retention.
- The host-side console producer/consumer round-trip succeeds.
- The Kafka version, topic details, and verification result are recorded for
  the project.

This completes broker and topic setup only. Python producer/consumer
connectivity, Databricks ingestion, and watermark integration belong to later
Step 19 substeps.

## References

- [Apache Kafka quickstart](https://kafka.apache.org/quickstart/)
- [Apache Kafka KRaft documentation](https://kafka.apache.org/44/operations/kraft/)
- [Apache Kafka 4.4.0 release archive and checksums](https://downloads.apache.org/kafka/4.4.0/)
- [Step 19.1 streaming scope and contracts](../step_19_1_streaming_scope_and_contracts.md)
