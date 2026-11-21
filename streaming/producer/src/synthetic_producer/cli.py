import argparse
import json
import sys
from pathlib import Path

from .config import load_json, validate_allocations, validate_reference_manifest
from .contracts import TOPICS
from .manifest import read_run, write_generation
from .scenarios import build_events


def add_generate_arguments(parser):
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)


def add_publish_arguments(parser):
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--max-events-per-second", type=float, default=10.0)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="synthetic_producer")
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate = subparsers.add_parser("generate")
    add_generate_arguments(generate)
    publish = subparsers.add_parser("publish")
    add_publish_arguments(publish)
    args = parser.parse_args(argv)

    if args.command == "generate":
        config = load_json(args.config)
        references = validate_reference_manifest(load_json(config["references_file"]))
        producer_id, ranges = validate_allocations(load_json(config["allocation_file"]))
        if producer_id != config["producer_id"]:
            raise ValueError("scenario producer_id differs from allocation producer_id")
        events = build_events(config, references, producer_id, ranges)
        allocations = {"producer_id": producer_id, "ranges": ranges}
        manifest = write_generation(
            args.output,
            events,
            config,
            references,
            allocations,
            {"Faker": "37.12.0", "confluent-kafka": "2.12.2"},
        )
        print(json.dumps({"run_dir": args.output, "event_count": len(events), "manifest": manifest["scenario_id"]}))
        return 0

    if args.command == "publish":
        manifest, records = read_run(args.run_dir)
        manifest["_run_dir"] = args.run_dir
        try:
            from confluent_kafka import KafkaException, Producer
            from confluent_kafka.admin import AdminClient
        except ImportError as exc:
            print(f"publish unavailable: {exc}", file=sys.stderr)
            return 2

        references = manifest["reference_snapshot"]
        admin = AdminClient({"bootstrap.servers": args.bootstrap_servers, "security.protocol": "PLAINTEXT"})
        metadata = admin.list_topics(timeout=10.0)
        missing = [topic for topic in TOPICS.values() if topic not in metadata.topics]
        if missing:
            print("missing required Kafka topics: " + ", ".join(missing), file=sys.stderr)
            return 2

        producer = Producer({
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": manifest["producer_id"],
            "security.protocol": "PLAINTEXT",
            "enable.idempotence": True,
            "acks": "all",
            "allow.auto.create.topics": False,
            "delivery.timeout.ms": 120000,
        })
        from .publisher import PublishingError, publish_records

        try:
            publish_records(records, manifest, references, producer, args.max_events_per_second)
            print(json.dumps({"status": "success", "run_dir": args.run_dir, "events": len(records)}))
            return 0
        except (PublishingError, KafkaException, ValueError) as exc:
            print(str(exc), file=sys.stderr)
            return 1
