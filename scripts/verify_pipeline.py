"""Run the Kafka → PostgreSQL → API check. Requires the Compose stack."""

from scripts.pipeline_flow import main

if __name__ == "__main__":
    main()
