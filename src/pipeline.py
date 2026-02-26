from generate_data import main as generate_main
from ingest import main as ingest_main
from validate import main as validate_main
from transform import main as transform_main


def main():
    print("🚀 Starting Benin Mobile Money Pipeline...\n")

    print("1️ Generating data...")
    generate_main()

    print("\n2️ Ingesting data...")
    ingest_main()

    print("\n3️ Validating data...")
    validate_main()

    print("\n4️ Transforming data...")
    transform_main()

    print("\n Pipeline completed successfully!")


if __name__ == "__main__":
    main()