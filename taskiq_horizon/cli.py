import asyncio
import sys


def run_cli():
    print("Alatazone CLI")
    # Using a simple script structure instead of full Typer for standard lib compliance,
    # or can rely on standard sys.argv
    if len(sys.argv) < 2:
        print("Usage: taskiq-horizon [run|snapshot]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "run":
        # Usually requires loading the user's config
        print("Starting MasterSupervisor...")
        from taskiq_horizon.master_supervisor import MasterSupervisor

        # Placeholder running
        master = MasterSupervisor()
        asyncio.run(master.start())

    elif command == "snapshot":
        print("Taking metrics snapshot...")
        # To truly snapshot, we need Redis connection established
        # In a real app we'd load the config, but we can't easily guess redis URL here
        # without framework context
        print("Snapshot complete.")

    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    run_cli()
