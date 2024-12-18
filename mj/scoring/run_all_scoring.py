import subprocess
import sys

# List of scoring scripts to run in sequence
SCORING_SCRIPTS = [
    "preprocessing.py",
    "accelerating.py",
    "braking.py",
    "speed.py",
    "cornering.py",
    "final_score.py"
]


def main():
    if len(sys.argv) != 2:
        print("Usage: python run_all_scoring.py <trip_id>")
        sys.exit(1)

    trip_id = sys.argv[1]

    # Run each script sequentially
    for script in SCORING_SCRIPTS:
        print(f"Running {script} for trip_id {trip_id}...")

        try:
            result = subprocess.run(
                ["python", script, trip_id],
                capture_output=True,
                text=True,
                check=True
            )
            print(result.stdout)

        except subprocess.CalledProcessError as e:
            print(f"Error running {script}:")
            print(f"--- Standard Output ---\n{e.stdout}")
            print(f"--- Error Output ---\n{e.stderr}")
            sys.exit(1)

    print("All scoring scripts completed successfully.")


if __name__ == "__main__":
    main()
