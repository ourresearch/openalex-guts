import subprocess
from datetime import datetime


def backup_snapshot():
    current_date = datetime.now().strftime("%Y-%m-%d")
    print(f"Backing up snapshot for {current_date}...")

    command = f"aws s3 sync s3://openalex s3://openalex-sandbox/snapshot-backups/openalex-jsonl/{current_date}"
    result = subprocess.run(command, shell=True, check=True)

    if result.returncode == 0:
        print("Backup completed successfully.")
    else:
        print("Backup failed.")


if __name__ == "__main__":
    backup_snapshot()
