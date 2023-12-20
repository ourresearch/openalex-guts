import subprocess


def backup_snapshot():
    snapshot_date = '2023-11-21'
    print(f"Backing up snapshot for {snapshot_date}...")

    command = f"aws s3 sync s3://openalex s3://openalex-sandbox/snapshot-backups/openalex-jsonl/{snapshot_date}"
    result = subprocess.run(command, shell=True, check=True)

    if result.returncode == 0:
        print("Backup completed successfully.")
    else:
        print("Backup failed.")


if __name__ == "__main__":
    backup_snapshot()
