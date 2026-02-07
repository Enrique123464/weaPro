import weaviate
from weaviate.classes.backup import BackupLocation

client = weaviate.connect_to_local()

try:
    result = client.backup.restore(
        backup_id="my-very-first-backup",
        backend="filesystem",
        wait_for_completion=True,
        #backup_location=BackupLocation.FileSystem(path="/var/lib/weaviate/backups")
    )

    print(result)

except Exception as e:
    print(f"Error restoring backup: {e}")
    
finally:
    client.close()
    print("Connection closed")