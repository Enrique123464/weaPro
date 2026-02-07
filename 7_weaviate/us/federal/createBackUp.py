import weaviate
from weaviate.classes.backup import BackupLocation

client = weaviate.connect_to_local()

try:
    result = client.backup.create(
        backup_id="my-very-first-backup",
        backend="filesystem",
        include_collections=["cfr_articles", "cfr_chunks"],
        wait_for_completion=True,
        #backup_location=BackupLocation.FileSystem(path="/var/lib/weaviate/backups")
    )
    
    print(result)
finally:
    client.close()