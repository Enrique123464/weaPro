import weaviate

client = weaviate.connect_to_local()

try:
    backup_id = "my-very-first-backup"  # The name you used when creating the backup
    backend = "filesystem"  # Usually "filesystem" for local backups
    
    print(f"Deleting backup: {backup_id}")
    
    # Delete the backup
    client.backup.delete(
        backup_id=backup_id,
        backend=backend
    )
    
    print("✓ Backup deleted successfully!")
    
except Exception as e:
    print(f"Error deleting backup: {e}")
    
finally:
    client.close()
    print("Connection closed")