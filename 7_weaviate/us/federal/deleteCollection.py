import weaviate
from weaviate.classes.backup import BackupLocation

client = weaviate.connect_to_local()

try:
    collections_to_delete = ["cfr_articles", "cfr_chunks"]
    
    print(f"Deleting collections: {collections_to_delete}")
    
    client.collections.delete(collections_to_delete)
    
    print("✓ Collections deleted successfully!")
finally:
    client.close()
    print("Connection closed")