import weaviate
from weaviate.classes.config import Configure, Reconfigure
import json

# Connect to Weaviate
client = weaviate.connect_to_local(
    headers={
        "X-OpenAI-Api-Key": "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",
    },
)

try:
    collection_name = "cdmx"
    
    if client.collections.exists(collection_name):
        print(f"Collection '{collection_name}' exists. Checking configuration...")
        
        collection = client.collections.get(collection_name)
        config = collection.config.get()
        
        print(f"Current reranker config: {config.reranker_config}")
        
        try:
            collection.config.update(
                reranker_config=Reconfigure.Reranker.transformers()
            )
            print("✓ Reranker configuration updated successfully!")
            
            # Verify the update
            updated_config = collection.config.get()
            print(f"Updated reranker config: {updated_config.reranker_config}")
            
        except Exception as e:
            print(f"✗ Could not update reranker configuration: {e}")
    else:
        print(f"Collection '{collection_name}' does not exist.")

except Exception as e:
    print(f"Error: {e}")
finally:
    client.close()
    print("\nConnection closed.")