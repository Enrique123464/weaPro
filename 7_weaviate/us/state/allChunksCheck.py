import weaviate
import json
from weaviate.classes.init import Auth

# Connect to local Weaviate instance
client = weaviate.connect_to_local(
    #headers={
    #    "X-OpenAI-Api-Key": "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"
    #},
)


try:
    # Specify your collection name
    collection_name = "california_1"
    
    # Get the collection
    collection = client.collections.get(collection_name)
    
    # Save to JSONL file - write chunk by chunk
    output_file = "7_weaviate/weaviate_chunks.jsonl"
    count = 0
    
    with open(output_file, 'w', encoding='utf-8') as f:
        # Iterate and write each object immediately
        for item in collection.iterator():
            obj_data = {
                "id": str(item.uuid),
                "properties": item.properties,
                "vector": item.vector  # Optional: remove if not needed
            }
            f.write(json.dumps(obj_data) + '\n')
            count += 1
            
            # Optional: print progress every 1000 items
            if count % 1000 == 0:
                print(f"Processed {count} objects...")
    
    print(f"Successfully saved {count} objects to {output_file}")

finally:
    # Close the connection
    client.close()