import weaviate
import json
import os
import time
from pathlib import Path
from weaviate.classes.config import Configure, Property, DataType

# Connect to Weaviate
client = weaviate.connect_to_local(
    headers={
        "X-OpenAI-Api-Key": "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"
    },
)

collection_name = "cdmx"

# Output file for oversized chunks
output_file = Path("6.51_simplerApproach/oversized_chunks.json")
output_file.parent.mkdir(parents=True, exist_ok=True)

# Initialize the output file with an empty list
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump([], f)

def append_oversized_chunk(chunk_data):
    """Append a chunk to the oversized chunks file immediately"""
    # Read existing data
    with open(output_file, 'r', encoding='utf-8') as f:
        oversized_chunks = json.load(f)
    
    # Append new chunk
    oversized_chunks.append(chunk_data)
    
    # Write back
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(oversized_chunks, f, indent=2, ensure_ascii=False)

try:
    # Create collection with vector configuration
    # Delete collection if it exists (for fresh start)
    if client.collections.exists(collection_name):
        client.collections.delete(collection_name)
    
    client.collections.create(
        name=collection_name,
        properties=[
            Property(name="content", data_type=DataType.TEXT),
            Property(name="priorContent", data_type=DataType.TEXT, skip_vectorization=True),
            Property(name="contentAndPriorConcatenated", data_type=DataType.TEXT, skip_vectorization=True),
            Property(name="article", data_type=DataType.TEXT, skip_vectorization=True),
            Property(name="fromArticle", data_type=DataType.TEXT, skip_vectorization=True),
            Property(name="document", data_type=DataType.TEXT, skip_vectorization=True)
        ],
        vector_config=[
            Configure.Vectors.text2vec_openai(
                vectorize_collection_name=False,
                name="openaiVector",
                model="text-embedding-3-large",
                source_properties=["content"],
            )
        ]
    )
    
    print(f"Collection '{collection_name}' created successfully")
    
    # Get the collection
    collection = client.collections.get(collection_name)
    
    # Read all JSON files from the directory
    json_dir = Path("6.51_simplerApproach/clean")
    json_files = list(json_dir.glob("*.json"))
    
    print(f"Found {len(json_files)} JSON files to process")
    
    # Process each JSON file
    total_chunks = 0
    total_failed = 0
    
    for json_file in json_files:
        print(f"Processing {json_file.name}...")
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        document_name = data.get("name", "")
        paragraphs = data.get("paragraphs_by_bold", [])
        total_in_file = len(paragraphs)
        
        print(f"  Total chunks in file: {total_in_file}")
        
        # Process chunks one by one
        for idx, paragraph in enumerate(paragraphs, 1):
            content = paragraph.get("content", "")
            prior_content = paragraph.get("priorContent", "")
            article = paragraph.get("name", "")
            from_article = paragraph.get("fromArticle", "")
            
            # Concatenate content and priorContent
            content_and_prior = f"{prior_content} {content}".strip()
            
            obj = {
                "content": content,
                "priorContent": prior_content,
                "contentAndPriorConcatenated": content_and_prior,
                "article": article,
                "fromArticle": from_article,
                "document": document_name
            }
            
            # Try to insert, with one retry on failure
            success = False
            for attempt in range(2):  # 0 = first try, 1 = retry
                try:
                    collection.data.insert(obj)
                    total_chunks += 1
                    success = True
                    print(f"  ✓ [{idx}/{total_in_file}] Inserted chunk from {article}")
                    break
                except Exception as e:
                    if attempt == 0:  # First attempt failed
                        print(f"  ⚠️  [{idx}/{total_in_file}] Failed to insert chunk from {article}, retrying in 60 seconds...")
                        time.sleep(60)
                    else:  # Retry also failed
                        error_info = {
                            "error": str(e),
                            "chunk": paragraph,
                            "document": document_name,
                            "file": json_file.name
                        }
                        append_oversized_chunk(error_info)
                        total_failed += 1
                        print(f"  ✗ [{idx}/{total_in_file}] Failed to insert chunk from {article} after retry: {str(e)[:100]}")
        
        print(f"  Completed {json_file.name}")
    
    print(f"\n{'='*50}")
    print(f"Import Summary:")
    print(f"Total chunks inserted: {total_chunks}")
    print(f"Total failed chunks: {total_failed}")
    
    if total_failed > 0:
        print(f"\n⚠️  Failed chunks saved to: {output_file}")
    
    print("\nData import completed!")

except Exception as e:
    print(f"An error occurred: {e}")
    import traceback
    traceback.print_exc()

finally:
    client.close()