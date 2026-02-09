import weaviate
import json
from pathlib import Path
import re

def sanitize_document_name(filename: str) -> str:
    """
    Remove '-vol{x}' suffix from filenames.
    Example: 'CFR-2024-title48-vol1' -> 'CFR-2024-title48'
    """
    # Remove '-vol' followed by any number at the end
    sanitized = re.sub(r'-vol\d+$', '', filename)
    return sanitized

def populate_collections_batch(
    json_file_path: str, 
    client: weaviate.WeaviateClient,
    articles_collection_name: str,
    chunks_collection_name: str
):
    """
    Populate collections with proper ordering to avoid reference errors.
    """
    original_name = Path(json_file_path).stem
    document_name = sanitize_document_name(original_name)
    
    # Show if name was modified
    if original_name != document_name:
        print(f"  📝 Document name: '{original_name}' -> '{document_name}'")
    
    with open(json_file_path, 'r', encoding='utf-8') as f:
        articles_data = json.load(f)
    
    articles_collection = client.collections.get(articles_collection_name)
    chunks_collection = client.collections.get(chunks_collection_name)
    
    print(f"Processing {len(articles_data)} articles from '{document_name}'...")
    
    # Step 1: Insert ALL articles first
    article_uuids = {}  # Map article_name to UUID
    total_articles = len(articles_data)

    with articles_collection.batch.dynamic() as article_batch:
        for idx, article_data in enumerate(articles_data, 1):
            article_uuid = article_batch.add_object(
                properties={
                    "content": article_data["full_content"],
                    "article": article_data["article_name"],
                    "document": document_name
                }
            )
            article_uuids[article_data["article_name"]] = article_uuid
            
            # Print progress every 10% or every 100 articles, whichever is smaller
            if idx % max(1, min(100, total_articles // 10)) == 0 or idx == total_articles:
                print(f"  Articles: {idx}/{total_articles} ({idx/total_articles*100:.1f}%)")
            
    print(f"✅ Inserted {len(article_uuids)} articles")

    # Step 2: Now insert ALL chunks (articles are guaranteed to exist)
    chunk_count = 0
    # Pre-calculate total chunks for progress tracking
    total_chunks = sum(len(article_data.get("sub_chunks", [])) for article_data in articles_data)
    print(f"Inserting {total_chunks} chunks...")

    with chunks_collection.batch.dynamic() as chunk_batch:
        for article_data in articles_data:
            article_name = article_data["article_name"]
            article_uuid = article_uuids[article_name]
            
            for chunk in article_data.get("sub_chunks", []):
                chunk_batch.add_object(
                    properties={
                        "content": chunk["content"],
                        "article": article_name,
                        "document": document_name,
                        "start_offset": chunk["start_offset"]
                    },
                    references={
                        "source_article": article_uuid
                    }
                )
                chunk_count += 1
                
                # Print progress every 10% or every 500 chunks, whichever is smaller
                if chunk_count % max(1, min(500, total_chunks // 10)) == 0 or chunk_count == total_chunks:
                    print(f"  Chunks: {chunk_count}/{total_chunks} ({chunk_count/total_chunks*100:.1f}%)")

    print(f"✅ Inserted {chunk_count} chunks")
    print(f"  Articles: {len(article_uuids)}")
    print(f"  Chunks: {chunk_count}\n")
    
    return len(article_uuids), chunk_count


def process_all_json_files(
    directory_path: str, 
    client: weaviate.WeaviateClient,
    articles_collection_name: str,
    chunks_collection_name: str
):
    """
    Process all JSON files in the specified directory.
    """
    directory = Path(directory_path)
    
    # Get all JSON files in the directory
    json_files = list(directory.glob("*.json"))
    
    if not json_files:
        print(f"⚠️  No JSON files found in {directory_path}")
        return
    
    print(f"Found {len(json_files)} JSON files to process\n")
    print(f"Using collections: '{articles_collection_name}' and '{chunks_collection_name}'\n")
    print("=" * 60)
    
    total_articles = 0
    total_chunks = 0
    processed_files = 0
    failed_files = []
    
    for json_file in json_files:
        try:
            print(f"\n📄 Processing: {json_file.name}")
            print("-" * 60)
            
            articles, chunks = populate_collections_batch(
                str(json_file), 
                client,
                articles_collection_name,
                chunks_collection_name
            )
            total_articles += articles
            total_chunks += chunks
            processed_files += 1
            
        except Exception as e:
            print(f"❌ Error processing {json_file.name}: {str(e)}")
            failed_files.append(json_file.name)
    
    # Final summary
    print("\n" + "=" * 60)
    print("🎉 PROCESSING COMPLETE!")
    print("=" * 60)
    print(f"Files processed successfully: {processed_files}/{len(json_files)}")
    print(f"Total articles inserted: {total_articles}")
    print(f"Total chunks inserted: {total_chunks}")
    
    if failed_files:
        print(f"\n⚠️  Failed files ({len(failed_files)}):")
        for failed_file in failed_files:
            print(f"  - {failed_file}")


# Main execution
if __name__ == "__main__":
    # Configuration
    json_directory = "/workspace/data"
    articles_collection_name = "cfr_articles"
    chunks_collection_name = "cfr_chunks"
    
    # Connect to Weaviate
    client = weaviate.connect_to_local()
    
    try:
        process_all_json_files(
            json_directory, 
            client,
            articles_collection_name,
            chunks_collection_name
        )
        
    finally:
        client.close()