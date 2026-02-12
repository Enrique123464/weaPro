import weaviate
import json
from pathlib import Path

def populate_collections_batch(
    json_file_path: str, 
    client: weaviate.WeaviateClient,
    articles_collection_name: str,
    chunks_collection_name: str
):
    """
    Populate collections with proper ordering to avoid reference errors.
    Handles new JSON structure with document_name at root.
    """
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    document_name = data.get("document_name", "")
    tipo = data.get("Tipo", "")
    articles_data = data.get("articles", [])
    
    print(f"  📝 Document name: '{document_name}'")
    
    articles_collection = client.collections.get(articles_collection_name)
    chunks_collection = client.collections.get(chunks_collection_name)
    
    print(f"Processing {len(articles_data)} articles from '{document_name}'...")
    
    # Step 1: Insert articles that have non-empty full_content
    article_uuids = {}  # Map article_index to UUID (or None if empty)
    total_articles = len(articles_data)
    inserted_articles = 0
    skipped_articles = 0

    with articles_collection.batch.dynamic() as article_batch:
        for idx, article_data in enumerate(articles_data):
            full_content = article_data.get("full_content", "")
            sub_chunks = article_data.get("sub_chunks", [])
            
            # Get article name from first sub_chunk
            if sub_chunks and len(sub_chunks) > 0:
                article_name = sub_chunks[0].get("name", "")
            else:
                article_name = f"article_{idx}"  # Fallback if no sub_chunks
            
            # Only insert if full_content is not empty
            if full_content.strip():
                article_uuid = article_batch.add_object(
                    properties={
                        "content": full_content,
                        "article": article_name,
                        "document": document_name
                    }
                )
                article_uuids[idx] = article_uuid  # Use index as key
                inserted_articles += 1
            else:
                # Mark as None - chunks will reference nothing
                article_uuids[idx] = None  # Use index as key
                skipped_articles += 1
            
            # Print progress every 10% or every 100 articles, whichever is smaller
            if (idx + 1) % max(1, min(100, total_articles // 10)) == 0 or (idx + 1) == total_articles:
                print(f"  Articles: {idx + 1}/{total_articles} ({(idx + 1)/total_articles*100:.1f}%) - Inserted: {inserted_articles}, Skipped: {skipped_articles}")
            
    print(f"✅ Processed {total_articles} articles: {inserted_articles} inserted, {skipped_articles} skipped (empty content)")

    # Step 2: Insert ALL chunks
    chunk_count = 0
    chunks_with_reference = 0
    chunks_without_reference = 0
    
    # Pre-calculate total chunks for progress tracking
    total_chunks = sum(len(article_data.get("sub_chunks", [])) for article_data in articles_data)
    print(f"Inserting {total_chunks} chunks...")

    with chunks_collection.batch.dynamic() as chunk_batch:
        for article_idx, article_data in enumerate(articles_data):
            sub_chunks = article_data.get("sub_chunks", [])
            
            if not sub_chunks:
                continue  # Skip if no sub_chunks
            
            # Get the UUID for this article using its index
            article_uuid = article_uuids.get(article_idx)
            
            for chunk in sub_chunks:
                chunk_properties = {
                    "content": chunk.get("content", ""),
                    "article": chunk.get("name", ""),
                    "tipo": tipo,
                    "document": document_name,
                    "start_offset": chunk.get("start_offset", 0)
                }
                
                # Only add reference if article was actually inserted
                if article_uuid is not None:
                    chunk_batch.add_object(
                        properties=chunk_properties,
                        references={
                            "source_article": article_uuid
                        }
                    )
                    chunks_with_reference += 1
                else:
                    # No reference - article had empty content
                    chunk_batch.add_object(
                        properties=chunk_properties
                    )
                    chunks_without_reference += 1
                
                chunk_count += 1
                
                # Print progress every 10% or every 500 chunks, whichever is smaller
                if chunk_count % max(1, min(500, total_chunks // 10)) == 0 or chunk_count == total_chunks:
                    print(f"  Chunks: {chunk_count}/{total_chunks} ({chunk_count/total_chunks*100:.1f}%)")

    print(f"✅ Inserted {chunk_count} chunks")
    print(f"  With article reference: {chunks_with_reference}")
    print(f"  Without reference (empty article): {chunks_without_reference}")
    print(f"  Articles inserted: {inserted_articles}")
    print(f"  Articles skipped: {skipped_articles}\n")
    
    return inserted_articles, chunk_count


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
        return 0, 0  # Return zeros for articles and chunks
    
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
    
    # Summary for this directory
    print("\n" + "=" * 60)
    print("📊 DIRECTORY SUMMARY")
    print("=" * 60)
    print(f"Files processed successfully: {processed_files}/{len(json_files)}")
    print(f"Articles inserted: {total_articles}")
    print(f"Chunks inserted: {total_chunks}")
    
    if failed_files:
        print(f"\n⚠️  Failed files ({len(failed_files)}):")
        for failed_file in failed_files:
            print(f"  - {failed_file}")
    
    return total_articles, total_chunks


# Main execution
if __name__ == "__main__":
    # Configuration - Add multiple directories here
    json_directories = [
        "/root/mxData/",
    ]
    
    articles_collection_name = "mx_federal_articles"
    chunks_collection_name = "mx_federal_chunks"
    
    # Connect to Weaviate
    client = weaviate.connect_to_local()
    
    try:
        print("=" * 60)
        print(f"🚀 STARTING BATCH PROCESSING")
        print(f"Processing {len(json_directories)} director{'y' if len(json_directories) == 1 else 'ies'}")
        print("=" * 60)
        
        overall_articles = 0
        overall_chunks = 0
        
        for idx, json_directory in enumerate(json_directories, 1):
            print(f"\n{'='*60}")
            print(f"📁 DIRECTORY {idx}/{len(json_directories)}: {json_directory}")
            print(f"{'='*60}")
            
            dir_articles, dir_chunks = process_all_json_files(
                json_directory, 
                client,
                articles_collection_name,
                chunks_collection_name
            )
            
            overall_articles += dir_articles
            overall_chunks += dir_chunks
        
        print("\n" + "=" * 60)
        print("🎊 ALL DIRECTORIES PROCESSED!")
        print("=" * 60)
        print(f"Total articles inserted across all directories: {overall_articles}")
        print(f"Total chunks inserted across all directories: {overall_chunks}")
        print("=" * 60)
        
    finally:
        client.close()