import weaviate
import json
from pathlib import Path

def populate_collections_batch(
    jsonl_file_path: str, 
    client: weaviate.WeaviateClient,
    articles_collection_name: str,
    chunks_collection_name: str
):
    """
    Populate collections from JSONL format with proper ordering to avoid reference errors.
    """
    # Read all lines from JSONL file
    articles_data = []
    with open(jsonl_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():  # Skip empty lines
                articles_data.append(json.loads(line))
    
    articles_collection = client.collections.get(articles_collection_name)
    chunks_collection = client.collections.get(chunks_collection_name)
    
    print(f"Processing {len(articles_data)} articles from '{jsonl_file_path}'...")
    
    # Step 1: Insert ALL articles first
    article_uuids = {}  # Map (document, article) tuple to UUID
    total_articles = len(articles_data)

    with articles_collection.batch.dynamic() as article_batch:
        for idx, article_data in enumerate(articles_data, 1):
            # Extract document (index 0) and article (list without index 0)
            article_name_list = article_data["article_name"]
            document = article_name_list[0]
            article = article_name_list[1:]  # Keep as list
            
            article_uuid = article_batch.add_object(
                properties={
                    "content": article_data["full_content"],
                    "article": article,
                    "document": document
                }
            )
            # Store UUID with both document and article as key (convert list to tuple for hashing)
            article_uuids[(document, tuple(article))] = article_uuid
            
            # Print progress every 10% or every 100 articles, whichever is smaller
            if idx % max(1, min(100, total_articles // 10)) == 0 or idx == total_articles:
                print(f"  Articles: {idx}/{total_articles} ({idx/total_articles*100:.1f}%)")
            
    print(f"✅ Inserted {len(article_uuids)} articles")

    # Step 2: Now insert ALL chunks (articles are guaranteed to exist)
    chunk_count = 0
    # Pre-calculate total chunks for progress tracking
    total_chunks = sum(len(article_data.get("chunks", [])) for article_data in articles_data)
    print(f"Inserting {total_chunks} chunks...")

    with chunks_collection.batch.dynamic() as chunk_batch:
        for article_data in articles_data:
            # Extract document and article same way as Step 1
            article_name_list = article_data["article_name"]
            document = article_name_list[0]
            article = article_name_list[1:]  # Keep as list
            
            article_uuid = article_uuids[(document, tuple(article))]
            
            for chunk in article_data.get("chunks", []):
                chunk_batch.add_object(
                    properties={
                        "content": chunk["content"],
                        "article": article,
                        "document": document,
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


def process_jsonl_file(
    file_path: str, 
    client: weaviate.WeaviateClient,
    articles_collection_name: str,
    chunks_collection_name: str
):
    """
    Process a single JSONL file.
    """
    file_path_obj = Path(file_path)
    
    if not file_path_obj.exists():
        print(f"⚠️  File not found: {file_path}")
        return
    
    print(f"Processing file: {file_path_obj.name}")
    print(f"Using collections: '{articles_collection_name}' and '{chunks_collection_name}'\n")
    print("=" * 60)
    
    try:
        print(f"\n📄 Processing: {file_path_obj.name}")
        print("-" * 60)
        
        articles, chunks = populate_collections_batch(
            str(file_path_obj), 
            client,
            articles_collection_name,
            chunks_collection_name
        )
        
        # Final summary
        print("\n" + "=" * 60)
        print("🎉 PROCESSING COMPLETE!")
        print("=" * 60)
        print(f"Total articles inserted: {articles}")
        print(f"Total chunks inserted: {chunks}")
        
    except Exception as e:
        print(f"❌ Error processing {file_path_obj.name}: {str(e)}")
        raise


# Main execution
if __name__ == "__main__":
    # Configuration
    jsonl_file_path = "/root/data/combined_california_reformatted.jsonl"
    articles_collection_name = "california_articles"
    chunks_collection_name = "california_chunks"
    
    # Connect to Weaviate
    client = weaviate.connect_to_local()
    
    try:
        process_jsonl_file(
            jsonl_file_path, 
            client,
            articles_collection_name,
            chunks_collection_name
        )
        
    finally:
        client.close()