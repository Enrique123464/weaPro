import weaviate
import json
import time
from pathlib import Path
from typing import Dict, List, Any, Tuple


MAX_BATCH_TOKENS = 120000


def approx_tokens(text: str) -> int:
    """Approximate token count for text"""
    return max(1, len(text) // 4)


def initialize_output_file(output_file: Path) -> None:
    """Create output directory and initialize the oversized chunks file."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump([], f)


def append_oversized_chunk(output_file: Path, chunk_data: Dict[str, Any]) -> None:
    """Append a failed chunk to the oversized chunks file immediately."""
    # Read existing data
    with open(output_file, 'r', encoding='utf-8') as f:
        oversized_chunks = json.load(f)
    
    # Append new chunk
    oversized_chunks.append(chunk_data)
    
    # Write back
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(oversized_chunks, f, indent=2, ensure_ascii=False)


def get_json_files(json_dir: Path) -> List[Path]:
    """Get all JSON files from the specified directory."""
    json_files = list(json_dir.glob("*.json"))
    print(f"Found {len(json_files)} JSON files to process")
    return json_files


def load_document(json_file: Path) -> Dict[str, Any]:
    """Load a JSON document from file."""
    with open(json_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def create_chunk_object(paragraph: Dict, document_name: str, 
                       document_tipo: str, only_content: bool) -> Tuple[Dict[str, Any], str, int]:
    """
    Create a Weaviate object from a paragraph.
    
    Returns:
        Tuple of (chunk_object, article_name, token_count)
    """
    content = paragraph.get("content", "")
    prior_content = paragraph.get("priorContent", "")
    
    # If onlyContent is True, set article and fromArticle to empty strings
    if only_content:
        article = ""
        from_article = ""
    else:
        article = paragraph.get("name", "")
        from_article = paragraph.get("fromArticle", "")
    
    # Concatenate content and priorContent
    content_and_prior = f"{prior_content} {content}".strip()
    
    obj = {
        "content": content,
        "priorContent": prior_content,
        "contentAndPriorConcatenated": content_and_prior,
        "article": article,
        "fromArticle": [from_article] if from_article else [],
        "document": document_name,
        "tipo": document_tipo
    }
    
    # Calculate token count for the content field
    token_count = approx_tokens(content)
    
    return obj, article, token_count


def insert_batch_with_retry(collection, batch_items: List[Dict], 
                            output_file: Path, retry_delay: int = 60) -> Tuple[int, int]:
    """
    Insert a batch of chunks into Weaviate with retry logic.
    
    Returns:
        Tuple of (chunks_inserted, chunks_failed)
    """
    batch_size = len(batch_items)
    total_tokens = sum(item['token_count'] for item in batch_items)
    
    print(f"  Processing batch of {batch_size} chunks ({total_tokens:,} tokens)...")
    
    for attempt in range(2):  # 0 = first try, 1 = retry
        try:
            # Extract just the objects for insertion
            objects = [item['obj'] for item in batch_items]
            
            # Batch insert
            collection.data.insert_many(objects)
            
            print(f"  ✓ Successfully inserted batch of {batch_size} chunks")
            return batch_size, 0
            
        except Exception as e:
            if attempt == 0:  # First attempt failed
                print(f"  ⚠️  Batch insert failed, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:  # Retry also failed
                print(f"  ✗ Batch insert failed after retry: {str(e)[:100]}")
                print(f"  Saving {batch_size} failed chunks...")
                
                # Save all failed chunks
                for item in batch_items:
                    error_info = {
                        "error": str(e),
                        "chunk": item['paragraph'],
                        "document": item['document_name'],
                        "tipo": item['document_tipo'],
                        "file": item['file_name']
                    }
                    append_oversized_chunk(output_file, error_info)
                
                return 0, batch_size
    
    return 0, batch_size


def process_document(json_file: Path, collection, output_file: Path) -> Tuple[int, int]:
    """
    Process a single JSON document and insert its chunks into Weaviate in batches.
    
    Returns:
        Tuple of (chunks_inserted, chunks_failed)
    """
    print(f"Processing {json_file.name}...")
    
    data = load_document(json_file)
    
    document_name = data.get("name", "")
    document_tipo = data.get("Tipo", "")
    only_content = data.get("onlyContent", False)
    paragraphs = data.get("paragraphs_by_bold", [])
    total_in_file = len(paragraphs)
    
    print(f"  Total chunks in file: {total_in_file}")
    print(f"  Document type: {document_tipo}")
    print(f"  Only content mode: {only_content}")
    
    chunks_inserted = 0
    chunks_failed = 0
    
    current_batch = []
    current_batch_tokens = 0
    
    # Process each paragraph
    for idx, paragraph in enumerate(paragraphs, 1):
        obj, article, token_count = create_chunk_object(
            paragraph, document_name, document_tipo, only_content
        )
        
        # Create batch item with metadata
        batch_item = {
            'obj': obj,
            'article': article,
            'token_count': token_count,
            'paragraph': paragraph,
            'document_name': document_name,
            'document_tipo': document_tipo,
            'file_name': json_file.name
        }
        
        # Check if adding this chunk would exceed the token limit
        if current_batch and (current_batch_tokens + token_count > MAX_BATCH_TOKENS):
            # Process current batch without this chunk
            inserted, failed = insert_batch_with_retry(collection, current_batch, output_file)
            chunks_inserted += inserted
            chunks_failed += failed
            
            # Start new batch with current chunk
            current_batch = [batch_item]
            current_batch_tokens = token_count
            
        elif token_count > MAX_BATCH_TOKENS:
            # Single chunk exceeds limit - process it alone
            if current_batch:
                # First process any pending batch
                inserted, failed = insert_batch_with_retry(collection, current_batch, output_file)
                chunks_inserted += inserted
                chunks_failed += failed
                current_batch = []
                current_batch_tokens = 0
            
            # Process oversized chunk as single-item batch
            print(f"  ⚠️  Chunk {idx} has {token_count:,} tokens (exceeds {MAX_BATCH_TOKENS:,})")
            inserted, failed = insert_batch_with_retry(collection, [batch_item], output_file)
            chunks_inserted += inserted
            chunks_failed += failed
            
        else:
            # Add chunk to current batch
            current_batch.append(batch_item)
            current_batch_tokens += token_count
    
    # Process any remaining chunks in the batch
    if current_batch:
        inserted, failed = insert_batch_with_retry(collection, current_batch, output_file)
        chunks_inserted += inserted
        chunks_failed += failed
    
    print(f"  Completed {json_file.name}")
    return chunks_inserted, chunks_failed


def print_summary(total_chunks: int, total_failed: int, output_file: Path) -> None:
    """Print the import summary."""
    print(f"\n{'='*50}")
    print(f"Import Summary:")
    print(f"Total chunks inserted: {total_chunks}")
    print(f"Total failed chunks: {total_failed}")
    
    if total_failed > 0:
        print(f"\n⚠️  Failed chunks saved to: {output_file}")
    
    print("\nData import completed!")


def main():
    """Main function to run the Weaviate import process."""
    # Configuration
    collection_name = "reg_ley_federalBatch"
    json_dir = Path("D:/data/Legalis/Prepro/Federal/Reglamento de Ley Federal/clean")
    output_file = Path("7_weaviate/mx/oversized_chunks.json")
    
    # Initialize
    initialize_output_file(output_file)
    client = weaviate.connect_to_local()
    
    try:
        collection = client.collections.get(collection_name)
        json_files = get_json_files(json_dir)
        
        total_chunks = 0
        total_failed = 0
        
        # Process each file
        for json_file in json_files:
            chunks_inserted, chunks_failed = process_document(
                json_file, collection, output_file
            )
            total_chunks += chunks_inserted
            total_failed += chunks_failed
        
        print_summary(total_chunks, total_failed, output_file)
        
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        client.close()


if __name__ == "__main__":
    main()