import weaviate
import json
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================
collection_name = "california_1"
MAX_BATCH_TOKENS = 136000
fileName = "calTitle1_regulations_content"
jsonl_path = Path("D:/data/Legalis/Prepro/USA/State Regulations/California/calTitle1_regulations_content.jsonl")
output_file = Path("7_weaviate/failed_chunks.jsonl")

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def approx_tokens(text: str) -> int:
    """Approximate token count for text"""
    return max(1, len(text) // 4)

def setup_output_file(file_path: Path):
    """Create output directory and clear existing file"""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    if file_path.exists():
        file_path.unlink()

def append_failed_chunk(chunk_data, file_path: Path):
    """Append a failed chunk to the JSONL file"""
    with open(file_path, 'a', encoding='utf-8') as f:
        json.dump(chunk_data, f, ensure_ascii=False)
        f.write('\n')

# ============================================================================
# DATA EXTRACTION FUNCTIONS
# ============================================================================

def extract_parent_info(data: dict) -> tuple[str, list[str]]:
    """Extract article and fromArticle from data
    
    Returns:
        tuple: (article, fromArticle list)
    """
    parent_keys = sorted(
        [k for k in data.keys() if k.startswith('parent_')],
        key=lambda x: int(x.split('_')[1])
    )
    
    # Get article (last parent)
    article = data.get(parent_keys[-1], "") if parent_keys else ""
    
    # Get fromArticle (all parents except parent_0)
    from_article = [data[k] for k in parent_keys[1:]] if len(parent_keys) > 1 else []
    
    return article, from_article

def create_chunk_properties(chunk: dict, article: str, from_article: list[str], document: str) -> dict:
    """Create properties object for a chunk
    
    Args:
        chunk: Raw chunk data
        article: Article identifier
        from_article: List of parent articles
        document: Document name
        
    Returns:
        dict: Properties ready for Weaviate insertion
    """
    content = chunk.get('content', '')
    prior_content = chunk.get('priorContent', '')
    content_and_prior = f"{prior_content} {content}".strip()
    
    return {
        "content": content,
        "priorContent": prior_content,
        "contentAndPriorConcatenated": content_and_prior,
        "article": article,
        "fromArticle": from_article,
        "document": document
    }

# ============================================================================
# BATCH MANAGEMENT FUNCTIONS
# ============================================================================

def send_batch_to_weaviate(batch_objects: list[dict], collection, batch_number: int, 
                           current_tokens: int, stats: dict, failed_file: Path):
    """Send a batch to Weaviate and update statistics"""
    if not batch_objects:
        return
    
    batch_size = len(batch_objects)
    
    try:
        with collection.batch.dynamic() as batch:
            for obj in batch_objects:
                batch.add_object(properties=obj)
        
        # Check for failed objects AFTER the context manager exits
        failed_objects = collection.batch.failed_objects
        
        if failed_objects:
            for failed in failed_objects:
                stats['failed'] += 1
                print(f"  ✗ Failed object in batch #{batch_number}: {failed.message}")
                append_failed_chunk({
                    "error": failed.message,
                    "properties": failed.object_.properties if hasattr(failed, 'object_') else None,
                    "batch_number": batch_number
                }, failed_file)
            
            successful = batch_size - len(failed_objects)
            stats['imported'] += successful
            print(f"  ⚠ Batch #{batch_number}: Partial success - {successful}/{batch_size} chunks sent ({current_tokens:,} tokens)")
        else:
            stats['imported'] += batch_size
            print(f"  ✓ Batch #{batch_number}: Sent {batch_size} chunks ({current_tokens:,} tokens)")
        
    except Exception as e:
        stats['failed'] += batch_size
        error_msg = str(e)
        print(f"  ✗ Batch #{batch_number} failed: {error_msg[:200]}")
        
        # Check for disk space error
        if "disk usage too high" in error_msg.lower() or "read-only" in error_msg.lower():
            print(f"\n{'='*70}")
            print("⚠️  CRITICAL ERROR: Weaviate disk space full!")
            print("The database is in read-only mode due to high disk usage.")
            print("\nSolutions:")
            print("1. Free up disk space on your system")
            print("2. Increase DISK_USE_READONLY_PERCENTAGE in docker-compose.yml")
            print("3. Move Weaviate data directory to a larger drive")
            print(f"{'='*70}\n")
            raise  # Re-raise to stop execution
        
        # Log all failed chunks in this batch
        for obj in batch_objects:
            append_failed_chunk({
                "error": error_msg,
                "properties": obj,
                "batch_number": batch_number
            }, failed_file)
            
def should_send_batch(current_tokens: int, chunk_tokens: int, max_tokens: int) -> bool:
    """Check if adding chunk would exceed batch token limit"""
    return current_tokens + chunk_tokens > max_tokens

def handle_oversized_chunk(properties: dict, chunk_tokens: int, collection, 
                          stats: dict, failed_file: Path):
    """Handle a chunk that exceeds max batch tokens by sending it alone"""
    print(f"  ⚠ Large chunk ({chunk_tokens:,} tokens) - sending alone...")
    stats['batches'] += 1
    send_batch_to_weaviate([properties], collection, stats['batches'], 
                          chunk_tokens, stats, failed_file)

# ============================================================================
# MAIN PROCESSING FUNCTIONS
# ============================================================================

def process_chunks_from_line(data: dict, document: str, collection, 
                            batch_state: dict, stats: dict, 
                            max_tokens: int, failed_file: Path):
    """Process all chunks from a single JSONL line
    
    Args:
        data: Parsed JSON data from line
        document: Document name
        collection: Weaviate collection
        batch_state: Dict with 'objects' and 'tokens' for current batch
        stats: Statistics dictionary
        max_tokens: Maximum tokens per batch
        failed_file: Path to failed chunks file
    """
    article, from_article = extract_parent_info(data)
    chunks = data.get('chunks', [])
    
    for chunk in chunks:
        properties = create_chunk_properties(chunk, article, from_article, document)
        chunk_tokens = approx_tokens(properties['content'])
        
        stats['processed'] += 1
        
        # Handle oversized chunks
        if chunk_tokens > max_tokens:
            # Send current batch first if not empty
            if batch_state['objects']:
                stats['batches'] += 1
                send_batch_to_weaviate(batch_state['objects'], collection, 
                                      stats['batches'], batch_state['tokens'], 
                                      stats, failed_file)
                batch_state['objects'] = []
                batch_state['tokens'] = 0
            
            handle_oversized_chunk(properties, chunk_tokens, collection, stats, failed_file)
            continue
        
        # Check if we need to send current batch
        if should_send_batch(batch_state['tokens'], chunk_tokens, max_tokens):
            stats['batches'] += 1
            send_batch_to_weaviate(batch_state['objects'], collection, 
                                  stats['batches'], batch_state['tokens'], 
                                  stats, failed_file)
            batch_state['objects'] = []
            batch_state['tokens'] = 0
        
        # Add chunk to current batch
        batch_state['objects'].append(properties)
        batch_state['tokens'] += chunk_tokens

def process_jsonl_file(jsonl_path: Path, collection, document: str, 
                      max_tokens: int, failed_file: Path, progress_interval: int = 200):
    """Process entire JSONL file line by line
    
    Args:
        jsonl_path: Path to JSONL file
        collection: Weaviate collection
        document: Document name
        max_tokens: Maximum tokens per batch
        failed_file: Path to failed chunks file
        progress_interval: Show progress every N chunks
        
    Returns:
        dict: Final statistics
    """
    stats = {
        'processed': 0,
        'imported': 0,
        'failed': 0,
        'batches': 0
    }
    
    batch_state = {
        'objects': [],
        'tokens': 0
    }
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                data = json.loads(line)
                process_chunks_from_line(data, document, collection, batch_state, 
                                        stats, max_tokens, failed_file)
                
                # Display progress
                if stats['processed'] % progress_interval == 0:
                    print(f"  → Processed {stats['processed']:,} chunks, "
                          f"{stats['batches']} batches sent...")
                
                # Clear for memory
                data = None
                
            except json.JSONDecodeError as e:
                print(f"  ✗ Error parsing line {line_num}: {str(e)[:100]}")
                continue
    
    # Send any remaining chunks in final batch
    if batch_state['objects']:
        stats['batches'] += 1
        send_batch_to_weaviate(batch_state['objects'], collection, 
                              stats['batches'], batch_state['tokens'], 
                              stats, failed_file)
    
    return stats

def print_summary(stats: dict, failed_file: Path):
    """Print final processing summary"""
    print(f"\n{'='*70}")
    print(f"Processing complete!")
    print(f"Total chunks processed: {stats['processed']:,}")
    print(f"Total batches sent: {stats['batches']}")
    print(f"Total chunks imported: {stats['imported']:,}")
    print(f"Total chunks failed: {stats['failed']}")
    if stats['failed'] > 0:
        print(f"Failed chunks saved to: {failed_file}")
    print(f"{'='*70}\n")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    setup_output_file(output_file)
    
    client = weaviate.connect_to_local()
    
    try:
        collection = client.collections.get(collection_name)
        
        print(f"\nProcessing file: {jsonl_path}")
        print(f"Max tokens per batch: {MAX_BATCH_TOKENS:,}")
        print("Starting dynamic batch import with streaming...\n")
        
        stats = process_jsonl_file(jsonl_path, collection, fileName, 
                                   MAX_BATCH_TOKENS, output_file)
        
        print_summary(stats, output_file)
        
    finally:
        client.close()

if __name__ == "__main__":
    main()