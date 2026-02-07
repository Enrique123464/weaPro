import weaviate
import json
from pathlib import Path

collection_name = "california_1"
MAX_BATCH_TOKENS = 136000

# Path and name to JSONL file
fileName = "calTitle1_regulations_content"
jsonl_path = Path("D:/data/Legalis/Prepro/USA/State Regulations/California/calTitle1_regulations_content.jsonl")

# Output file for failed chunks (JSONL)
output_file = Path("7_weaviate/failed_chunks.jsonl")
output_file.parent.mkdir(parents=True, exist_ok=True)
# Clear the output file if it exists (start fresh)
if output_file.exists():
    output_file.unlink()

def approx_tokens(text: str) -> int:
    """Approximate token count for text"""
    return max(1, len(text) // 4)

def append_failed_chunk(chunk_data):
    """Append a failed chunk to the JSONL file"""
    with open(output_file, 'a', encoding='utf-8') as f:
        json.dump(chunk_data, f, ensure_ascii=False)
        f.write('\n')

# Connect to Weaviate
client = weaviate.connect_to_local()

try:    
    # Get the collection
    collection = client.collections.get(collection_name)
    
    # Counters for statistics
    total_chunks_processed = 0
    total_chunks_imported = 0
    total_failed = 0
    total_batches = 0
    interval = 200  # print progress every this many records
    
    print(f"\nProcessing file: {jsonl_path}")
    print(f"Max tokens per batch: {MAX_BATCH_TOKENS:,}")
    print("Starting dynamic batch import with streaming...\n")
    
    # Batch accumulator
    current_batch = []
    current_batch_tokens = 0
    
    def send_batch(batch_objects):
        """Send a batch to Weaviate"""
        global total_batches, total_chunks_imported, total_failed
        
        if not batch_objects:
            return
        
        total_batches += 1
        batch_size = len(batch_objects)
        
        try:
            with collection.batch.dynamic() as batch:
                for obj in batch_objects:
                    batch.add_object(properties=obj)
            
            total_chunks_imported += batch_size
            print(f"  ✓ Batch #{total_batches}: Sent {batch_size} chunks ({current_batch_tokens:,} tokens)")
            
        except Exception as e:
            total_failed += batch_size
            print(f"  ✗ Batch #{total_batches} failed: {str(e)[:100]}")
            # Log all failed chunks in this batch
            for obj in batch_objects:
                append_failed_chunk({
                    "error": str(e),
                    "properties": obj,
                    "batch_number": total_batches
                })
    
    # Process JSONL line by line
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                # Parse the line
                data = json.loads(line)
                
                # Extract parent properties
                parent_keys = sorted([k for k in data.keys() if k.startswith('parent_')], 
                                    key=lambda x: int(x.split('_')[1]))
                
                # Get article (last parent)
                article = data.get(parent_keys[-1], "") if parent_keys else ""
                
                # Get fromArticle (all parents except parent_0)
                from_article = [data[k] for k in parent_keys[1:]] if len(parent_keys) > 1 else []
                
                # Process each chunk in this line
                chunks = data.get('chunks', [])
                for chunk in chunks:
                    content = chunk.get('content', '')
                    prior_content = chunk.get('priorContent', '')
                    content_and_prior = f"{prior_content} {content}".strip()
                    
                    # Calculate tokens for this chunk (based on content which gets vectorized)
                    chunk_tokens = approx_tokens(content)
                    
                    # Create the object to insert
                    properties = {
                        "content": content,
                        "priorContent": prior_content,
                        "contentAndPriorConcatenated": content_and_prior,
                        "article": article,
                        "fromArticle": from_article,
                        "document": fileName
                    }
                    
                    total_chunks_processed += 1
                    
                    # Check if single chunk exceeds max tokens
                    if chunk_tokens > MAX_BATCH_TOKENS:
                        # Send current batch first if not empty
                        if current_batch:
                            send_batch(current_batch)
                            current_batch = []
                            current_batch_tokens = 0
                        
                        # Handle oversized chunk alone
                        print(f"  ⚠ Large chunk ({chunk_tokens:,} tokens) - sending alone...")
                        send_batch([properties])
                        
                    # Check if adding this chunk would exceed batch limit
                    elif current_batch_tokens + chunk_tokens > MAX_BATCH_TOKENS:
                        # Send current batch
                        send_batch(current_batch)
                        
                        # Start new batch with this chunk
                        current_batch = [properties]
                        current_batch_tokens = chunk_tokens
                        
                    else:
                        # Add to current batch
                        current_batch.append(properties)
                        current_batch_tokens += chunk_tokens
                    
                    # Display progress
                    if total_chunks_processed % interval == 0:
                        print(f"  → Processed {total_chunks_processed:,} chunks, {total_batches} batches sent...")
                
                # Clear variables to free memory
                data = None
                chunks = None
                
            except json.JSONDecodeError as e:
                print(f"  ✗ Error parsing line {line_num}: {str(e)[:100]}")
                continue
    
    # Send any remaining chunks in the final batch
    if current_batch:
        send_batch(current_batch)
    
    print(f"\n{'='*70}")
    print(f"Processing complete!")
    print(f"Total chunks processed: {total_chunks_processed:,}")
    print(f"Total batches sent: {total_batches}")
    print(f"Total chunks imported: {total_chunks_imported:,}")
    print(f"Total chunks failed: {total_failed}")
    if total_failed > 0:
        print(f"Failed chunks saved to: {output_file}")
    print(f"{'='*70}\n")

finally:
    client.close()