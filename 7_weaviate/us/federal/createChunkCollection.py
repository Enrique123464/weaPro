import weaviate
from weaviate.classes.config import Configure, Property, DataType, ReferenceProperty

collection_name = "cfr_chunks"
reference_collection = "cfr_articles"

# Connect to Weaviate
client = weaviate.connect_to_local()

try:
    # Delete collection if it exists (for fresh start)
    if client.collections.exists(collection_name):
        client.collections.delete(collection_name)
    
    # Create collection with vector configuration
    client.collections.create(
        name=collection_name,
        properties=[
            Property(
                name="content", 
                data_type=DataType.TEXT
            ),
            Property(
                name="article",
                data_type=DataType.TEXT, 
            ),
            Property(
                name="document",
                data_type=DataType.TEXT, 
                skip_vectorization=True
            ),
            Property(
                name="start_offset",
                data_type=DataType.INT, 
                skip_vectorization=True
            )
        ],
        references=[
            ReferenceProperty(
                name="source_article",
                target_collection=reference_collection
            )
        ],
        vector_config=[
            Configure.Vectors.text2vec_ollama(
                name="article_content_vector",
                source_properties=["content", "article"],#source_properties=["content"],
                api_endpoint="http://host.docker.internal:11434",  # If using Docker, use this to contact your local Ollama instance
                model="qwen3-embedding:0.6b",  # The model to use, e.g. "nomic-embed-text"
                vectorize_collection_name=False,
            )
        ],
        reranker_config=Configure.Reranker.transformers()
    )
    
    print(f"Collection '{collection_name}' created successfully")

finally:
    client.close()