import weaviate
from weaviate.classes.config import Configure, Property, DataType

collection_name = "reg_ley_federalBatch"

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
                name="priorContent",
                data_type=DataType.TEXT, 
                skip_vectorization=True
            ),
            Property(
                name="contentAndPriorConcatenated",
                data_type=DataType.TEXT
            ),
            Property(
                name="article",
                data_type=DataType.TEXT, 
                skip_vectorization=True
            ),
            Property(
                name="fromArticle",
                data_type=DataType.TEXT_ARRAY,
                skip_vectorization=True
            ),
            Property(
                name="document",
                data_type=DataType.TEXT, 
                skip_vectorization=True
            ),
            Property(
                name="tipo",
                data_type=DataType.TEXT, 
                skip_vectorization=True
            )
        ],
        vector_config=[
            Configure.Vectors.text2vec_ollama(
                name="content_vector",
                source_properties=["content"],
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