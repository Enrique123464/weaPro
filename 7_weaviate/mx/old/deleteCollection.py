import weaviate

client = weaviate.connect_to_local()

client.collections.delete("sadfsdfsd")

client.close()
