from opensearchpy import OpenSearch

client = OpenSearch("http://localhost:9200")

# Определение маппинга индекса
mapping = {
    "properties": {
        "title": {"type": "text"},
        "content": {"type": "text"},
        "content_type": {"type": "keyword"}
    }
}

# Создание индекса
client.indices.create(
    index="my_documents",
    body={"mappings": mapping}
)
