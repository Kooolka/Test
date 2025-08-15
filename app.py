from opensearchpy import OpenSearch, helpers
from flask import Flask, request, jsonify

app = Flask(__name__)

OPENSEARCH_HOST = "opensearch"
OPENSEARCH_PORT = 9200
INDEX_NAME = "my_documents"
CONTENT_TYPES = ["news", "tutorial", "review", "report"]

client = OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
    http_compress=True,
    use_ssl=False
)

# Создание индекса


def create_index():
    mapping = {
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "content": {"type": "text"},
                "content_type": {"type": "keyword"}
            }
        }
    }

    if client.indices.exists(index=INDEX_NAME):
        client.indices.delete(index=INDEX_NAME)

    client.indices.create(index=INDEX_NAME, body=mapping)


def generate_documents():
    return [
        {
            "title": "Introduction to OpenSearch",
            "content": "OpenSearch is a powerful open-source search and analytics engine based on Apache Lucene.",
            "content_type": "tutorial"
        },
        {
            "title": "Latest Tech News",
            "content": "Major tech companies announce new partnerships in AI development during the summit.",
            "content_type": "news"
        },
        {
            "title": "Product Review: New Smartphone",
            "content": "The device offers excellent battery life but has average camera performance in low light.",
            "content_type": "review"
        },
        {
            "title": "Quarterly Financial Report",
            "content": "Company revenue grew by 15% year-over-year, exceeding analysts' expectations.",
            "content_type": "report"
        }
    ]

# Загрузка документов в индекс


def upload_documents():
    docs = generate_documents()
    actions = [{"_index": INDEX_NAME, "_source": doc} for doc in docs]
    helpers.bulk(client, actions)


def search_documents(query: str, content_type: str = None) -> list:
    search_body = {
        "query": {
            "bool": {
                "must": {
                    "multi_match": {
                        "query": query,
                        "fields": ["title", "content"]
                    }
                },
                "filter": []
            }
        },
        "_source": ["title", "content"]
    }

    if content_type:
        search_body["query"]["bool"]["filter"].append(
            {"term": {"content_type": content_type}}
        )

    response = client.search(index=INDEX_NAME, body=search_body)
    results = []

    for hit in response["hits"]["hits"]:
        content = hit["_source"]["content"]
        results.append({
            "title": hit["_source"]["title"],
            "snippet": f"{content[:50]}..." if len(content) > 50 else content
        })

    return results


@app.route('/')
def index():
    return "OpenSearch Demo App: Use /search?q=query&content_type=type"


@app.route('/search')
def search():
    query = request.args.get('q')
    content_type = request.args.get('content_type')

    if not query:
        return jsonify({"error": "Missing 'q' parameter"}), 400

    try:
        results = search_documents(query, content_type)
        return jsonify({
            "query": query,
            "content_type_filter": content_type,
            "results": results
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    print("Initializing OpenSearch...")
    create_index()
    upload_documents()
    print("Index created and documents uploaded")
    app.run(host='0.0.0.0', port=5000)
