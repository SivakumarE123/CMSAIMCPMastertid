import os
from azure.cosmos import CosmosClient
from dotenv import load_dotenv

load_dotenv()

COSMOS_URL = os.getenv("COSMOS_URL")
COSMOS_KEY = os.getenv("COSMOS_KEY")
COSMOS_DB_NAME = os.getenv("COSMOS_DB_NAME")
COSMOS_CONTAINER_NAME = os.getenv("COSMOS_CONTAINER_NAME")

if not COSMOS_URL or not COSMOS_KEY:
    raise ValueError("Missing Cosmos credentials")

client = CosmosClient(COSMOS_URL, credential=COSMOS_KEY)

db = client.get_database_client(COSMOS_DB_NAME)
container = db.get_container_client(COSMOS_CONTAINER_NAME)


def get_user_permissions(email: str):
    try:
        query = "SELECT * FROM c WHERE c.email=@email"

        items = container.query_items(
            query=query,
            parameters=[{"name": "@email", "value": email}],
            enable_cross_partition_query=True
        )

        for item in items:
            return item

    except Exception as e:
        print("❌ Cosmos error:", str(e))
        return None

    return None


def upsert_user(email: str, products: list):
    """Create or update a user's product permissions."""
    try:
        existing = get_user_permissions(email)
        if existing:
            existing["products"] = products
            container.upsert_item(existing)
            return {"status": "updated", "email": email, "products": products}
        else:
            import uuid
            doc = {
                "id": str(uuid.uuid4()),
                "email": email,
                "products": products,
            }
            container.create_item(doc)
            return {"status": "created", "email": email, "products": products}
    except Exception as e:
        print("❌ Cosmos upsert error:", str(e))
        return {"status": "error", "error": str(e)}


def list_all_users():
    """List all users and their products."""
    try:
        items = container.query_items(
            query="SELECT c.email, c.products FROM c",
            enable_cross_partition_query=True
        )
        return [{"email": item["email"], "products": item.get("products", [])} for item in items]
    except Exception as e:
        print("❌ Cosmos list error:", str(e))
        return []


def get_all_products():
    """Get distinct product names across all users."""
    try:
        items = container.query_items(
            query="SELECT DISTINCT VALUE p FROM c JOIN p IN c.products",
            enable_cross_partition_query=True
        )
        return sorted(set(items))
    except Exception as e:
        print("❌ Cosmos products error:", str(e))
        return ["pii", "ocr", "transcription", "debug", "admin"]


def delete_user(email: str):
    """Remove a user from Cosmos."""
    try:
        existing = get_user_permissions(email)
        if not existing:
            return {"status": "not_found"}
        container.delete_item(item=existing["id"], partition_key=existing["email"])
        return {"status": "deleted", "email": email}
    except Exception as e:
        print("❌ Cosmos delete error:", str(e))
        return {"status": "error", "error": str(e)}
