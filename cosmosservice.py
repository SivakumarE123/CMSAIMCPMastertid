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
