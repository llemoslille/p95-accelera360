import os
from dotenv import load_dotenv

load_dotenv()

clint_url = os.getenv("CLINT_URL")
clint_user = os.getenv("CLINT_USER")
clint_password = os.getenv("CLINT_PASSWORD")

# URLs para coleta de dados
clint_urls = [
    "https://app.clint.digital/origin/1bb864dd-9fdf-498c-aaee-256776337fe8",
]

pbi_tenant_id = os.getenv('PBI_TENANT_ID')
pbi_client_id = os.getenv('PBI_CLIENT_ID')
pbi_client_secret = os.getenv('PBI_CLIENT_SECRET')
pbi_authority = os.getenv('PBI_AUTHORITY')
pbi_scope = os.getenv('PBI_SCOPE')
pbi_group_id = os.getenv('PBI_GROUP_ID')
pbi_dataset_id = os.getenv('PBI_DATASET_ID')