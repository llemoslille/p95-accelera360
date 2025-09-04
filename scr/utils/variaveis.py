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
