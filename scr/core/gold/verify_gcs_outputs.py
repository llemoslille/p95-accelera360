import os
import sys
from google.cloud import storage
from google.oauth2 import service_account
import yaml


def load_credentials():
    # Localiza o config.yaml na árvore do projeto
    project_root = os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    config_path = os.path.join(project_root, 'scr', 'config', 'config.yaml')
    if not os.path.exists(config_path):
        print(f"❌ config.yaml não encontrado em {config_path}")
        sys.exit(1)
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    cred_path = config.get('credentials-path')
    if not cred_path or not os.path.exists(cred_path):
        print(f"❌ Credencial inválida em credentials-path: {cred_path}")
        sys.exit(1)
    credentials = service_account.Credentials.from_service_account_file(
        cred_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return credentials


def blob_exists(client: storage.Client, bucket_name: str, blob_name: str) -> bool:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.exists()


def main():
    credentials = load_credentials()
    client = storage.Client(credentials=credentials)
    bucket_name = 'p95-accelera360'

    checks = [
        (bucket_name, 'silver/leads-forms-accelera.parquet',
         'Silver: leads-forms-accelera.parquet'),
        (bucket_name, 'gold/dim_cliente.parquet', 'Gold: dim_cliente.parquet'),
        (bucket_name, 'gold/dim_vendedores.parquet', 'Gold: dim_vendedores.parquet'),
        (bucket_name, 'gold/dim_pipeline.parquet', 'Gold: dim_pipeline.parquet'),
        (bucket_name, 'gold/dim_estagio.parquet', 'Gold: dim_estagio.parquet'),
        (bucket_name, 'gold/fato_clint_digital.parquet',
         'Gold: fato_clint_digital.parquet'),
    ]

    all_ok = True
    for bkt, blob_name, label in checks:
        exists = blob_exists(client, bkt, blob_name)
        if exists:
            print(f"✅ {label} encontrado em gs://{bkt}/{blob_name}")
        else:
            print(f"❌ {label} NÃO encontrado em gs://{bkt}/{blob_name}")
            all_ok = False

    sys.exit(0 if all_ok else 1)


if __name__ == '__main__':
    main()
