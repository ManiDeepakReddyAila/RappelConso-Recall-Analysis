import time
import requests
import json
import re
from google.cloud import translate_v3 as translate
from google.oauth2 import service_account
from google.cloud.pubsublite.cloudpubsub import PublisherClient
from google.cloud.pubsublite.types import (
    CloudRegion,
    TopicPath,
)

# Configuration
PROJECT_ID = "rappelconso-434222"
OFFSET_FILE = "offset.json"
LOCATION = "us-central1"
TOPIC_NAME = "rappelconso-lite-topic"
LIMIT = 10

credentials = service_account.Credentials.from_service_account_file(
    'path to json'
)

location = CloudRegion(LOCATION)
topic_path = TopicPath(PROJECT_ID, location, TOPIC_NAME)
client = translate.TranslationServiceClient(credentials=credentials)

def get_offset():
    try:
        with open(OFFSET_FILE) as file:
            return json.load(file).get('offset', 0)
    except FileNotFoundError:
        return 0

def update_offset(offset):
    with open(OFFSET_FILE, 'w') as file:
        json.dump({"offset": offset}, file)

def fetch_and_publish():
    while True:
        url = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/rappelconso0/records"
        offset = get_offset()
        params = {
            'limit': LIMIT,
            'offset': offset
        }

        response = requests.get(url, params=params)
        records = response.json().get('results', [])

        if not records:
            print("No new records available. Retrying...")
        else:
            with PublisherClient(credentials=credentials) as publisher_client:
                for record in records:
                    transformed_record = transform_row(record)
                    print(transformed_record)
                    data = json.dumps(transformed_record).encode("utf-8")
                    try:
                        api_future = publisher_client.publish(topic_path, data)
                        message_id = api_future.result()
                        print(f"Published a message to {topic_path} with ID: {message_id}")
                    except Exception as e:
                        print(f"Failed to publish message: {e}")

                update_offset(offset + LIMIT)

        time.sleep(60)

def clean_text(text):
    """Clean and normalize text data by removing excessive whitespace and newline characters."""
    if text:
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    return None

def translate_text(text):
    """Translate text from French to English using Google Cloud Translation API v3."""
    if text:
        try:
            parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
            response = client.translate_text(
                parent=parent,
                contents=[text],
                mime_type="text/plain",
                source_language_code="fr",
                target_language_code="en"
            )
            return response.translations[0].translated_text
        except Exception as e:
            print(f"Error during translation: {e}")
    return None

def merge_columns(col1, col2):
    """Merge two columns, handling None values gracefully."""
    if col1 and col2:
        return f"{col1}. {col2}"
    return col1 or col2

def transform_row(record):
    """Transforms a single record by cleaning, merging, and translating fields."""
    transformed_record = {
        "reference_sheet": clean_text(record.get("reference_fiche")),
        "version": record.get("ndeg_de_version"),
        "legal_nature": translate_text(clean_text(record.get("nature_juridique_du_rappel"))),
        "product_category": translate_text(clean_text(record.get("categorie_de_produit"))),
        "sub_category": translate_text(clean_text(record.get("sous_categorie_de_produit"))),
        "brand_name": clean_text(record.get("nom_de_la_marque_du_produit")),
        "product_models": clean_text(record.get("noms_des_modeles_ou_references")),
        "risk_description": merge_columns(
            translate_text(clean_text(record.get("risques_encourus_par_le_consommateur"))),
            translate_text(clean_text(record.get("description_complementaire_du_risque")))
        ),
        "consumer_recommendations": merge_columns(
            translate_text(clean_text(record.get("preconisations_sanitaires"))),
            translate_text(clean_text(record.get("conduites_a_tenir_par_le_consommateur")))
        ),
        "compensation_methods": translate_text(clean_text(record.get("modalites_de_compensation"))),
        "additional_information": translate_text(clean_text(record.get("informations_complementaires_publiques"))),
        "date_of_publication": record.get("date_de_publication"),
        "distributors": clean_text(record.get("distributeurs")),
        "image_url": record.get("liens_vers_les_images"),
    }
    return transformed_record

if __name__ == "__main__":
    fetch_and_publish()
