import logging
from googleapiclient.discovery import build
from google.auth import default

logging.basicConfig(level=logging.INFO)

INPUT_FOLDER_ID = "1tjO0DM0XOEnBwcfWyJiaNINUb5tbl7Ed"

def main():
    logging.info("Starting feb 9 2.43 Drive processing job")

    creds, _ = default()
    service = build("drive", "v3", credentials=creds)

    results = service.files().list(
        q=f"'{INPUT_FOLDER_ID}' in parents and trashed=false",
        fields="files(id, name)"
    ).execute()

    files = results.get("files", [])

    if not files:
        logging.info("No input files found")
    else:
        for f in files:
            logging.info(f"Found file: {f['name']} ({f['id']})")

    logging.info("Job completed")

if __name__ == "__main__":
    main()




