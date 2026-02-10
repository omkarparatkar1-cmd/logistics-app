import io
import re
import logging
from datetime import datetime, date

from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import cv2
import pytesseract
import numpy as np
from PIL import Image
import spacy

# ======================
# CONFIGURATION
# ======================

INPUT_FOLDER_ID = "1tjO0DM0XOEnBwcfWyJiaNINUb5tbl7Ed"
ARCHIVE_FOLDER_ID = "1ic5paKlxZZRy4zAtMcjBMU9TRoilom6s"
OUTPUT_FOLDER_ID = "1KpoGkgxWIWXNva0o7tkxvFhRQyrPHoSq"

logging.basicConfig(level=logging.INFO)
nlp = spacy.load("en_core_web_sm")

# ======================
# AUTHENTICATION
# ======================

creds, _ = default()
drive = build("drive", "v3", credentials=creds)
sheets = build("sheets", "v4", credentials=creds)

# ======================
# OCR + PARSER LOGIC
# (minimally adapted from your file)
# ======================

STATE_ZIP = re.compile(r'\b[A-Z]{2}\s+\d{5}(-\d{4})?\b')
STREET = re.compile(r'\d+\s+[A-Z]')

TRACKING_PATTERNS = {
    "USPS": r'\b9\d{21,22}\b',
    "UPS": r'\b1Z[A-Z0-9]{16}\b',
    "FEDEX": r'\b\d{12,15}\b'
}


def detect_carrier(lines):
    joined = " ".join(lines)
    if "USPS" in joined:
        return "USPS"
    if "1Z" in joined or "UPS" in joined:
        return "UPS"
    if "FEDEX" in joined:
        return "FEDEX"
    return "UNKNOWN"


def is_person(line):
    return any(ent.label_ == "PERSON" for ent in nlp(line).ents)


def is_address_line(line):
    return bool(STREET.search(line) or STATE_ZIP.search(line))


def parse_image(image_bytes):
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)

    img = cv2.resize(img, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    binary = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY, 31, 2
    )

    data = pytesseract.image_to_data(binary, output_type=pytesseract.Output.DICT)

    lines, current, last_y = [], [], None
    for i, txt in enumerate(data["text"]):
        txt = txt.strip()
        if not txt or int(data["conf"][i]) < 60:
            continue
        y = data["top"][i]
        if last_y is None or abs(y - last_y) < 15:
            current.append(txt)
        else:
            lines.append(" ".join(current))
            current = [txt]
        last_y = y

    if current:
        lines.append(" ".join(current))

    lines = [re.sub(r'[^A-Z0-9\s#.,-]', '', l.upper()).strip() for l in lines]

    carrier = detect_carrier(lines)
    tracking_id = ""

    pattern = TRACKING_PATTERNS.get(carrier)
    if pattern:
        for l in lines:
            m = re.search(pattern, l.replace(" ", ""))
            if m:
                tracking_id = m.group()
                break

    lines = [l for l in lines if tracking_id not in l.replace(" ", "")]

    sender, receiver, mode = [], [], "sender"

    for l in lines:
        if "SHIP TO" in l or "TO:" in l:
            mode = "receiver"
            continue
        if is_person(l) and mode == "sender":
            mode = "receiver"
            receiver.append(l)
            continue
        if is_address_line(l) or is_person(l):
            (sender if mode == "sender" else receiver).append(l)

    sender_add = "|".join(dict.fromkeys(sender))
    receiver_add = "|".join(dict.fromkeys(receiver))

    return sender_add, receiver_add, tracking_id

# ======================
# GOOGLE SHEETS HELPERS
# ======================

def get_or_create_daily_sheet():
    sheet_name = f"Parsed_Labels_{date.today().isoformat()}"

    q = f"name='{sheet_name}' and '{OUTPUT_FOLDER_ID}' in parents"
    files = drive.files().list(q=q, fields="files(id)").execute()["files"]

    if files:
        return files[0]["id"]

    spreadsheet = sheets.spreadsheets().create(
        body={"properties": {"title": sheet_name}},
        fields="spreadsheetId"
    ).execute()

    sheet_id = spreadsheet["spreadsheetId"]

    drive.files().update(
        fileId=sheet_id,
        addParents=OUTPUT_FOLDER_ID,
        removeParents="root",
        fields="id"
    ).execute()

    return sheet_id


def append_rows(sheet_id, rows):
    sheets.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range="A1",
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()

# ======================
# MAIN JOB
# ======================

def main():
    logging.info("Job started")

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sheet_id = get_or_create_daily_sheet()

    append_rows(sheet_id, [[f"Run at {run_ts}"], []])

    results = drive.files().list(
        q=f"'{INPUT_FOLDER_ID}' in parents and trashed=false",
        fields="files(id,name,mimeType)"
    ).execute()["files"]

    rows = [["sender_address", "receiver_address", "tracking_id"]]

    for f in results:
        if not f["mimeType"].startswith("image/"):
            continue

        logging.info(f"Processing {f['name']}")

        request = drive.files().get_media(fileId=f["id"])
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, request).next_chunk()
        fh.seek(0)

        sender, receiver, tracking = parse_image(fh.read())
        rows.append([sender, receiver, tracking])

        drive.files().update(
            fileId=f["id"],
            addParents=ARCHIVE_FOLDER_ID,
            removeParents=INPUT_FOLDER_ID,
            fields="id, parents"
        ).execute()

    append_rows(sheet_id, rows)
    append_rows(sheet_id, [[]])

    logging.info("Job completed successfully")


if __name__ == "__main__":
    main()
