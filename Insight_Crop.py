import os
import json
import hashlib
import requests
from io import BytesIO
from PIL import Image
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Step 1: Crop image to 2:1 ratio ---
def crop_to_2x1(image_url, output_folder='cropped_images'):
    os.makedirs(output_folder, exist_ok=True)
    response = requests.get(image_url)
    if response.status_code != 200:
        raise Exception(f"Failed to download image: {image_url}")
    img = Image.open(BytesIO(response.content))
    width, height = img.size
    target_width = width
    target_height = width // 2

    if height > target_height:
        top = (height - target_height) // 2
        bottom = top + target_height
        left = 0
        right = width
    else:
        target_width = height * 2
        left = (width - target_width) // 2
        right = left + target_width
        top = 0
        bottom = height

    cropped_img = img.crop((left, top, right, bottom))
    file_hash = hashlib.md5(image_url.encode('utf-8')).hexdigest()
    file_path = os.path.join(output_folder, f"{file_hash}.jpg")
    cropped_img.save(file_path, "JPEG")
    return file_path

# --- Step 2: Upload to Cloudinary into folder ---
def upload_to_cloudinary(file_path, cloud_name, upload_preset, folder="Insight_Crop"):
    url = f"https://api.cloudinary.com/v1_1/{cloud_name}/image/upload"
    with open(file_path, "rb") as file_data:
        response = requests.post(
            url,
            files={"file": file_data},
            data={
                "upload_preset": upload_preset,
                "folder": folder
            }
        )
    if response.status_code != 200:
        raise Exception(f"Upload failed: {response.text}")
    return response.json()["secure_url"]

# --- Step 3: Authorize Google Sheets with service key from GitHub secret ---
def authorize_gspread_from_secret(json_key_env_var='JSON_KEY'):
    json_key = os.environ.get(json_key_env_var)
    if not json_key:
        raise ValueError(f"Environment variable '{json_key_env_var}' is not set.")
    key_dict = json.loads(json_key)
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scopes)
    return gspread.authorize(credentials)

# --- Step 4: Process Sheet Rows ---
def process_sheet_images(sheet_id, sheet_name, cloud_name, upload_preset):
    client = authorize_gspread_from_secret()
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    records = sheet.get_all_records()

    headers = sheet.row_values(1)
    if 'Cropped Image URL' not in headers:
        sheet.update_cell(1, len(headers) + 1, 'Cropped Image URL')
        headers.append('Cropped Image URL')

    cropped_col_index = headers.index('Cropped Image URL') + 1

    for i, row in enumerate(records):
        image_url = row.get('ImageFile URL')
        cropped_url = row.get('Cropped Image URL')

        if image_url and not cropped_url:
            try:
                print(f"Processing row {i + 2}...")
                cropped_path = crop_to_2x1(image_url)
                public_url = upload_to_cloudinary(
                    file_path=cropped_path,
                    cloud_name=cloud_name,
                    upload_preset=upload_preset,
                    folder="Insight_Crop"
                )
                sheet.update_cell(i + 2, cropped_col_index, public_url)
                os.remove(cropped_path)
            except Exception as e:
                sheet.update_cell(i + 2, cropped_col_index, f"Error: {str(e)}")

# --- Step 5: Entry Point for GitHub Action ---
if __name__ == "__main__":
    process_sheet_images(
        sheet_id='1HFN3fmDG927674xXzjtf6mMQEneCOQEkxaAfDGEQONU',
        sheet_name='IESE_Insight',
        cloud_name=os.environ['CLOUDINARY_CLOUD_NAME'],
        upload_preset=os.environ['CLOUDINARY_UPLOAD_PRESET']
    )
