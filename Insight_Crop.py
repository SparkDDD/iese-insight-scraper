import os
import json
import hashlib
import requests
from io import BytesIO
from PIL import Image
import gspread
from oauth2client.service_account import ServiceAccountCredentials

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

    cropped_img = img.crop((left, top, right, bottom)).convert("RGB")
    file_hash = hashlib.md5(image_url.encode('utf-8')).hexdigest()
    file_path = os.path.join(output_folder, f"{file_hash}.jpg")
    cropped_img.save(file_path, "JPEG")
    return file_path

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

def authorize_gspread_from_secret(json_key_env_var='JSON_KEY'):
    json_key = os.environ.get(json_key_env_var)
    if not json_key:
        raise ValueError(f"Environment variable '{json_key_env_var}' is not set.")
    key_dict = json.loads(json_key)
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scopes)
    return gspread.authorize(credentials)

def process_sheet_images(sheet_id, sheet_name, cloud_name, upload_preset):
    client = authorize_gspread_from_secret()
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)

    # Fetch all data to determine the number of rows and column indices efficiently
    all_data = sheet.get_all_values()
    if not all_data:
        print("Sheet is empty. No data to process.")
        return

    headers = all_data[0] # First row is headers

    # Ensure 'Cropped Image URL' column exists
    if 'Cropped Image URL' not in headers:
        # Update the header row in the sheet
        sheet.update_cell(1, len(headers) + 1, 'Cropped Image URL')
        headers.append('Cropped Image URL') # Update local headers list too

    try:
        image_col_index = headers.index('ImageFile URL')
        cropped_col_index = headers.index('Cropped Image URL')
    except ValueError as e:
        print(f"Error: Required column not found in headers. Please ensure 'ImageFile URL' and 'Cropped Image URL' exist. {e}")
        return

    # Iterate starting from the second row (index 1 in all_data list)
    # The 'row_num' variable represents the actual row number in Google Sheet (1-indexed)
    for row_num_list_index, row_data in enumerate(all_data):
        if row_num_list_index == 0: # Skip header row
            continue

        # Get actual sheet row number (1-indexed)
        actual_sheet_row_num = row_num_list_index + 1

        # Safely get values, handling cases where row_data might be shorter than expected
        image_url = row_data[image_col_index] if image_col_index < len(row_data) else ''
        cropped_url = row_data[cropped_col_index] if cropped_col_index < len(row_data) else ''

        # Check if image_url exists and cropped_url is empty
        if image_url and not cropped_url:
            print(f"Processing row {actual_sheet_row_num} (Image: {image_url})...")
            try:
                cropped_path = crop_to_2x1(image_url)
                public_url = upload_to_cloudinary(
                    file_path=cropped_path,
                    cloud_name=cloud_name,
                    upload_preset=upload_preset,
                    folder="Insight_Crop"
                )
                # Update the cell in the Google Sheet
                sheet.update_cell(actual_sheet_row_num, cropped_col_index + 1, public_url) # +1 for gspread's 1-indexed column
                os.remove(cropped_path) # Clean up local cropped file
                print(f"Successfully processed row {actual_sheet_row_num}: {public_url}")
            except Exception as e:
                # Log error in the sheet and to console
                error_message = f"Error: {str(e)}"
                sheet.update_cell(actual_sheet_row_num, cropped_col_index + 1, error_message)
                print(f"Failed to process row {actual_sheet_row_num}: {error_message}")
        elif image_url and cropped_url:
            print(f"Skipping row {actual_sheet_row_num}: Image already processed.")
        else:
            print(f"Skipping row {actual_sheet_row_num}: No original image URL found.")


if __name__ == "__main__":
    process_sheet_images(
        sheet_id='1HFN3fmDG927674xXzjtf6mMQEneCOQEkxaAfDGEQONU',
        sheet_name='IESE_Insight',
        cloud_name=os.environ['CLOUDINARY_CLOUD_NAME'],
        upload_preset=os.environ['CLOUDINARY_UPLOAD_PRESET']
    )
