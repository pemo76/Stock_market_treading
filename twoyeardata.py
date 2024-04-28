import os
import urllib.request
import zipfile
from datetime import datetime, timedelta
import pandas as pd
import calendar


def generate_bhavcopy_url(date_str):
    date_obj = datetime.strptime(date_str, '%d/%m/%Y')
    month_abbr = date_obj.strftime('%b').upper()[:3]  # Convert to uppercase and take the first 3 letters
    year = date_obj.strftime('%Y')
    day = date_obj.strftime('%d')
    url = f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{year}/{month_abbr}/cm{day}{month_abbr}{year}bhav.csv.zip"
    return url


def download_bhavcopy(date_str, save_folder):
    bhavcopy_url = generate_bhavcopy_url(date_str)
    zip_file_path = os.path.join(save_folder, f"bhavcopy.zip")

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://www.nseindia.com/api/chart-databyindex?index=OPTIDXBANKNIFTY25-01-2024CE46000.00',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
        'cookie': 'bm_sv=1A09781A6A86B0BAAEDE295093FFC9FC~YAAQ3Y0sMancktOOAQAACglf1xdg+fK1FUSYqL/GrtQAr6H1jZGZkQ3yTNxGuoGXkCkVxxA6KoWcLkLI2oxuXlBS8jvjcwnINncmtwps5D3OevaiW8bXXDOn4DwJ3fTlLwnzI6rHI/yWpQHV/HJCg83lAjWbz2mHhEp1urz0AcShETDzLJ0SVBqplEq7reh/PXFpqaaaoKN0R2/jULZw8ZiS+O8FTFoM9WDyTHS+rGAx3LAgYdNu1gw+tzsPSF8x9r9u~1; Domain=.nseindia.com; Path=/; Expires=Sat, 13 Apr 2024 14:10:57 GMT; Max-Age=7144; Secure'
    }

    req = urllib.request.Request(bhavcopy_url, headers=headers)
    try:
        # Download bhavcopy.zip
        print(f"Downloading Bhavcopy for {date_str} from URL: {bhavcopy_url}")
        with urllib.request.urlopen(req) as response, open(zip_file_path, 'wb') as out_file:
            data = response.read()  # a `bytes` object
            out_file.write(data)
        print("Download complete.")

        # Unzip the file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(save_folder)
        print("Bhavcopy downloaded and extracted successfully!")
        return True

    except Exception as e:
        print(f"Error downloading or extracting Bhavcopy for {date_str}: {str(e)}")
        return False


def process_bhavcopy_csv(date_str, save_folder):
    day, month, year = map(int, date_str.split('/'))

    # Getting the month abbreviation in capital letters
    month_abbr = calendar.month_abbr[month].upper()

    # Generating the filename dynamically based on the input date
    filename = f"cm{day:02d}{month_abbr}{year}bhav.csv"
    bhav_csv_path = os.path.join(save_folder, filename)

    if not os.path.isfile(bhav_csv_path):
        print(f"Error: File '{bhav_csv_path}' not found.")
        return

    df = pd.read_csv(bhav_csv_path)

    # Filter 'SERIES' column for 'EQ'
    df = df[df['SERIES'] == 'EQ']

    # Remove unnecessary column
    df.drop(['Unnamed: 13'], axis=1, inplace=True)

    # Save processed DataFrame to CSV
    output_csv_path = os.path.join(save_folder, "BhavDB.csv")

    # Check if the file exists
    file_exists = os.path.isfile(output_csv_path)

    # If file doesn't exist, write the header
    if not file_exists:
        df.to_csv(output_csv_path, index=False)
        print(f"Header written to BhavDB.csv")
        return

    # Check if data already exists in the file
    existing_data = pd.read_csv(output_csv_path)
    if not existing_data.empty:
        if 'Unnamed: 0' in existing_data.columns:
            existing_data.drop(['Unnamed: 0'], axis=1, inplace=True)  # Drop the index column for comparison
            if df.equals(existing_data):
                print(f"Data for {date_str} already exists in BhavDB.csv. Skipping...")
                return

    # Append data to the file
    df.to_csv(output_csv_path, mode='a', index=False, header=False)
    print(f"Processed CSV for {date_str} and appended to BhavDB.csv")


if __name__ == "__main__":
    # Define the save folder
    save_folder = "D:/Bhav Folder1"

    # Create the folder if it doesn't exist
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    # Get today's date
    today = datetime.today()

    # Calculate the end date (today)
    end_date = today

    # Calculate the start date (2 years ago)
    start_date = today - timedelta(days=365 * 2)

    # Loop over the date range
    current_date = start_date
    while current_date <= end_date:
        # Convert the date to string format
        date_input = current_date.strftime("%d/%m/%Y")

        # Print the processing date
        print("Processing date:", date_input)

        # Download Bhavcopy for the current date
        success = download_bhavcopy(date_input, save_folder)
        if success:
            # Process Bhavcopy for the current date if download is successful
            process_bhavcopy_csv(date_input, save_folder)

        # Move to the next date
        current_date += timedelta(days=1)
