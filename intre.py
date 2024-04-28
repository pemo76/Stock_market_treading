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

    except Exception as e:
        print(f"Error downloading or extracting Bhavcopy: {str(e)}")


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
    print(bhav_csv_path)

    df = pd.read_csv(bhav_csv_path)


    # Filter 'SERIES' column for 'EQ'
    df = df[df['SERIES'] == 'EQ']

    # Remove unnecessary column
    df.drop(['Unnamed: 13'], axis=1, inplace=True)

    # Save processed DataFrame to CSV
    output_csv_path = os.path.join(save_folder, "BhavDB.csv")
    #df.to_csv(output_csv_path, index=False)
    df.to_csv(output_csv_path, mode='a', index=False, header=False)
    print("Processed CSV and appended to BhavDB.csv")


def filter_dataframe():
    # Assuming 'TIMESTAMP' is the name of your date column
    df = pd.read_csv('D:/Bhav Folder/BhavDB.csv')
    df.drop_duplicates(subset=['SYMBOL', 'TIMESTAMP'], inplace=True, keep='first')
    df.to_csv('D:/Bhav Folder/BhavDB.csv', index=False)
    print('Success')


def filter_date():
    df = pd.read_csv('D:/Bhav Folder/BhavDB.csv')

    # Define a custom function to parse dates with multiple formats
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    current_date = datetime.now()
    start_date = current_date - timedelta(days=365 * 2)
    df = df[df['TIMESTAMP'] >= start_date]

    # Convert back to the original timestamp format "%d-%b-%Y"
    df['TIMESTAMP'] = df['TIMESTAMP'].dt.strftime('%d-%b-%Y')

    df.to_csv('D:/Bhav Folder/BhavDB.csv', mode='w', index=False, header=True)
    print('Data filtered for the last two years.')


if __name__ == "__main__":
    date_input = input("Enter the date in format DD/MM/YYYY: ")
    save_folder = "D:/Bhav Folder"

    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    download_bhavcopy(date_input, save_folder)
    process_bhavcopy_csv(date_input, save_folder)

    filter_dataframe()
    filter_date()


#897184