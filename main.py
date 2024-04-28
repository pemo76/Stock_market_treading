import datetime
import os
import urllib.request
import zipfile
import csv


def generate_bhavcopy_url(date_str):
    date_obj = datetime.datetime.strptime(date_str, '%d/%m/%Y')
    month_abbr = date_obj.strftime('%b').upper()[:3]  # Convert to uppercase and take the first 3 letters
    year = date_obj.strftime('%Y')
    day = date_obj.strftime('%d')
    url = f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{year}/{month_abbr}/cm{day}{month_abbr}{year}bhav.csv.zip"
    return url


def filter_symbols_with_series_eq(csv_file):
    symbols = []
    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['SERIES'] == 'EQ':
                symbols.append(row['SYMBOL'])
    return symbols



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

        # Construct CSV file path
        date_obj = datetime.datetime.strptime(date_str, '%d/%m/%Y')
        csv_file_name = f"cm{date_obj.strftime('%d%b%Y').upper()}bhav.csv"
        csv_file_path = os.path.join(save_folder, csv_file_name)
        print("CSV File Path:", csv_file_path)

        # Filter symbols with series 'EQ'
        symbols = filter_symbols_with_series_eq(csv_file_path)
        # print("Symbols with series 'EQ':", symbols)
        SERIES_EQ_csv_file_name = f"cm{date_obj.strftime('%d%b%Y').upper()}filterbhav.csv"
        # Append the filtered symbols into BhavDB.csv
        with open(os.path.join(save_folder, SERIES_EQ_csv_file_name), 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for symbol in symbols:
                writer.writerow([symbol])

        print("Data appended to BhavDB.csv")

    except Exception as e:
        print(f"Error downloading or extracting Bhavcopy: {str(e)}")



if __name__ == "__main__":
    date_input = input("Enter the date in format DD/MM/YYYY: ")
    save_folder = "D:/Bhav Folder"

    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    download_bhavcopy(date_input, save_folder)
