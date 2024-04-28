import pandas as pd
import yfinance

def calculate_RS(roc_63, roc_126, roc_189, roc_252):
    return (roc_63 * 2 + roc_126 + roc_189 + roc_252) / 5

def calculate_ema(data, window):
    ema_values = []
    if len(data) == 0:
        return ema_values  # Return empty list if data is empty
    
    smoothing_factor = 2 / (window + 1)
    ema_values.append(data.iloc[0])  # Use iloc to access first value safely
    for i in range(1, len(data)):
        ema = (data.iloc[i] - ema_values[-1]) * smoothing_factor + ema_values[-1]
        ema_values.append(ema)
    return ema_values

def generate_UPD_csv(watchlist_path, bhavdb_path, upd_path):
    # Read watchlist.csv without specifying column names
    watchlist_df = pd.read_csv(watchlist_path, header=None, names=['Symbol'])

    # Read BhavDB.csv
    bhavdb_df = pd.read_csv(bhavdb_path)

    # Check if required columns are present in BhavDB.csv
    required_columns = ['SYMBOL', 'TIMESTAMP', 'CLOSE']
    if not all(column in bhavdb_df.columns for column in required_columns):
        print("Required columns are missing in BhavDB.csv.")
        return

    # Initialize an empty DataFrame for UPD.csv
    upd_df = pd.DataFrame(columns=[
        'Date', 'Symbol', 'R.S.', 'Daily High', 'Daily Low', 'Weekly Close', 'One Week Ago Close', 
        'Weekly EMA(10)', 'Weekly EMA(40)', 'Daily SMA(50)', 'Daily SMA(200)', 'Daily SMA(250)', 
        'Daily EMA(10)', 'Daily EMA(30)', 'One Week Ago 52-week High', 'One Week Ago 52-week Low', 
        'One Day Ago 100-day High', 'One Day Ago 100-day Low', 'Weekly Upper Bollinger Band(20,2)', 
        'Weekly Lower Bollinger Band(20,1)'
    ])

    for symbol in watchlist_df['Symbol']:
        # Check if symbol exists in BhavDB.csv
        if symbol in bhavdb_df['SYMBOL'].values:
            # Extract data for the symbol from BhavDB.csv
            symbol_data = bhavdb_df[bhavdb_df['SYMBOL'] == symbol].copy()

            # Calculate Rate of Change (ROC)
            symbol_data['ROC_63'] = symbol_data['CLOSE'].pct_change(periods=63)
            symbol_data['ROC_126'] = symbol_data['CLOSE'].pct_change(periods=126)
            symbol_data['ROC_189'] = symbol_data['CLOSE'].pct_change(periods=189)
            symbol_data['ROC_252'] = symbol_data['CLOSE'].pct_change(periods=252)
            symbol_data['R.S.'] = calculate_RS(symbol_data['ROC_63'], symbol_data['ROC_126'], symbol_data['ROC_189'], symbol_data['ROC_252'])

            # Calculate other required metrics
            if not symbol_data.empty:
                symbol_data['Weekly EMA(10)'] = calculate_ema(symbol_data['CLOSE'], window=10)
                symbol_data['Weekly EMA(40)'] = calculate_ema(symbol_data['CLOSE'], window=40)
                symbol_data['Daily SMA(50)'] = symbol_data['CLOSE'].rolling(window=50).mean()
                symbol_data['Daily SMA(200)'] = symbol_data['CLOSE'].rolling(window=200).mean()
                symbol_data['Daily SMA(250)'] = symbol_data['CLOSE'].rolling(window=250).mean()
                symbol_data['Daily EMA(10)'] = calculate_ema(symbol_data['CLOSE'], window=10)
                symbol_data['Daily EMA(30)'] = calculate_ema(symbol_data['CLOSE'], window=30)
                symbol_data['One Week Ago 52-week High'] = symbol_data['CLOSE'].rolling(window=5*52).max().shift(7)
                symbol_data['One Week Ago 52-week Low'] = symbol_data['CLOSE'].rolling(window=5*52).min().shift(7)
                symbol_data['One Day Ago 100-day High'] = symbol_data['CLOSE'].rolling(window=100).max().shift(1)
                symbol_data['One Day Ago 100-day Low'] = symbol_data['CLOSE'].rolling(window=100).min().shift(1)
                symbol_data['Weekly Upper Bollinger Band(20,2)'] = symbol_data['CLOSE'].rolling(window=20).mean() + 2 * symbol_data['CLOSE'].rolling(window=20).std()
                symbol_data['Weekly Lower Bollinger Band(20,1)'] = symbol_data['CLOSE'].rolling(window=20).mean() - symbol_data['CLOSE'].rolling(window=20).std()

                # Add symbol column
                symbol_data['Symbol'] = symbol

                # Reorder columns according to specified sequence
                symbol_data = symbol_data[[
                    'TIMESTAMP', 'Symbol', 'R.S.', 'HIGH', 'LOW', 'CLOSE', 'LAST', 
                    'Weekly EMA(10)', 'Weekly EMA(40)', 'Daily SMA(50)', 'Daily SMA(200)', 'Daily SMA(250)', 
                    'Daily EMA(10)', 'Daily EMA(30)', 'One Week Ago 52-week High', 'One Week Ago 52-week Low', 
                    'One Day Ago 100-day High', 'One Day Ago 100-day Low', 'Weekly Upper Bollinger Band(20,2)', 
                    'Weekly Lower Bollinger Band(20,1)'
                ]]

                # Append the processed data to UPD.csv DataFrame
                upd_df = pd.concat([upd_df, symbol_data.dropna(how='all').reset_index(drop=True)], ignore_index=True, sort=False)
            else:
                print(f"No data available for symbol: {symbol}")

    # Sort UPD.csv DataFrame on R.S. descending
    upd_df = upd_df.sort_values(by='R.S.', ascending=False)

    # Write UPD.csv to the specified path
    upd_df.to_csv(upd_path, index=False)

if __name__ == "__main__":
    # Paths to watchlist.csv, BhavDB.csv, and UPD.csv
    watchlist_path = "D:/Bhav Folder/watchlist.csv"
    bhavdb_path = "D:/Bhav Folder/BhavDB.csv"
    upd_path = "D:/Bhav Folder/UPD.csv"

    # Generate UPD.csv
    generate_UPD_csv(watchlist_path, bhavdb_path, upd_path)
