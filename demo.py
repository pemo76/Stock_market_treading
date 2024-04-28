import pandas as pd
import numpy as np
import talib

def calculate_RS(data):
    """
    Calculate RS (Relative Strength) using the given data.

    Parameters:
    - data: List of numeric values representing the price data.

    Returns:
    - RS value calculated based on the formula.
    """
    rocr_63 = talib.ROCR(np.array(data), timeperiod=63)[-1]
    rocr_126 = talib.ROCR(np.array(data), timeperiod=126)[-1]
    rocr_189 = talib.ROCR(np.array(data), timeperiod=189)[-1]
    rocr_252 = talib.ROCR(np.array(data), timeperiod=252)[-1]

    rs = (rocr_63 * 2 + rocr_126 + rocr_189 + rocr_252) / 5
    return rs

# Load the Bhavcopy data into a pandas DataFrame
bhavcopy_data = pd.read_csv("D:/Bhav Folder/BhavDB.csv")

# Get unique symbols
symbols = bhavcopy_data['SYMBOL'].unique()

# Dictionary to store RS values for each symbol
rs_values = {}

# Iterate through each symbol
for symbol in symbols:
    # Filter the DataFrame for the symbol
    symbol_data = bhavcopy_data[bhavcopy_data['SYMBOL'] == symbol]
    
    # Extract closing prices
    closing_prices = symbol_data['CLOSE'].tolist()
    
    # Calculate RS
    rs_value = calculate_RS(closing_prices)
    
    # Store RS value in dictionary
    rs_values[symbol] = rs_value

# Print RS values for all symbols
for symbol, rs_value in rs_values.items():
    print("RS value for", symbol, ":", rs_value)
