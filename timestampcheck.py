import pandas as pd

# Read the CSV file
df = pd.read_csv("D:/Bhav Folder/test.csv")

# Parse timestamp column to datetime object
df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')

# Filter out rows where parsing failed
df = df.dropna(subset=['TIMESTAMP'])

# Filter data for last two years
current_year = pd.Timestamp.now().year
filtered_df = df[df['TIMESTAMP'].dt.year >= current_year - 1]

# Convert datetime objects back to original timestamp format ("%d-%b-%Y")
filtered_df['TIMESTAMP'] = filtered_df['TIMESTAMP'].dt.strftime('%d-%b-%Y')

# Write filtered data to a new CSV file
filtered_df.to_csv("D:/Bhav Folder/final.csv", index=False)
