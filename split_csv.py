import pandas as pd

def split_large_csv(file_path, output_path, num_files):
    df = pd.read_csv(file_path)
    chunk_size = len(df) // num_files
    for i in range(num_files):
        df[i*chunk_size:(i+1)*chunk_size].to_csv(f'{output_path}/output_{i+1}.csv', index=False)

split_large_csv('JRR_Products_06_19_23.csv', '/Users/zola/Downloads/shopifycsv', 10)