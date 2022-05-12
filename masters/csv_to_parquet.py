import sys
import pandas as pd
if __name__ == '__main__':
    try:
        df = pd.read_csv(sys.argv[1])
        df.to_parquet(sys.argv[1].replace('csv', 'parquet'), engine='fastparquet')
    except Exception as e:
        print(e)