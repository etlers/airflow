import pandas as pd
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--df", type=pd.DataFrame, required=True)
    args = parser.parse_args()

    print(args.df.describes)