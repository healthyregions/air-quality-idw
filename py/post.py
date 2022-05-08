# %%
import pandas as pd 
from os import path

# %%
# %%
def main():
    temp_dir = path.join("temp", "interpolated_data.csv")
    data = pd.read_csv(temp_dir)[["geometry","topline_median"]]
    data['x'] = data['geometry'].apply(lambda x: float(x.split(',')[0][2:]))
    data['y'] = data['geometry'].apply(lambda x: float(x.split(',')[1][1:-1]))
    data[['topline_median','x','y']].to_csv(path.join("temp", "processed_data.csv"), index=False)
    

# %%
if __name__ == "__main__":
    main()
# %%
