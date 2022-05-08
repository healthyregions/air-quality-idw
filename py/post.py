# %%
import pandas as pd 
from os import path

# %%
def main():
    temp_dir = path.join("temp", "interpolated_data.csv")
    data = pd.read_csv(temp_dir)[["geometry","topline_median"]]
    idw_min = data['topline_median'].min()
    idw_max = data['topline_median'].max()
    idw_range = idw_max - idw_min
    data['x'] = data['geometry'].apply(lambda x: float(x.split(',')[0][2:]))
    data['y'] = data['geometry'].apply(lambda x: float(x.split(',')[1][1:-1]))
    data['normalized_median'] = ( data['topline_median'] - idw_min ) / idw_range + 1    
    data['normalized_median'] = data['normalized_median'].round(2)
    data['topline_median'] = data['topline_median'].round(2)
    data[['topline_median','normalized_median', 'x','y']].to_csv(path.join("temp", "processed_data.csv"), index=False)

# %%
if __name__ == "__main__":
    main()
# %%
