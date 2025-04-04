import pandas as pd
import folium

# Load the data
stores_df = pd.read_csv('retail_stores.csv')

# Initialize a map centered around the average location of the stores
map_center = [stores_df['latitude'].mean(), stores_df['longitude'].mean()]
m = folium.Map(location=map_center, zoom_start=5)

# Add markers for each store location
for idx, row in stores_df.iterrows():
    folium.Marker([row['latitude'], row['longitude']], popup=f"Store ID: {row['store_id']}").add_to(m)

# Save the map to an HTML file
map_file_path = 'stores_map.html'
m.save(map_file_path)

map_file_path
