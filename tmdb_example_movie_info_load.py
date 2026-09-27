import os
import json
import requests

# Настройки
TMDB_API_KEY = ''  # Ваш ключ от TMDB
TMDB_SHOW_ID = 1399  # ID сериала в TMDB (например, Game of Thrones = 1399)
SEASON_NUMBER = 1  # Номер сезона

# Создаем папку для примеров, если её нет
output_dir = "./data/tmdb"
os.makedirs(output_dir, exist_ok=True)

url = f"https://api.themoviedb.org/3/tv/{TMDB_SHOW_ID}/season/{SEASON_NUMBER}?api_key={TMDB_API_KEY}&language=en-US"

response = requests.get(url)

if response.status_code == 200:
    season_data = response.json()
    
    file_path = os.path.join(output_dir, f"show_{TMDB_SHOW_ID}_season_{SEASON_NUMBER}_details.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(season_data, f, ensure_ascii=False, indent=4)
        
    print(f"Файл успешно сохранен по пути: {file_path}")
else:
    print(f"Ошибка запроса к TMDB API: {response.status_code} - {response.text}")