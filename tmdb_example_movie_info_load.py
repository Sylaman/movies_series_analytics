import os
import json
import requests

def download_tmdb_show_example():
    # Настройки
    TMDB_API_KEY = 'c02557af1a4e1b010070469b41ca9ced'  # Замените на ваш актуальный ключ
    TMDB_SHOW_ID = 125988               # ID сериала Silo в TMDB

    # Создаем папку для примеров, если её нет
    output_dir = "./data/tmdb"
    os.makedirs(output_dir, exist_ok=True)

    url = f"https://api.themoviedb.org/3/tv/{TMDB_SHOW_ID}?api_key={TMDB_API_KEY}&language=en-US"

    response = requests.get(url)

    if response.status_code == 200:
        show_data = response.json()
        
        file_path = os.path.join(output_dir, f"show_{TMDB_SHOW_ID}_details.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(show_data, f, ensure_ascii=False, indent=4)
            
        print(f"Файл успешно сохранен по пути: {file_path}")
    else:
        print(f"Ошибка запроса к TMDB API: {response.status_code} - {response.text}")

if __name__ == "__main__":
    download_tmdb_show_example()