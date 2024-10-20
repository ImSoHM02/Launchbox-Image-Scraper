import os
import requests
import re
import time
import threading
import xml.etree.ElementTree as ET
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from fuzzywuzzy import process
from requests.adapters import HTTPAdapter
from urllib.parse import quote
from urllib3.util.retry import Retry

def sanitize_filename(filename):
    if filename is None:
        return "Unknown"
    invalid_chars = r'[<>:"/\\|?*]'
    sanitized = re.sub(invalid_chars, '_', filename)
    sanitized = sanitized.strip('. ')
    return sanitized or "Unknown"

def parse_xml(file_path):
    print(f"Parsing XML file: {file_path}")
    start_time = time.time()
    tree = ET.parse(file_path)
    root = tree.getroot()
    parse_time = time.time() - start_time
    print(f"XML parsing completed in {parse_time:.2f} seconds.")
    return root

def get_game_info(root, database_id):
    for game in root.findall('.//Game'):
        if game.find('DatabaseID') is not None and game.find('DatabaseID').text == database_id:
            name = game.find('Name')
            platform = game.find('Platform')
            return {
                'name': sanitize_filename(name.text if name is not None else None),
                'platform': sanitize_filename(platform.text if platform is not None else None)
            }
    return None

def safe_find_text(element, tag, default="Unknown"):
    found = element.find(tag)
    return sanitize_filename(found.text if found is not None else default)

class FileExistenceCache:
    def __init__(self):
        self.cache = {}

    def file_exists(self, file_path):
        if file_path not in self.cache:
            self.cache[file_path] = os.path.exists(file_path)
        return self.cache[file_path]

def create_session_with_retries(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
    session = requests.Session()
    retry = Retry(total=retries,
                  read=retries,
                  connect=retries,
                  backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def download_image(image, game_info, output_dir, session, file_cache):
    platform = game_info['platform']
    game_name = game_info['name']
    region = safe_find_text(image, 'Region')
    file_name = safe_find_text(image, 'FileName')
    image_type = safe_find_text(image, 'Type')
    
    folder_path = os.path.join(output_dir, platform, game_name, region)
    os.makedirs(folder_path, exist_ok=True)
    
    url = f"https://images.launchbox-app.com/{quote(file_name)}"
    try:
        full_file_path = os.path.join(folder_path, f"{image_type}")
        
        if file_cache.file_exists(full_file_path):
            return f"Skipped (already exists): {full_file_path}"
        
        response = session.get(url)
        response.raise_for_status()
        
        content_type = response.headers.get('Content-Type', '')
        ext = '.jpg'  # Default to .jpg
        if 'png' in content_type:
            ext = '.png'
        elif 'gif' in content_type:
            ext = '.gif'
        
        full_file_path += ext
        
        with open(full_file_path, 'wb') as f:
            f.write(response.content)
        file_cache.cache[full_file_path] = True  # Update cache
        return f"Downloaded: {full_file_path}"
    except requests.exceptions.RequestException as e:
        return f"Failed to download after retries: {url}. Error: {str(e)}"

def worker_task(worker_id, image_queue, output_dir, session, file_cache, progress):
    results = []
    while True:
        try:
            image, game_info = image_queue.popleft()
            result = download_image(image, game_info, output_dir, session, file_cache)
            results.append(result)
            with progress['lock']:
                progress['completed'] += 1
        except IndexError:
            break  # Queue is empty
    return results

def print_progress(progress, total_images, start_time):
    while progress['completed'] < total_images:
        with progress['lock']:
            completed = progress['completed']
        elapsed_time = time.time() - start_time
        images_per_second = completed / elapsed_time if elapsed_time > 0 else 0
        print(f"\rProgress: {completed}/{total_images} images. "
              f"Speed: {images_per_second:.2f} images/second", end='', flush=True)
        time.sleep(0.5)  # Update every half second
    print()

def get_available_consoles(root):
    print("Scanning for available consoles...")
    start_time = time.time()
    consoles = set()
    for game in root.findall('.//Game'):
        platform = game.find('Platform')
        if platform is not None and platform.text:
            consoles.add(platform.text.strip())
    scan_time = time.time() - start_time
    console_list = sorted(list(consoles))
    print(f"Found {len(console_list)} consoles in {scan_time:.2f} seconds.")
    return console_list

def select_console(available_consoles):
    print("\nAvailable consoles:")
    for i, console in enumerate(available_consoles, 1):
        print(f"{i}. {console}")
    
    while True:
        try:
            choice = int(input("\nEnter the number of the console you want to process: "))
            if 1 <= choice <= len(available_consoles):
                return available_consoles[choice - 1]
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def fuzzy_search_games(games, query):
    game_names = [game['name'] for game in games]
    results = process.extract(query, game_names, limit=5)
    return [games[game_names.index(result[0])] for result in results]

def select_games(root, selected_console):
    print(f"\nGathering games for {selected_console}...")
    games = []
    for game in root.findall('.//Game'):
        platform = game.find('Platform')
        if platform is not None and platform.text and platform.text.strip() == selected_console:
            name = game.find('Name')
            database_id = game.find('DatabaseID')
            if name is not None and name.text and database_id is not None and database_id.text:
                games.append({
                    'name': name.text.strip(),
                    'database_id': database_id.text,
                    'platform': selected_console
                })
    
    print(f"Found {len(games)} games for {selected_console}")
    
    while True:
        choice = input("\nDo you want to (1) search for a specific game or (2) process all games? Enter 1 or 2: ")
        if choice == '1':
            query = input("Enter your search query: ")
            results = fuzzy_search_games(games, query)
            print("\nTop 5 matches:")
            for i, game in enumerate(results, 1):
                print(f"{i}. {game['name']}")
            while True:
                try:
                    selection = int(input("\nEnter the number of the game you want to process (or 0 to search again): "))
                    if 0 <= selection <= len(results):
                        if selection == 0:
                            break
                        return [results[selection - 1]]
                    else:
                        print("Invalid choice. Please try again.")
                except ValueError:
                    print("Invalid input. Please enter a number.")
        elif choice == '2':
            return games
        else:
            print("Invalid choice. Please enter 1 or 2.")

def process_game_images(root, output_dir, games, max_workers=10, max_retries=3):
    print(f"\nPreparing to process images for {len(games)} games...")
    
    image_queue = deque()
    for game in games:
        for image in root.findall(f".//GameImage[DatabaseID='{game['database_id']}']"):
            image_queue.append((image, {
                'platform': game['platform'],
                'name': sanitize_filename(game['name'])
            }))
    
    total_images = len(image_queue)
    print(f"Found {total_images} images to process.")
    
    if total_images == 0:
        print("No images to process. Exiting.")
        return
    
    print("\nStarting image download process...")
    start_time = time.time()
    session = create_session_with_retries(retries=max_retries)
    file_cache = FileExistenceCache()
    
    progress = {'completed': 0, 'lock': threading.Lock()}
    progress_thread = threading.Thread(target=print_progress, args=(progress, total_images, start_time))
    progress_thread.start()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker_task, i, image_queue, output_dir, session, file_cache, progress) for i in range(max_workers)]
        
        for future in as_completed(futures):
            results = future.result()
            for result in results:
                if not result.startswith("Skipped"):
                    print(f"\n{result}")

    progress_thread.join()
    
    total_time = time.time() - start_time
    print(f"\nCompleted processing {total_images} images in {total_time:.2f} seconds.")
    print(f"Average speed: {total_images / total_time:.2f} images/second")
    
def main():
    xml_file = 'Metadata.xml'
    output_dir = 'game_images'
    
    root = parse_xml(xml_file)
    available_consoles = get_available_consoles(root)
    selected_console = select_console(available_consoles)
    selected_games = select_games(root, selected_console)
    
    if not selected_games:
        print("No games selected. Exiting.")
        return
    
    process_game_images(root, output_dir, selected_games, max_workers=20, max_retries=3)

if __name__ == "__main__":
    main()