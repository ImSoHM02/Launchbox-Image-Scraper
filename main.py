import os
import requests
import re
import time
import threading
import xml.etree.ElementTree as ET
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
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

def select_consoles(available_consoles):
    print("\nAvailable consoles:")
    for i, console in enumerate(available_consoles, 1):
        print(f"{i}. {console}")
    
    selected_indices = input("\nEnter the numbers of the consoles you want to process (comma-separated, or 'all'): ")
    
    if selected_indices.lower() == 'all':
        return available_consoles
    
    selected_consoles = []
    for index in selected_indices.split(','):
        try:
            i = int(index.strip()) - 1
            if 0 <= i < len(available_consoles):
                selected_consoles.append(available_consoles[i])
        except ValueError:
            pass
    
    return selected_consoles

def process_game_images(root, output_dir, selected_consoles, max_workers=10, max_retries=3):
    print("\nFiltering games for selected consoles...")
    start_time = time.time()
    
    selected_games = {}
    total_games = 0
    
    for game in root.findall('.//Game'):
        total_games += 1
        if total_games % 1000 == 0:
            print(f"Processed {total_games} games...")
        
        platform = game.find('Platform')
        if platform is not None and platform.text and platform.text.strip() in selected_consoles:
            database_id = game.find('DatabaseID')
            name = game.find('Name')
            if database_id is not None and database_id.text and name is not None and name.text:
                selected_games[database_id.text] = {
                    'platform': sanitize_filename(platform.text.strip()),
                    'name': sanitize_filename(name.text.strip())
                }
    
    filter_time = time.time() - start_time
    print(f"Filtered {len(selected_games)} games from {total_games} total games in {filter_time:.2f} seconds.")
    
    print("\nCollecting images for selected games...")
    start_time = time.time()
    image_queue = deque()
    total_images = 0
    
    for image in root.findall('.//GameImage'):
        total_images += 1
        if total_images % 1000 == 0:
            print(f"Processed {total_images} images...")
        
        database_id = safe_find_text(image, 'DatabaseID')
        if database_id in selected_games:
            image_queue.append((image, selected_games[database_id]))
    
    collection_time = time.time() - start_time
    print(f"Collected {len(image_queue)} images from {total_images} total images in {collection_time:.2f} seconds.")
    
    print("\nStarting image download process...")
    start_time = time.time()
    session = create_session_with_retries(retries=max_retries)
    file_cache = FileExistenceCache()
    
    progress = {'completed': 0, 'lock': threading.Lock()}
    progress_thread = threading.Thread(target=print_progress, args=(progress, len(image_queue), start_time))
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
    print(f"\nCompleted downloading {len(image_queue)} images in {total_time:.2f} seconds.")
    print(f"Average download speed: {len(image_queue) / total_time:.2f} images/second")
    
def main():
    xml_file = 'Metadata.xml'
    output_dir = 'game_images'
    
    root = parse_xml(xml_file)
    available_consoles = get_available_consoles(root)
    selected_consoles = select_consoles(available_consoles)
    
    if not selected_consoles:
        print("No consoles selected. Exiting.")
        return
    
    print(f"\nProcessing images for the following consoles: {', '.join(selected_consoles)}")
    process_game_images(root, output_dir, selected_consoles, max_workers=20, max_retries=3)

if __name__ == "__main__":
    main()