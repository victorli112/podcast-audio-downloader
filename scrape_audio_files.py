from selenium import webdriver
from joblib import Parallel, delayed
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import soundfile
import psutil
import whisper
import chromedriver_binary
from selenium.webdriver.chrome.options import Options

CHUNK_SIZE = 1000
BATCH_SIZE = 50
SUPPORTED_FILE_TYPES = ['.mp3', '.m4a', '.mp4']
DOWNLOAD_DIRECTORY = './episode_audio/'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

chrome_options = Options()
chrome_options.add_argument("--headless=new")

class scraper: 
    def __init__(self):
        self.SKIPPED = 0
        self.DOWNLOADED = 0
    
    def run(self, links_to_download):
        # set baseline for parallel processes
        current_process = psutil.Process()
        subproc_before = set([p.pid for p in current_process.children(recursive=True)])
        
        self.download_audio_files(links_to_download)
        
        # kill parallel processes
        subproc_after = set([p.pid for p in current_process.children(recursive=True)])
        for subproc in subproc_after - subproc_before:
            psutil.Process(subproc).terminate() 
            
    def download_audio_files(self, links_to_download):
        # Create directory
        if not os.path.exists(DOWNLOAD_DIRECTORY):
            os.makedirs(DOWNLOAD_DIRECTORY)
        
        for chunk in pd.read_csv(links_to_download, chunksize=CHUNK_SIZE, encoding='unicode_escape'):
            urls = chunk.iloc[:, 0]
            indexes = chunk.iloc[:, 1]
            
            # Create a map of index to url
            url_map = dict(zip(indexes, urls))

            # execute
            Parallel(n_jobs=BATCH_SIZE)(delayed(self.get_audio_file)(url_map[index],index) for index in url_map.keys())
            
            print("[METRICS] Skipped: ", self.SKIPPED)
            print("[METRICS] Downloaded: ", self.DOWNLOADED)
        
    def get_audio_file(self, url, index):
        driver = webdriver.Chrome(options=chrome_options)
        
        try:
            driver.get(url)
        except Exception:
            print("Failed to get url: ", url)
            driver.close()
            return
        
        time.sleep(5)
        html = driver.page_source
        
        driver.close()
        soup = BeautifulSoup(html, 'lxml')
        links = soup.find_all('a', href=True)
        filtered_links = [link for link in links if any(ext in link['href'] for ext in SUPPORTED_FILE_TYPES)]
        audio_links = [link['href'] for link in filtered_links]
        
        if len(audio_links) > 1:
            pass
            #print(f"More than one audio links found for index {index}, proceeding with first one")
        elif len(audio_links) == 0:
            print("No audio links found", index, url)
            self.SKIPPED += 1
            return
        
        try:
            audio_file = requests.get(audio_links[0], headers=HEADERS)
        except: 
            print("Failed to get audio file: ", audio_links[0])
            return
        
        file_name = f'EPISODEAUDIO_{index}'
        # download file_name
        with open(f'{file_name}.mp3', 'wb') as f:
            f.write(audio_file.content)
            f.close()
        
        # Try to transcribe audio URL to a text file    
        try:                       
            # CHANGE here for accuracy in exchange for speed
            # tiny.en > base.en > small.en roughly tiny.en is supposed to be 2x faster than base.en
            # but in my own test, base.en was slightly faster than tiny.en
            # https://github.com/openai/whisper
            model = whisper.load_model("base.en")
            print("Transcribing", file_name)
            result = model.transcribe(f'{file_name}.mp3', fp16=False)
            with open(f'{DOWNLOAD_DIRECTORY}{file_name}.txt', 'wb') as f:
                f.write(result['text'])
        except: 
            print("Failed to transcribe: ", file_name)
            
        
        # delete mp3 files
        os.remove(f'{file_name}.mp3')
        
        self.DOWNLOADED += 1
    
scraper = scraper()
scraper.run('episode_link_list.csv')