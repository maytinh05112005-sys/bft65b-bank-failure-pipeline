import os
import sys
import zipfile
import requests

JDK_URL = "https://api.adoptium.net/v3/binary/latest/17/ga/windows/x64/jdk/hotspot/normal/eclipse?project=jdk"
TARGET_DIR = r"C:\Users\BAN\tools\jdk17"
ZIP_FILE = r"C:\Users\BAN\tools\jdk17.zip"

os.makedirs(os.path.dirname(ZIP_FILE), exist_ok=True)

if not os.path.exists(TARGET_DIR):
    print("Downloading OpenJDK 17 portable zip (~180MB)...")
    with requests.get(JDK_URL, stream=True, timeout=60) as r:
        r.raise_for_status()
        total_len = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(ZIP_FILE, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024 * 2):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_len:
                        print(f"Downloaded {downloaded / (1024*1024):.1f}/{total_len / (1024*1024):.1f} MB ({(downloaded/total_len)*100:.1f}%)")
    print("Extracting OpenJDK 17...")
    with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
        zip_ref.extractall(r"C:\Users\BAN\tools")
    # find the extracted directory name
    dirs = [d for d in os.listdir(r"C:\Users\BAN\tools") if d.startswith("jdk-17")]
    if dirs:
        extracted = os.path.join(r"C:\Users\BAN\tools", dirs[0])
        if os.path.exists(TARGET_DIR):
            os.remove(TARGET_DIR)
        os.rename(extracted, TARGET_DIR)
    if os.path.exists(ZIP_FILE):
        os.remove(ZIP_FILE)
    print("OpenJDK 17 setup complete at", TARGET_DIR)
else:
    print("OpenJDK 17 already exists at", TARGET_DIR)
