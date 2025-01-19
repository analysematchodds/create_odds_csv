import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
from github import Github

# GitHub token'larını ortam değişkenlerinden al
SOURCE_REPO_TOKEN = os.environ.get('SOURCE_REPO_TOKEN')
TARGET_REPO_TOKEN = os.environ.get('TARGET_REPO_TOKEN')

# GitHub API'si ile bağlantı kur
source_github = Github(SOURCE_REPO_TOKEN)
target_github = Github(TARGET_REPO_TOKEN)

# Hedef repo bilgileri
TARGET_REPO_OWNER = 'analysematchodds'
TARGET_REPO_NAME = 'match_odds_csv'
TARGET_FILE_PATH = 'matchodds.csv'

def get_current_week():
    try:
        base_url = "https://www.spordb.com/view/iddaa_program_table.php"
        response = requests.get(base_url)
        response.raise_for_status()  # HTTP hatalarını kontrol eder
        soup = BeautifulSoup(response.content, 'html.parser')

        # Doğru select etiketini bul
        select_tag = soup.find('select', {'id': 'iddaa_daterange'})
        if not select_tag:
            raise ValueError("iddaa_daterange select etiketi bulunamadı.")

        # En üstteki option etiketini al
        top_option_tag = select_tag.find('option')
        if top_option_tag and top_option_tag.has_attr('value'):
            return int(top_option_tag['value'])  # En üst option'un value değerini döndür

    except Exception as e:
        print(f"Mevcut hafta kontrolünde hata: {str(e)}")

    return None

def read_existing_csv(file_path):
    try:
        target_repo = target_github.get_user(TARGET_REPO_OWNER).get_repo(TARGET_REPO_NAME)
        file = target_repo.get_contents(file_path)
        csv_content = file.decoded_content.decode('utf-8-sig')
        return pd.read_csv(pd.compat.StringIO(csv_content))
    except Exception as e:
        print(f"CSV dosyası okunamadı: {str(e)}")
        return pd.DataFrame()  # Eğer dosya yoksa boş bir DataFrame döndür

def get_iddaa_data(iddaa_hafta):
    try:
        base_url = "https://www.spordb.com/iddaa-programi/"
        session = requests.Session()
        response = session.get(base_url)

        ajax_url = "https://www.spordb.com/view/iddaa_program_table.php"
        params = {
            'iddaa_hafta': str(iddaa_hafta),
            'tarih': '*',
            'orderby': 'lig'
        }

        response = session.get(ajax_url, params=params)
        soup = BeautifulSoup(response.content, 'html.parser')
        superlig_header = soup.find('tr', {'class': 'tablemainheader'}, string=lambda x: x and 'Türkiye - Süper Lig' in x)

        if not superlig_header:
            print("Süper Lig maçları bulunamadı!")
            return None

        data = []
        current_row = superlig_header.find_next_sibling('tr')

        while current_row and not current_row.get('class', [''])[0] == 'tablemainheader':
            if current_row.get('filtervalue'):
                cells = current_row.find_all(['td'])
                if cells:
                    mbs_value = cells[3].get_text(strip=True)
                    lig = cells[2].get_text(strip=True)
                    if (mbs_value == '1') and (lig == 'TÜR S'):
                        row_data = {
                            'Tarih': cells[0].get_text(strip=True),
                            'Saat': cells[0].find('span').get_text(strip=True),
                            'Lig': lig,
                            'MBS': mbs_value,
                            'Ev Sahibi': cells[4].get_text(strip=True),
                            'Skor': cells[5].get_text(strip=True),
                            'Deplasman': cells[6].get_text(strip=True),
                            'MS1': cells[8].get_text(strip=True),
                            'MS0': cells[9].get_text(strip=True),
                            'MS2': cells[10].get_text(strip=True),
                        }
                        data.append(row_data)

            current_row = current_row.find_next_sibling('tr')

        if not data:
            print("İşlenebilir Süper Lig maçı bulunamadı!")
            return None

        df = pd.DataFrame(data)
        if 'Tarih' in df.columns:
            df['Tarih'] = pd.to_datetime(df['Tarih'], format='%d.%m.%Y', errors='coerce')

        if 'Saat' in df.columns:
            df['Saat'] = pd.to_datetime(df['Saat'], format='%H:%M', errors='coerce').dt.time

        return df.sort_values(by=['Tarih', 'Saat'], ascending=[False, False]).reset_index(drop=True)

    except Exception as e:
        print(f"Hata oluştu (Hafta {iddaa_hafta}): {str(e)}")
        return None

def update_csv_with_current_week(file_path, current_week):
    existing_df = read_existing_csv(file_path)
    current_week_df = get_iddaa_data(current_week)

    if current_week_df is not None and not current_week_df.empty:
        if not existing_df.empty and 'Hafta' in existing_df.columns:
            existing_df = existing_df[existing_df['Hafta'] != current_week]

        current_week_df['Hafta'] = current_week
        updated_df = pd.concat([existing_df, current_week_df], ignore_index=True)

        csv_content = updated_df.to_csv(index=False, encoding='utf-8-sig')
        update_file_in_target_repo(file_path, csv_content, f"Update for week {current_week}")
        print(f"{current_week}. hafta başarıyla eklendi/güncellendi.")
    else:
        print(f"Güncel haftaya ait veri bulunamadı.")

def update_file_in_target_repo(file_path, content, commit_message):
    target_repo = target_github.get_user(TARGET_REPO_OWNER).get_repo(TARGET_REPO_NAME)
    try:
        file = target_repo.get_contents(file_path)
        target_repo.update_file(file_path, commit_message, content, file.sha)
        print(f"Updated {file_path} successfully")
    except Exception as e:
        target_repo.create_file(file_path, commit_message, content)
        print(f"Created {file_path} successfully")

if __name__ == "__main__":
    start_time = datetime.now()

    current_week = get_current_week()
    if current_week is not None:
        print(f"Güncel hafta: {current_week}")
        update_csv_with_current_week(TARGET_FILE_PATH, current_week)
    else:
        print("Güncel hafta belirlenemedi!")

    end_time = datetime.now()
    print(f"\nScript çalışma süresi: {end_time - start_time}")
