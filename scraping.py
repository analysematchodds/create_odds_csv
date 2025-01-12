import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
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
TARGET_REPO_NAME = 'sofa_csv'
TARGET_FILE_PATH = 'matchodds.csv'

def get_detail_value(detail_row, header_text, value_text):
    try:
        div = detail_row.find('div', string=lambda x: x and x.strip() == header_text.strip())
        if not div:
            div = detail_row.find('div', string=lambda x: x and header_text in x)
            
        if div:
            span = div.find_next('span', string=lambda x: x and x.strip() == value_text.strip())
            if not span:
                span = div.find_next('span', string=lambda x: x and value_text in x)
                
            if span:
                next_element = span.find_next('br')
                if next_element:
                    value = next_element.next_sibling
                    if value and isinstance(value, str):
                        cleaned_value = value.strip()
                        return '0' if cleaned_value == '-' else cleaned_value
                    elif hasattr(value, 'get_text'):
                        cleaned_value = value.get_text(strip=True)
                        return '0' if cleaned_value == '-' else cleaned_value
    except Exception:
        pass
    return '0'

def get_cell_value(cell):
    bet_span = cell.find('span', {'class': ['betwhite', 'betred']})
    return bet_span.get_text(strip=True) if bet_span else cell.get_text(strip=True)

def get_team_name(cell):
    mobile_span = cell.find('span', {'class': 'hide-on-desktop'})
    desktop_span = cell.find('span', {'class': 'hide-on-mobile'})
    return desktop_span.get_text(strip=True) if desktop_span else mobile_span.get_text(strip=True) if mobile_span else cell.get_text(strip=True)

def get_date_value(cell):
    date_span = cell.find('span', attrs={'date': True})
    if date_span and date_span.get('date'):
        try:
            dt = datetime.strptime(date_span.get('date'), '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%d.%m.%Y')
        except ValueError:
            pass
    icon = cell.find('i', {'class': 'fa-angle-double-right'})
    return icon.get('title') if icon and icon.get('title') else ''

def get_iddaa_data(iddaa_hafta):
    try:
        base_url = "https://www.spordb.com/iddaa-programi/"
        print("Ana sayfa yükleniyor...")
        
        session = requests.Session()
        response = session.get(base_url)
        print("Ana sayfa yanıt kodu:", response.status_code)
        
        ajax_url = "https://www.spordb.com/view/iddaa_program_table.php"
        params = {
            'iddaa_hafta': str(iddaa_hafta),
            'tarih': '*',
            'orderby': 'lig'
        }
        
        print(f"\n{iddaa_hafta} haftası verileri yükleniyor...")
        response = session.get(ajax_url, params=params)
        print(f"AJAX yanıt kodu: {response.status_code}")
        
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
                            'Tarih': get_date_value(cells[0]),
                            'Saat': cells[0].find('span').get_text(strip=True),
                            'Lig': lig,
                            'MBS': mbs_value,
                            'Ev Sahibi': get_team_name(cells[4]),
                            'Skor': cells[5].get_text(strip=True),
                            'Deplasman': get_team_name(cells[6]),
                            'İY': cells[7].get_text(strip=True),
                            'MS1': get_cell_value(cells[8]),
                            'MS0': get_cell_value(cells[9]),
                            'MS2': get_cell_value(cells[10]),
                            'AU2.5 Alt': get_cell_value(cells[11]),
                            'AU2.5 Üst': get_cell_value(cells[12]),
                            'KG Var': get_cell_value(cells[13]),
                            'KG Yok': get_cell_value(cells[14]),
                            'IY0.5 Alt': get_cell_value(cells[15]),
                            'IY0.5 Üst': get_cell_value(cells[16]),
                            'AU1.5 Alt': get_cell_value(cells[17]),
                            'AU1.5 Üst': get_cell_value(cells[18]),
                            'Çifte Şans 1-X': get_cell_value(cells[20]) if len(cells) > 20 else '',
                            'Çifte Şans 1-2': get_cell_value(cells[21]) if len(cells) > 21 else '',
                            'Çifte Şans X-2': get_cell_value(cells[22]) if len(cells) > 22 else '',
                        }
                        
                        detail_row = current_row.find_next_sibling('tr', {'class': 'detail'})
                        if detail_row:
                            detail_data = {
                                'IY Çifte Şans 1-X': get_detail_value(detail_row, 'İlk Yarı Çifte Şans', '1/X'),
                                'IY Çifte Şans 1-2': get_detail_value(detail_row, 'İlk Yarı Çifte Şans', '1/2'),
                                'IY Çifte Şans X-2': get_detail_value(detail_row, 'İlk Yarı Çifte Şans', '0/2'),
                                'IY1': get_detail_value(detail_row, 'İlk Yarı Sonucu', '1'),
                                'IY0': get_detail_value(detail_row, 'İlk Yarı Sonucu', '0'),
                                'IY2': get_detail_value(detail_row, 'İlk Yarı Sonucu', '2'),
                                '2Y1': get_detail_value(detail_row, 'İkinci Yarı Sonucu', '1'),
                                '2Y0': get_detail_value(detail_row, 'İkinci Yarı Sonucu', '0'),
                                '2Y2': get_detail_value(detail_row, 'İkinci Yarı Sonucu', '2'),
                                'Tek': get_detail_value(detail_row, 'Tek / Çift', 'Tek'),
                                'Çift': get_detail_value(detail_row, 'Tek / Çift', 'Çift'),
                                'IY/MS 1/1': get_detail_value(detail_row, 'İlk Yarı / Maç Sonucu', '1/1'),
                                'IY/MS 1/0': get_detail_value(detail_row, 'İlk Yarı / Maç Sonucu', '1/0'),
                                'IY/MS 1/2': get_detail_value(detail_row, 'İlk Yarı / Maç Sonucu', '1/2'),
                                'IY/MS 0/1': get_detail_value(detail_row, 'İlk Yarı / Maç Sonucu', '0/1'),
                                'IY/MS 0/0': get_detail_value(detail_row, 'İlk Yarı / Maç Sonucu', '0/0'),
                                'IY/MS 0/2': get_detail_value(detail_row, 'İlk Yarı / Maç Sonucu', '0/2'),
                                'IY/MS 2/1': get_detail_value(detail_row, 'İlk Yarı / Maç Sonucu', '2/1'),
                                'IY/MS 2/0': get_detail_value(detail_row, 'İlk Yarı / Maç Sonucu', '2/0'),
                                'IY/MS 2/2': get_detail_value(detail_row, 'İlk Yarı / Maç Sonucu', '2/2')
                            }
                            row_data.update(detail_data)
                        
                        data.append(row_data)
            
            current_row = current_row.find_next_sibling('tr')
            
        if not data:
            print("İşlenebilir Süper Lig maçı bulunamadı!")
            return None
            
        print(f"Bulunan Süper Lig maç sayısı: {len(data)}")
        
        df = pd.DataFrame(data)
        
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        return df
        
    except Exception as e:
        print(f"Hata oluştu (Hafta {iddaa_hafta}): {str(e)}")
        return None

def collect_historical_data(start_week=1832, end_week=1830):
    print("Geçmiş veriler toplanıyor...")
    all_data = []
    
    for hafta in range(start_week, end_week-1, -1):
        df = get_iddaa_data(hafta)
        
        if df is not None and not df.empty:
            df['Hafta'] = hafta
            all_data.append(df)
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        initial_rows = len(final_df)
        final_df = final_df.drop_duplicates(subset=['Saat', 'Ev Sahibi', 'Deplasman', 'MS1', 'MS0', 'MS2'])
        duplicate_rows = initial_rows - len(final_df)
        
        print(f"\nToplam {len(all_data)} hafta verisi toplandı")
        print(f"Toplam {len(final_df)} maç verisi bulundu")
        if duplicate_rows > 0:
            print(f"{duplicate_rows} duplike kayıt temizlendi")
        
        # DataFrame'i CSV formatına dönüştür
        csv_content = final_df.to_csv(index=False, encoding='utf-8-sig')
        
        # Hedef repo'ya dosyayı güncelle veya oluştur
        update_file_in_target_repo(TARGET_FILE_PATH, csv_content, "Update matchodds.csv")
        
        return final_df
    else:
        print("Hiç veri toplanamadı!")
        return None

def update_file_in_target_repo(file_path, content, commit_message):
    target_repo = target_github.get_user(TARGET_REPO_OWNER).get_repo(TARGET_REPO_NAME)
    try:
        # Mevcut dosyayı al
        file = target_repo.get_contents(file_path)
        # Dosyayı güncelle
        target_repo.update_file(file_path, commit_message, content, file.sha)
        print(f"Updated {file_path} successfully")
    except Exception as e:
        # Dosya yoksa yeni dosya oluştur
        target_repo.create_file(file_path, commit_message, content)
        print(f"Created {file_path} successfully")

if __name__ == "__main__":
    start_time = time.time()
    df = collect_historical_data()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nScript çalışma süresi: {execution_time:.2f} saniye")
