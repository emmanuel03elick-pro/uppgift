import pandas as pd
import sqlite3

df = pd.read_csv('resenorden_data.csv')


# clean_dates
def clean_dates(df):
    df_clean = df
    date_cols = ['bokningsdatum', 'avresedatum', 'hemresedatum']
    for col in date_cols:
        df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce', dayfirst=True)

    return df_clean

# clean_status
def clean_status(df):
    df_clean = df
    df_clean["status"] = df_clean["status"].str.strip().str.lower()
    status_map = {
        'genomförd':'bekräftad',
        'klar':'bekräftad',
        'completed':'bekräftad',
        'confirmed':'bekräftad',
        'ok':'bekräftad',
        'bokad':'bekräftad',
        'struken':'avbokad',
        'avbokad':'avbokad',
        'cancelled':'avbokad',

        'färdig': 'bekräftad',
        'done': 'bekräftad',
        'avbeställd': 'avbokad',
        'conf': 'bekräftad',
        'bokat': 'bekräftad',
        'canceled': 'avbokad'

    }
    df_clean['status'] = df_clean['status'].map(status_map).fillna(df_clean['status'])

    return df_clean



# clean_destination
def clean_destination(df):
    df_clean = df
    df_clean["destination"] = df_clean["destination"].str.strip().str.lower()
    destination_map = {
        'kopenhamn':'köpenhamn',
        'oslö':'oslo',
        'rey':'reykjavik',
        'reykjavik':'reykjavik',
        'reykavik':'reykjavik',
        'tromsö':'tromsø',
        'tromso':'tromsø',
        'hfors':'helsingfors',
        'rovaniem':'rovaniemi',
        'tronheim':'trondheim',
        'københavn':'köpenhamn',
        'cph':'köpenhamn',
        'copenhagen':'köpenhamn', 

        'köpnhamn':'köpenhamn',
        'köpenhman':'köpenhamn',
        'bergen':'bergen',
        'köpenhamnn':'köpenhamn',
        'tromsøø':'tromsø',
        'reykajvik':'reykjavik',
        'reykjavk':'reykjavik',
        'helsingfross':'helsingfors',
        'reikjavik':'reykjavik',
        'helsingfor':'helsingfors',
    }
    df_clean['destination'] = df_clean['destination'].map(destination_map).fillna(df_clean['destination'])

    return df_clean


# clean_land
def clean_land(df):
    df_clean = df
    df_clean["land"] = df_clean["land"].str.strip().str.lower()

    city_to_country = {
        'köpenhamn':'danmark',
        'oslo':'norge',
        'reykjavik':'island',
        'tromsø':'norge',
        'helsingfors':'finland',
        'rovaniemi':'finland',
        'trondheim':'norge',
        'helsinki':'finland',
        'bergen':'norge'
        }

        # Apply mapping if value is a known city
    df_clean['land'] = df_clean['land'].map(city_to_country).fillna(df_clean['land'])

    return df_clean



# clean_pris_sek
#def clean_pris_sek(df):
    #df_clean = df.copy()
    #valuta_map = {
        #'sek': 1.0,
        #'eur': 11.3,
        #'nok': 0.95,
        #'dkk': 1.52
    #}
    #df_clean['valuta_clean'] = df_clean['valuta'].str.strip().str.lower()
    #df_clean['pris_sek'] = df_clean['pris'] * df_clean['valuta_clean'].map(valuta_map)
    #df_clean = df_clean.drop(columns=['valuta_clean'])
    
    #return df_clean


# clean_pakettyp
def clean_pakettyp(df):
    df_clean = df
    df_clean["pakettyp"] = df_clean["pakettyp"].str.strip().str.lower()
    paket_map = {
        'weekend-paket':'weekend-paket',
        'weekend paket':'weekend-paket', 
        'weekend':'weekend-paket',
        'helgresa':'weekend-paket',
        'halvpension':'halvpension',
        'hp':'halvpension',
        'half board':'halvpension',
        'all-inclusive':'all-inclusive', 
        'all inclusive':'all-inclusive', 
        'all inkl':'all-inclusive', 
        'ai':'all-inclusive',
        'endast-boende':'endast-boende', 
        'room only':'endast-boende', 
        'ro':'endast-boende', 
        'bara hotell':'endast-boende',


        'endast boende':'endast-boende',
        'all-inclusive':'all-inclusive',
        'weekendresa':'weekend-paket',
        'bed & breakfast':'endast-boende',
        'helgpaket':'helgpaket',
        'full board':'all-inclusive',
        'kortresa':'helgpaket',
        'fb':'all-inclusive',
        'allt ingår':'all-inclusive',
        'b&b':'endast-boende'
    }
    df_clean['pakettyp'] = df_clean['pakettyp'].map(paket_map).fillna(df_clean['pakettyp'])
    
    return df_clean


# clean_bokningskanal
def clean_bokningskanal(df):
    df_clean = df
    df_clean["bokningskanal"] = df_clean["bokningskanal"].str.strip().str.lower()
    kanal_map = {
        'webb':'website', 
        'web':'website', 
        'website':'website', 
        'online':'website', 
        'internet':'website',
        'app':'app', 
        'telefon':'telefon', 
        'phone':'telefon', 
        'mobile':'telefon', 
        'ring':'telefon', 
        'tel':'telefon', 
        'mobil':'telefon'
    }
    df_clean['bokningskanal'] = df_clean['bokningskanal'].map(kanal_map).fillna(df_clean['bokningskanal'])
    
    return df_clean


# remove_duplicates
#def remove_duplicates(df):
    #df_clean = df.copy()
    #df_clean = df_clean.drop_duplicates(subset=["bokning_id"])
    
    #return df_clean

# Optional: save to SQLite
def load_data(df, db_path='output/resenorden.db', table_name='resenorden'):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Data has been saved to {db_path} in the table '{table_name}'")
    conn.close() 


def pd_data(df, db_path='output/resenorden.db', table_name='resenorden'):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    print(f"Data has been saved to {db_path} in the table '{table_name}'")
    conn.close() 


# Main pipeline function
def transform_data(df):
    df_clean = df.copy()
    
    df_clean = clean_dates(df_clean)
    df_clean = clean_status(df_clean)
    df_clean = clean_destination(df_clean)
    df_clean = clean_land(df_clean)
    df_clean = clean_pakettyp(df_clean)
    df_clean = clean_bokningskanal(df_clean)
    #df_clean = clean_pris_sek(df_clean)
    #df_clean = remove_duplicates(df_clean)
    
    load_data(df_clean)  # optional: save to SQLite, pd_data
    
    return df_clean

