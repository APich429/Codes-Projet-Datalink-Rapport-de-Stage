import mysql.connector
from sqlalchemy import create_engine
import requests
import requests
import json
import pandas as pd
import os   
import datetime
import concurrent.futures
import traceback

heure_debut = datetime.datetime.now()
yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
today = datetime.datetime.now()
date_hier = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
date_id = date_hier = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
date_avant_hier = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")


######################################
### Recuperation TOKEN 
######################################


url = "https://api.sekoia.io/v1/sic/conf/alerts/types?limit=100"
payload = {}
headers = {
  'Authorization': 'Bearer '
}
### mot de passe retiré ici 


response = requests.request("GET", url, headers=headers, data=payload)
if response.status_code != 200:
    print(f"Error: {response.status_code}")
else :
    if response.json() != []:    
        data = response.json()

df_alerts= pd.DataFrame(data['items'])
df_alerts['categorie_alerte']=df_alerts['category_name']
df_alerts.drop(columns=['value','category_name', 'detail', 'description'], inplace=True)


def ajout_date_hier(df_date):
        dateJJ = yesterday.strftime("%Y-%m-%d")
        annee = yesterday.strftime("%Y")
        mois = yesterday.strftime("%m")
        jour = yesterday.strftime("%d")
        date_id = yesterday.strftime("%Y%m%d")
        new_date = pd.DataFrame({'date_id': [date_id], 'dateJJ': [dateJJ],
                                                        'annee': [annee], 'mois': [mois], 'jour': [jour]})
        df_date = pd.concat([df_date, new_date], ignore_index=True)
        return df_date

#### Example usage
df_date = pd.DataFrame(columns=['date_id', 'dateJJ', 'annee', 'mois', 'jour'])
df_date = ajout_date_hier(df_date)
df_date.head()


######################################
### requête alertes
######################################
url = "https://api.sekoia.io/v1/sic/alerts/entities?limit=20"

payload = {}
headers = {
  'Authorization': 'Bearer'
}
if response.status_code != 200:
    print(f"Error: {response.status_code}")
else :
  response = requests.request("GET", url, headers=headers, data=payload)
  data = response.json()
df_clients_sekoia= pd.DataFrame(data['items'])
liste_id = []
### enlever les doublons
for index in range(len(df_clients_sekoia)):
    if df_clients_sekoia['uuid'][index] not in liste_id:
      liste_id.append(df_clients_sekoia['uuid'][index])
    else : 
      df_clients_sekoia.drop(index, inplace=True)

df_clients_sekoia.drop(columns=['community_uuid'], inplace=True)  
df_clients_sekoia.rename(columns={'uuid': 'client_id', 'name': 'client_nom'}, inplace=True)
df_clients_sekoia.head(10)
## inverser les colonnes
df_clients_sekoia = df_clients_sekoia[['client_id', 'client_nom']]


ids_clients = df_clients_sekoia['client_id']

df_fact_alert_sekoia = pd.DataFrame()

def recup_data_alertes(item) :
    dico_alerte = {'date_id':'', 'client_id':'','alerte_id':'', 'categorie_alerte':'', 'criticite_alerte':''} 
    dico_alerte['date_id'] = date_id
    if 'entity' in item.keys():
        dico_alerte['client_id'] = item['entity']['uuid']
    if 'uuid' in item.keys():  
        dico_alerte['alerte_id'] = item['uuid']
    if 'alert_type' in item.keys():    
        dico_alerte['categorie_alerte'] = item['alert_type']['category']
    if 'rule' in item.keys():    
        dico_alerte['criticite_alerte'] = item['rule']['severity']
    return dico_alerte


######################################
### Requête nombre d'alertes
######################################

date_hier = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


for id_client in ids_clients:
        url ='https://api.sekoia.io/v1/sic/alerts?match[entity_uuid]='+id_client+'&date[created_at]='+date_hier+'T00:00:00Z'+','+date_hier+'T23:59:59Z&limit=100'
        payload = {}
        headers = {
    'Authorization': 'Bearer'
    }
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
        else : 
            if response.json() != []:    
                data = response.json()
                for item in data['items']:
                    dico_alerte = recup_data_alertes(item)
                    df_fact_alert_sekoia = pd.concat([df_fact_alert_sekoia, pd.DataFrame([dico_alerte])], ignore_index=True)

df_fact_alert_sekoia = df_fact_alert_sekoia[['alerte_id', 'date_id', 'client_id',  'categorie_alerte', 'criticite_alerte']]
df_fact_alert_sekoia.to_csv(f'csv/alertes/fact_alerte_sekoia_{date_hier}.csv', sep=',', index=False, encoding='utf-8', header=True)
print(len(df_fact_alert_sekoia), " alertes pour le ", date_hier)
df_fact_alert_sekoia.head(10)


ids_clients = df_clients_sekoia['client_id']


df_fact_event_sekoia = pd.DataFrame()

def recup_data_events(data) :
    dico_alerte = {'date_id':'', 'client_id':'','nb_event':'', 'nb_event_alert':''} 
    dico_alerte['date_id'] = date_id
    dico_alerte['client_id'] = id_client
    if 'total' in data.keys():
        dico_alerte['nb_event'] = data["total"]
    if 'warning_events_occurences' in data.keys():
        dico_alerte["nb_event_alert"] = data['warning_events_occurences']
    return dico_alerte


for id_client in ids_clients:
    url = "https://api.sekoia.io/v1/telemetry/events-by-status/counters"

    payload = json.dumps({
    "latest": date_hier+'T23:59:59Z',
    "earliest": date_hier+'T00:00:00Z',
    "filters": {
        "entity_uuid": [
        id_client
        ]
    }
    })
    headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
    else : 
        if response.json() != []:    
            data = response.json()
            dico_events = recup_data_events(data)
            df_fact_event_sekoia = pd.concat([df_fact_event_sekoia, pd.DataFrame([dico_events])], ignore_index=True)

df_fact_event_sekoia.to_csv(f'csv/evenements/fact_event_sekoia_{date_hier}.csv', sep=',', index=False, encoding='utf-8', header=True)
print(len(df_fact_event_sekoia), " evenements pour le ", date_hier)
df_fact_event_sekoia.head(10)


# Fonction pour afficher la longueur maximale de chaque colonne d'un DataFrame
def afficher_longueur_max(df, nom_table):
    print(f"Table : {nom_table}")
    for col in df.columns:
        max_len = df[col].astype(str).map(len).max()
        print(f"  {col} : {max_len}")
    print("\n")

afficher_longueur_max(df_fact_alert_sekoia, "fact_alert_sekoia")
afficher_longueur_max(df_fact_event_sekoia, "fact_event_sekoia")
afficher_longueur_max(df_clients_sekoia, "dim_client_sekoia")
