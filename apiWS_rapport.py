###########################################
### Code de récupération des données de WithSecure
### le code d'accès à été retiré pour rendre le code public
###########################################



import datetime
import requests
import pandas as pd
##########################################################################
### Recuperation TOKEN
##########################################################################
url = "https://api.connect.withsecure.com/as/token.oauth2"
payload = 'grant_type=client_credentials&scope=connect.api.read'
headers = {
  'Content-Type': 'application/x-www-form-urlencoded',
  'Authorization': 'Basic' ### le code d'accès à été retiré pour rendre le code public
  }

response = requests.request("POST", url, headers=headers, data=payload)
if response.status_code != 200:
    print(f"Erreur : {response.status_code}")
responsestr = response.text
responsejson = response.json()

token = responsejson["access_token"]
print('TOKEN : ', token)

#############################################################################
### premiere requete sur les organisations
#############################################################################
url = "https://api.connect.withsecure.com/organizations/v1/organizations"
payload = {}
headers = {
  'Authorization': 'Bearer '+ token
}
response = requests.request("GET", url, headers=headers, data=payload)
if response.status_code != 200:
    print(f"Erreur : {response.status_code}")
response = response.json()
dfclients = pd.DataFrame(response["items"])
dfclients = dfclients.drop(columns=['type'])

dfclients = dfclients.rename(columns={'id': 'client_id', 'name': 'client_nom'})
dfclients = dfclients[dfclients.columns[::-1]]  # Inverser l'ordre des colonnes
dfclients = dfclients.rename(columns={'id': 'client_id', 'name': 'client_nom'})

###############################################################################
### deuxieme requete sur les appareils
###############################################################################
url = "https://api.connect.withsecure.com/devices/v1/devices"
payload = {}
headers = {
  'Authorization': 'Bearer ' + token
}

response = requests.request("GET", url, headers=headers, data=payload)
if response.status_code != 200:
    print(f"Erreur : {response.status_code}")
response = response.json()

### création des dataframe vide 
dffact = pd.DataFrame()
dfappareil = pd.DataFrame()

# correspondance de l'API et de de qu'on veut :
#  "patchOverallState" = etat_maj 
#  "malwareState" = protection_malware
# "protectionStatusOverview" = etat_protection

def recup_informations_appareil(item) :
    """
    Récupère les éléments souhaités dans la table dim_appareil
    dans WithSecure pour chaque item(appareil) s'ils existent
    """
    dico_item = {'appareil_uuid':'', 'appareil_ip':'', 'appareil_nom':'',}
    if 'id' in item.keys() :
         dico_item['appareil_uuid'] = item['id']   
    if 'ipAddresses' in item.keys() :
        dico_item['appareil_ip'] = item['ipAddresses']
    if 'name' in item.keys() :  
        dico_item['appareil_nom'] = item['name']
    return dico_item

def recup_informations_fact(item) :
    """
    Récupère les éléments souhaités dans la table fact_withsecure
    dans WithSecure pour chaque item(appareil) s'ils existent
    """
    dico_item = {'date_id':'', 'client_id':'', 'appareil_uuid':'',
                 'protection_malware':'', 'etat_maj':'', 'etat_edr':'',
                 'LastScan_protection_malware':'', 'LastScan_maj':'',
                 'LastScan_edr':''}
    
    dico_item['date_id'] = datetime.datetime.now().strftime("%Y%m%d")
    if 'company' in item.keys() :
        if 'id' in item['company'].keys() :
            dico_item['client_id'] = item['company']['id']
    if 'id' in item.keys() :
         dico_item['appareil_uuid'] = item['id']   
    if 'malwareState' in item.keys() :
        dico_item['protection_malware'] = item['malwareState']
    if 'patchOverallState' in item.keys() :
        dico_item['etat_maj'] = item['patchOverallState']
    if 'patchOverallState' in item.keys() :
        dico_item['etat_edr'] = item['protectionStatusOverview']
    if 'malwareDbUpdateTimestamp' in item.keys() :
        dico_item['LastScan_protection_malware'] = item['malwareDbUpdateTimestamp']
    if 'patchLastScanTimestamp' in item.keys() :
        dico_item['LastScan_maj'] = item['patchLastScanTimestamp']
    if 'statusUpdateTimestamp' in item.keys() :
        dico_item['LastScan_edr'] = item['statusUpdateTimestamp']
    return dico_item

### on remplit les dataframes avec les informations
for appareil in response['items'] :
        dico_fact = recup_informations_fact(appareil)
        dffact = pd.concat([dffact, pd.DataFrame([dico_fact])], ignore_index = True)  
        dico_appareil = recup_informations_appareil(appareil)
        dfappareil = pd.concat([dfappareil, pd.DataFrame([dico_appareil])], ignore_index = True)
        compteur = 1

### s'il y a une nextAnchor, on la prend pour continuer
if "nextAnchor" in response.keys() :                
    while "nextAnchor" in response.keys() :
        url = "https://api.connect.withsecure.com/devices/v1/devices"
        url = url +'?anchor=' +response["nextAnchor"][:-2]
        print(url)
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code != 200:
            print(f"Erreur : {response.status_code}")
        response = response.json()
        for appareil in response['items'] :
            dico_fact = recup_informations_fact(appareil)
            dffact = pd.concat([dffact, pd.DataFrame([dico_fact])], ignore_index = True)                   
            dico_appareil = recup_informations_appareil(appareil)
            dfappareil = pd.concat([dfappareil, pd.DataFrame([dico_appareil])], ignore_index = True)
        compteur+=1
print('Nombre de nextAnchor : ', compteur)


#########################
### création de fichiers csv permettant une vérification des données récupérées ce jour là
#########################
dfclients.to_csv('dim_clientWS.csv', index=False)
dffact.to_csv('fact_appareilWS.csv', index=False)
dfappareil.to_csv('dim_appareilWS.csv', index=False)

