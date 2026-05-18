import json
import pandas as pd
import numpy as np
import sqlite3

filename="TieliikenneAvoinData_31_12_2025.csv"
cityfile="kuntarajat.json"

df = pd.read_csv(filename, sep=";", encoding="latin1", low_memory=False)
df = df[df['ajoneuvoluokka'].isin(["M1", "M1G"])]
df = df[df["ajoneuvonkaytto"].isin([1.0])]
df = df[df["korityyppi"].isin(["AA", "AB", "AC", "AD", "AE", 1.7])]
df = df.dropna(subset=["kayttovoima", "kunta"])
df = df[["ensirekisterointipvm", "kayttoonottopvm", "vari", "omamassa", "ajonKokPituus", "ajonLeveys", "kayttovoima", "merkkiSelvakielinen", "mallimerkinta", "kaupallinenNimi", "kunta", "NEDC_Co2", "matkamittarilukema"]]

with open(cityfile, 'r') as file:
    data = json.load(file)
    kunta_df = pd.json_normalize([feat['properties'] for feat in data['features']])

kunta_df = kunta_df[["NATCODE", "NAMEFIN"]]
kunta_df["NATCODE"] = kunta_df["NATCODE"].astype(float)
kunta_df.columns = ["kunta", "kunnan_nimi"]

df = pd.merge(df, kunta_df, on="kunta", how="inner")
df = df.drop("kunta", axis="columns")

df["kayttoonottopvm"]=df["kayttoonottopvm"].astype(str).str.replace("\0000$", "0101", regex=True)
df["kayttoonottopvm"] = pd.to_datetime(df["kayttoonottopvm"], format="%Y%m%d", errors="coerce")
df["ensirekisterointipvm"] = pd.to_datetime(df["ensirekisterointipvm"], format="%d.%m.%Y", errors="coerce")

df = df.dropna(subset=["kayttoonottopvm", "ensirekisterointipvm", "kunnan_nimi"])

df["kayttovoima"] = df["kayttovoima"].astype(float)
df.loc[df["kayttovoima"] == 4.0, "NEDC_Co2"] = 0
df.loc[df["vari"] == -1, "vari"] = pd.NA

df["kaupallinenNimi"]=df["kaupallinenNimi"].str.strip()
df["merkkiSelvakielinen"]=df["merkkiSelvakielinen"].str.strip()

df.loc[df["kaupallinenNimi"].isin(["SEBRING", "CROSSFIRE"]), "merkkiSelvakielinen"] = "Chrysler"
df.loc[(df["kaupallinenNimi"] == "XJ") & (df["merkkiSelvakielinen"] == "Daimler"), "merkkiSelvakielinen"] = "Jaguar"

korvaukset = {
    "QUATTRO": "Audi",
    "Quattro": "Audi",
    "ALPINA": "BMW",
    "Alpina": "BMW",
    "BMW Alpina": "BMW",
    "BMW i": "BMW",
    "BWW": "BMW",
    "GM Daewoo": "Daewoo",
    "FORD-CNG-TECHNIK": "Ford",
    "Ford-TEC": "Ford",
    "Hundai": "Hyundai",
    "Jaguar Land Rover Limited": "Jaguar",
    "Lada-Vaz": "Lada",
    "Niva": "Lada",
    "DaimlerChrysler": "Mercedes-Benz",
    "Daimler": "Mercedes-Benz",
    "MERCEDES-AMG": "Mercedes-Benz",
    "Mercedes-AMG": "Mercedes-Benz",
    "Mercedes-Benz-CI": "Mercedes-Benz",
    "BMW MINI": "Mini",
    "POLESTAR": "Polestar",
    "SALEEN": "Saleen",
    "SKD": "Skoda",
    "Skida": "Skoda",
    "TESLA MOTORS": "Tesla",
    "Tesla Motors": "Tesla",
    "THINK": "Think",
    "TOYOTA": "Toyota",
    "VOLKSWAGEN": "Volkswagen",
    "VW": "Volkswagen",
    "Volkswagen, VW": "Volkswagen"
}

df["merkkiSelvakielinen"] = df["merkkiSelvakielinen"].replace(korvaukset)

omat_korvaukset = {
    "Datsun-Nissan": "Datsun",
    "Range-Rover": "Land Rover",
    "Rover": "Land Rover",
    "AUSTIN-HEALEY": "Austin-Healey",
    "Chrysler-Sunbeam": "Sunbeam",
    "DE SOTO": "De Soto",
    "DE TOMASO": "De Tomaso",
    "DURANT": "Durant",
    "GOLIATH": "Goliath",
    "PACKARD": "Packard",
    "PANHARD": "Panhard",
    "VALIANT": "Plymouth"
}
    
df["merkkiSelvakielinen"] = df["merkkiSelvakielinen"].replace(omat_korvaukset)

df.rename(columns={'kunnan_nimi': 'kunta'}, inplace=True)

with sqlite3.connect("autodata.db") as sqltk:
    df.to_sql("autot", sqltk, if_exists="replace")