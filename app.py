#!/usr/bin/env python
# coding: utf-8

# # Dev AWV - Vorbereitung zum Upload meldepflichtiger Transaktionen gem AWV an das Bundesbank Melderegister
# 
# Prozess:
# * Laden FlexQuery von International Brokers
# * Spalten filtern und umbenennen
# * Ergänzen von Wechselkursen (Quelle: Yahoo Finance, tagesgenau)
# * Summieren nach OrderID
# * Filtern auf Relevanz (12,5kEUR Transaktion)
# * Erstellen CSV Format für Bundesregister

"""
# Job: Monthly creation of AWS Bundesregister reporting

* loads all transactions of the past months from LYNX via FlexQuery
* Filters and groups the data in a way that it can be submitted as CSV to the Bundesbank Meldeportal Statistik

Watch-outs: Some steps might not be 100% accurate - check before uploading
* Cut-off 12.500 EUR: Is taken on a monthly sum, not individual transactions, as there are different interpretation (err on the safe side)
* Inländisch/Ausländisch is decided by the first two letters of ISIN (not sure if this is always correct)
* No separation included for German Banken/Nicht-Banken

## D I S C L A I M E R #######################################
This is an improvised solution without guarantee or liability for functionality and correctness.
Use it at your own risk, and check results carefully before submitting.
"""

# ## Imports
import datetime as dt
# Argument parsing
import argparse
# Data management
import pandas as pd
# FlexQuery IB
from ibflex import client
# Currency conversion
import yfinance as yf

# ## Settings

# Settings

parser = argparse.ArgumentParser(description='AWV-Reporting Tool')
parser.add_argument("--accountName", help="Bitte den Account Name des Kontos in IB eingeben.")
parser.add_argument("--queryID", help="Bitte die queryID der FlexQuery von IB eingeben.", type=int)
parser.add_argument("--token", help="Bitte den FlexQuery-Token des Kontos in IB eingeben.")
parser.add_argument("--limit", help="Bitte die Meldeschwelle in TEUR eingeben (default: 12.5).", type=float)
parser.add_argument("--withoutLimit", help="Bitte '1' angeben, wenn ein Report ohne Meldeschwelle erstellt werden soll. (default: 0)", type=int)
args = parser.parse_args()

if(not args.accountName or not args.queryID or not args.token):
    print("Bitte alle Argumente eingeben.")
    exit()
if (not args.limit):
    limit = 12.5
else:
    limit = args.limit
    
if (not args.withoutLimit):
    withoutLimit = 0

# Limit for transactions in 1000 EUR:
#limit = args.limit # AWV requires transactions above limit to be reported.
currency = 'USD'

# Country ID to be used for counterparty if asset is German
counterpartCountryCodeGermanStocks = 'IE' # The counterparty for German stocks is Interactive Brokers, Ireland

# FlexQuery settings 
"""
# Kontoumsatz-Flex-Query - Einstellungen:
* Format: XML
* Abschnitt "Trades" aktiviert. 
* Datumsformat: YYYY-MM-DD
* Zeitformat: HH:mm:ss
* Datum-Uhrzeit-Trennzeichen: ' ' (Leerzeichen)
* Zeitraum: Letzte 50 Kalendertage

"""

# Definition of account:
accounts = [
    { 'accountName': args.accountName, 'queryID': args.queryID, 'token': args.token }
]

"""
Definition of Option exchanges: 
* 831 for German exchanges 
* 821 for foreign exchanges

https://www.bundesbank.de/resource/blob/883132/2ba0e593d28728f79fdde7adc55aa76b/mL/statso7-2013-data.pdf
Page 105

# List of option exchanges:
# https://www.interactivebrokers.com/en/index.php?f=1563&p=europe_opt&conf=am
"""

optionExchanges = {
    'AMEX':821, # USA
    'ASX':821, # Australia
    'BATS':821, # USA
    'BELFOX':821, # Belgium
    'BOX':821, # USA
    'CBOE': 821, # USA/Chicago
    'CBOE2': 821, # USA/Chicago
    'DTB':831, # Germany
    'CDE':821, # Canada
    'EDGX':821, # USA
    'EMERALD':821, # USA
    'EUREX':831, # Germany
    'EUREXUK':831, # Germany
    'FTA': 821, # Netherlands
    'GEMINI': 821, #  USA
    'HKFE': 821, # Hong Kong
    
    # If you use more optionExchanges, you need to add them here!
}

# Identify correct reporting period based on current month - 1
currentYear = dt.datetime.now().year
currentMonth = dt.datetime.now().month
if currentMonth == 1:
    month = 12
    year = currentYear - 1
else:
    month = currentMonth - 1
    year = currentYear

# ## Functions

def fxRate (currency, date): 
    # Returns f/x rate on given date from yahoo finance
    if currency == 'EUR':
        return 1
    
    date = pd.to_datetime(date, format='ISO8601')
    try:
        minDate = date + dt.timedelta(days=0)
        maxDate = date + dt.timedelta(days=1)
        fx = yf.download(currency + "EUR=X", start=minDate, end=maxDate, auto_adjust=True, progress=False).sort_values(by=['Date'])['Close'].values[0]
    except:
        fx = 0
    return fx


# ## Main script

# Load data from Interactive Brokers FlexQuery:

# Initialize empty dataframe:
df = pd.DataFrame()

for i in range(len(accounts)):
    token = accounts[i]['token']
    queryID = accounts[i]['queryID']
    nameAccount = accounts[i]['accountName']
    
    # Read from API
    try:
        response = client.download(token, queryID)

        # Read into dataframe:
        try:
            # Transfer xml to dataframe:
            df_new = pd.read_xml(response, xpath=".//Trade")
            df_new['Account'] = nameAccount 

            # Join new data with existing data:
            df = pd.concat([df, df_new])
        except:
            print('No transactions found in FlexQuery ' + str(token) + ', or format wrong. Error in transaction transfer from IB', 'Could not read query.')
            exit()
            
    except Exception as Ex:
        print('Could not load raw data, or no transactions available, from ' + nameAccount + ' for token: "' + str(token) +'". ' + str(Ex))

# Remove canceled trades:
df = df.loc[df['transactionType']!='TradeCancel']

# Remove unneeded columns from dataframe
neededCols = ['currency', 'assetCategory', 'description', 'isin', 'ibOrderID', 'tradeDate', 'buySell', 'origTradePrice', 'quantity', 'proceeds', 'putCall', 'listingExchange']

# Filter dataframe:
df = df[neededCols]

# Rename columns to already match the required AWV csv format:
renameCols = {'currency':'Währung', 'description':'Zahlungszweck / Wertpapierbezeichnung', 'isin':'ISIN', 'tradeDate':'Datum', 'buySell':'Transaktion', 'quantity':'Stückzahl',
              'proceeds':'Betrag NC', 'ibOrderID':'orderID'} # 'orderTime':'Auftragszeit'

df = df.rename(columns=renameCols)

df['Datum'] = pd.to_datetime(df['Datum'], format='ISO8601')

# Filter time period to defined month:
startDate = dt.datetime(year, month, 1)
if month==12:
    endDate = dt.datetime(year + 1, 1, 1) 
else:
    endDate = dt.datetime(year, month + 1, 1)

# Apply date filter to dataframe:
df = df.loc[df['Datum']>= startDate]
df = df.loc[df['Datum']< endDate]

# Remove irrelevant transactions (=Currencies):
df = df.loc[df['assetCategory']!='CASH']

# Add fx rates:
df['fxRate'] = None
df['Datum'] = pd.to_datetime(df['Datum'], format='ISO8601')
df['fxRate'] = df.apply(lambda x: fxRate(x['Währung'], x['Datum']), axis=1)

# Translate transaction types to german terms; include "ca." transactions:
df.loc[df['Transaktion']=='BUY', 'Transaktion'] = 'Kauf'
df.loc[df['Transaktion']=='BUY (Ca.)', 'Transaktion'] = 'Kauf'
df.loc[df['Transaktion']=='SELL', 'Transaktion'] = 'Verkauf'
df.loc[df['Transaktion']=='SELL (Ca.)', 'Transaktion'] = 'Verkauf'
df['Betrag'] = df['Betrag NC'] * df['fxRate'] / 1000 # in 1000 EUR

# Fill NaN fields with empty string, because otherwise, they don't get included in grouping:
df['putCall'] = df['putCall'].fillna('') 
df['ISIN'] = df['ISIN'].fillna('') 

# Group partial executions on orderID into df_g(rouped):
df_g = df.groupby(['orderID', 'Währung', 'Zahlungszweck / Wertpapierbezeichnung', 'ISIN', 'Transaktion', 'putCall', 'listingExchange']).agg({'Stückzahl': 'sum', 'Datum':'mean', 'Betrag':'sum'}).reset_index()

if len(df_g.index)>0:
    # Load country from ISIN:
    # This is an approximation, it could be that the ISIN country code is not identical with the location of the country.
    df_g['Länderschlüssel']=df_g.apply(lambda x: x['ISIN'][0:2], axis=1)
    
    # Set it to InteractiveBrokers Location for all DE stocks:
    df_g.loc[df_g['Länderschlüssel']=='DE', 'Länderschlüssel']= counterpartCountryCodeGermanStocks
else:
    df_g['Länderschlüssel'] = ''

# Group to month:
df_m = df_g.groupby(['Währung', 'Zahlungszweck / Wertpapierbezeichnung', 'Länderschlüssel', 'ISIN', 'putCall', 'listingExchange', 'Transaktion']).agg({'Stückzahl': 'sum', 'Betrag':'sum'}).reset_index()

# Limit to relevant transactions above limit (on a monthly base):
if withoutLimit < 1:
    df_m = df_m.loc[abs(df_m['Betrag']) >= limit]

try:
    # Add additional columns:
    df_m['Belegart']=3 # Put everything as "Eingehende Zahlung"
    df_m.loc[df_m['Transaktion']=='Kauf', 'Belegart'] = 4 # Change the purchases to "Ausgehende Zahlung"
    
    # Assign options first for Kennzahl: 
    # * 821: Optionen, ausländische Terminbörsen
    # * 831: Optionen, inländische Terminbörsen
    # For option trades, use the look-up dictionary "optionExchanges" to get the right Kennzahl:
    df_m.loc[df_m['putCall']!='', 'Kennzahl'] = df_m.loc[df_m['putCall']!=''].apply(lambda x: optionExchanges[x['listingExchange']], axis=1)
except Exception as E:
    if len(df_m.index)>0:
        print('Error modifying dataframe:'+str(E))

try:
    # Kennzahl für Aktien:
    # * 104: Aktien ausländischer Emittenten
    # * 258: Nicht-Bank-Aktien inländischer Emittenten

    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # ATTENTION: This will not create the right result for ADR (American Depository Receipts).
    # They need to be classified by the related stock. E.g. Biontech. 
    # Requires manual adjustment after upload!!!
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
    # Set all transactions that don't have a Kennzahl yet to "ausländische Emittenten":
    df_m.loc[(df_m['ISIN'].str[0:2]!='DE') & (df_m['ISIN'].str[0:2]!=''), 'Kennzahl'] = 104 # ausländische Emittenten
    
    # ... and then switch those where the WKN is "DE" to German.
    df_m.loc[df_m['ISIN'].str[0:2]=='DE', 'Kennzahl'] = 258 # inländische Nichtbanken
    
    df_m['Stückzahl'] = df_m['Stückzahl'].abs()
except Exception as E:
    if len(df_m.index)>0:
        print('Error modifying dataframe- changing Stückzahl:'+str(E))

try:
    # Sort columns in correct order:
    df_m = df_m [['Belegart', 
                  'Kennzahl', 
                  'Zahlungszweck / Wertpapierbezeichnung', 
                  'Länderschlüssel', 
                  'Betrag', 
                  'ISIN', 
                  'Stückzahl',
                  'Währung'
                 ]]
except Exception as E:
    if len(df_m.index)>0:
        print('Error reducing df columns:'+str(E))

# Round and turn to integer:
df_m = df_m.round()

# Columns that need to be converted to integer:
intCols = [['Belegart', 
            'Kennzahl', 
            'Betrag', 
            'Stückzahl',
          ]]

# Run through intCols and convert:
try:
    for intCol in intCols:
        df_m[intCol] = df_m[intCol].astype("int")
except Exception as E:
    if len(df_m.index)>0:
        print('Error modifying dataframe:'+str(E))

# Finally, some values need to be inverted so they appear with a positive sign:
try:
    # Turn values positive:
    df_g['Betrag']=df_g['Betrag'].abs()
    df_g['Stückzahl'] = df_g['Stückzahl'].abs() 
    df_m['Betrag']=df_m['Betrag'].abs()
    df_m['Stückzahl'] = df_m['Stückzahl'].abs()
except Exception as E:
    if len(df_m.index)>0:
        print('Error modifying dataframe:'+str(E))

# Export to CSV file:
filename = 'Bundesbank AWV Melderegister ' + str(year) + "-" + str(month).zfill(2) + '.csv'
df_m.to_csv(filename, index=False, header=False, sep = ';')

# Sort descending (for display in e-mail):
df_g = df_g.sort_values(by=['Betrag'], ascending=False)
print('Data translation completed.')
print('CSV-Datei erstellt: '+filename)
