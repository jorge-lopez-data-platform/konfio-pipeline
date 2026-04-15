import requests

#extraeremos los datos de la API de Frankfurter para el rango de fechas del 1 al 10 de enero de 2024, con base en USD y las monedas MXN, EUR, BRL y COP
def extract_data():
    url = "https://api.frankfurter.dev/v1/2024-01-01..2024-01-10?base=USD&symbols=MXN,EUR,BRL,COP"
    
    response = requests.get(url, timeout=10)
    data = response.json()

    rows = []

    for date, rates in data["rates"].items():
        for currency, rate in rates.items():
            rows.append((date, currency, float(rate)))

    return rows