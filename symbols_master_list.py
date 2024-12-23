import pandas as pd
import requests

url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

resp = requests.get(url).json()
token_df = pd.DataFrame(resp)

token_name = 'SILVERMIC'
token_name_filter = token_df.loc[(token_df['name'] == token_name)]
print()