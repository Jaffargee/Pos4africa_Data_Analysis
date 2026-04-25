import google.generativeai as genai
import os, json
from dotenv import load_dotenv

from pos4africa.infra.supabase_client import spb_client

sales = json.dumps((spb_client.table('sales').select('*, sale_items(*), payments(*)').limit(100).execute()).data)

with open('data.txt', 'w') as out:
      out.write(sales)

load_dotenv()
genai.configure(api_key=os.getenv('GEMINI_API_KEY'))

model = genai.GenerativeModel('gemini-3-flash-preview')
# response = model.generate_content("Explain quantum physics in one sentence.")
response = model.generate_content(f"As a data analyst, Analyse this sales transaction and give an actionable intel on it \n {sales}")

print(response.text)
