from pos4africa.infra.supabase_client import spb_client
import asyncio, json

async def main():
      
      sale = (spb_client.table('sales').select('*, sale_items(*), payments(*)').eq('pos_sale_id', 300).execute())
      
      print(json.dumps(sale.data, indent=4))
      
      pass

asyncio.run(main())