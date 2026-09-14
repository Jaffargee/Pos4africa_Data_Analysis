from supabase import create_client, Client
from dotenv import load_dotenv
import os

load_dotenv()

if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_KEY"):
      raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in the environment variables.")

spb_client: Client = create_client(os.getenv("SUPABASE_URL") or "", os.getenv("SUPABASE_KEY") or "")