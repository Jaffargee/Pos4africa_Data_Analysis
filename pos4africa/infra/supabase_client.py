from supabase import create_client, Client
from pos4africa.config import settings


spb_client: Client = create_client(settings.supabase_url, settings.supabase_key.get_secret_value())
