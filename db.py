from supabase import create_client

SUPABASE_URL = "https://kaqxagdykbhvorwfxcou.supabase.co"
SUPABASE_KEY = "sb_publishable_tumv4jt7h9v1waV05cJWmA_vdxg5bwy"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)