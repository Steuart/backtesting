from dotenv import load_dotenv
import os
load_dotenv()
trader_conn_str = os.getenv('DB')
