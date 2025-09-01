# Test this in your terminal from the mcp_server directory
cd C:\Dev\projects\cfdb_free_ml_project\mcp_server
python -c "
from databricks import sql
import os
from dotenv import load_dotenv
load_dotenv()

connection = sql.connect(
    server_hostname=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
    http_path=os.getenv('DATABRICKS_HTTP_PATH'),
    access_token=os.getenv('DATABRICKS_ACCESS_TOKEN')
)
cursor = connection.cursor()
cursor.execute('SELECT 1 as test')
print(cursor.fetchall())
connection.close()
"