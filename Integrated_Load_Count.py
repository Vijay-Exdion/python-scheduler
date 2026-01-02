import pyodbc
import pandas as pd
from datetime import datetime
import os
import warnings
import smtplib
from email.message import EmailMessage


#warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
warnings.simplefilter(action='ignore', category=UserWarning)

# save_folder = r"C:\Users\vijay_m\OneDrive - Exdion Solutions Pvt. Ltd.-70692290\Documents\Exdion Insurance\Auto Triggers\JobCounts"
# os.makedirs(save_folder, exist_ok=True)
# filename = os.path.join(save_folder, f"IntegrationJobCount_{datetime.now().strftime('%Y%m%d')}.csv")

save_folder = "JobCounts"
os.makedirs(save_folder, exist_ok=True)
filename = os.path.join(save_folder, f"IntegrationJobCount_{datetime.now().strftime('%Y%m%d')}.csv")


conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER=172.40.19.88,1433;'
    f'DATABASE=ExdionPOD;'
    f'UID={os.environ["DB_USER"]};'
    f'PWD={os.environ["DB_PASSWORD"]};'
)

# Your query
query = """
SELECT
    CAST(ProcessDate AT TIME ZONE 'UTC' AT TIME ZONE 'India Standard Time' AS DATE) AS ProcessDateOnly,
    BrokerID,
    COUNT(*) AS TotalCount
FROM [dbo].[tbl_Auto_Job_Create] WITH (NOLOCK)
WHERE ProcessDate >= DATEADD(DAY, -1, CAST(GETDATE() AT TIME ZONE 'India Standard Time' AS DATE))
GROUP BY 
    CAST(ProcessDate AT TIME ZONE 'UTC' AT TIME ZONE 'India Standard Time' AS DATE),
    BrokerID
ORDER BY 
    ProcessDateOnly,
    BrokerID;
"""

#df = pd.read_sql(query, conn)
df = pd.read_sql_query(query, conn)
df.to_csv(filename, index=False)
print(f"Results saved to {filename}")

conn.close()

EMAIL_ADDRESS = os.environ["EMAIL_USER"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]
      
SMTP_SERVER = "smtp.office365.com"       
SMTP_PORT = 587

msg = EmailMessage()
msg['Subject'] = f"Integration Job Count Report - {datetime.now().strftime('%Y-%m-%d')}"
msg['From'] = EMAIL_ADDRESS
msg['To'] = "vijay_m@exdion.com" #"sunil_chandrappa@exdion.com"    
msg.set_content("Hi,\n\nPlease find attached the Integration Job Count report.\n\nRegards,\nVijay M")

# Attach CSV
with open(filename, 'rb') as f:
    file_data = f.read()
    file_name = os.path.basename(filename)
msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

# Send email
with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
    smtp.starttls()
    smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    smtp.send_message(msg)

print("Email sent successfully via SMTP!")