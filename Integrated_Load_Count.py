import pyodbc
import pandas as pd
from datetime import datetime
import os
import warnings
import smtplib
from email.message import EmailMessage

warnings.simplefilter(action='ignore', category=UserWarning)

# Create output folder
save_folder = "JobCounts"
os.makedirs(save_folder, exist_ok=True)
filename = os.path.join(
    save_folder,
    f"IntegrationJobCount_{datetime.now().strftime('%Y%m%d')}.csv"
)

# Read all database info from environment variables
db_server = os.environ.get("DB_SERVER")
db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")

# Check if any variable is missing
missing_vars = [var for var, val in {"DB_SERVER": db_server, "DB_NAME": db_name, "DB_USER": db_user, "DB_PASSWORD": db_password}.items() if not val]
if missing_vars:
    raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

# Connect to the database
conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={db_server};'
    f'DATABASE={db_name};'
    f'UID={db_user};'
    f'PWD={db_password};'
)

print("Connected to database successfully!")


# SQL query
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

df = pd.read_sql_query(query, conn)
df.to_csv(filename, index=False)

print("Query executed, CSV generated:", filename)

conn.close()

# ---- EMAIL SETUP ----
EMAIL_ADDRESS = os.environ["EMAIL_USER"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]

SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587

msg = EmailMessage()
msg['Subject'] = f"Integration Job Count Report - {datetime.now().strftime('%Y-%m-%d')}"
msg['From'] = EMAIL_ADDRESS
msg['To'] = "vijay_m@exdion.com"
msg.set_content(
    "Hi,\n\nPlease find attached the Integration Job Count report.\n\nRegards,\nVijay M"
)

# Attach CSV
with open(filename, 'rb') as f:
    msg.add_attachment(
        f.read(),
        maintype='application',
        subtype='octet-stream',
        filename=os.path.basename(filename)
    )

print("Sending email...")

with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
    smtp.starttls()
    smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    smtp.send_message(msg)

print("Email sent successfully via SMTP!")
