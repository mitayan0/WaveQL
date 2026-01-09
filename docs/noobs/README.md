# WaveQL: A to Z for Beginners

Welcome! If you're new to WaveQL (or even new to coding/databases), this guide is for you. We're going to explain everything from the ground up, no "PhD required" jargon.

## What is WaveQL?

Imagine you have data everywhere:
-   Spreadsheets in Google Sheets.
-   Customer tickets in Zendesk.
-   Files in the cloud (AWS S3, Google Cloud).
-   User data in a traditional database like Postgres.

Normally, to get answers from all these places, you'd have to write Python scripts, learn different APIs, and juggle a dozen passwords. It's a mess.

**WaveQL is a universal translator.** It lets you talk to **all** of these using just one language: **SQL**.

If you know how to write `SELECT * FROM table`, you already know how to use WaveQL to talk to *anything*.

---

## The "A to Z" Breakdown

### A. Adapters (The Connectors)
Think of an **Adapter** as a plug. You have a "Google Sheets Adapter", a "Zendesk Adapter", etc.
WaveQL plugs into these services so you don't have to worry about how they work. You just treat them like tables.

### B. Configuration (The Settings)
Before you start, you have to tell WaveQL where your data is. This is done in a `waveql.config` file or using environment variables. You give it your API keys, and WaveQL handles the security.

### C. Connection (The Handshake)
When you run WaveQL, it "connects" to everything defined in your config. It doesn't download all the data (that would be slow!). It just says "Hello" and asks "What tables do you have?".

### D. Dataframes (The Results)
WaveQL is built to work great with Python. When you run a query, you usually get back a **Dataframe** (using a library like `pandas` or `polars`). Think of a Dataframe like a super-powered programmable Excel sheet in memory.

### E. SQL (The Language)
SQL (Structured Query Language) is how we ask questions.
-   **SELECT**: "Give me..."
-   **FROM**: "...from this place..."
-   **WHERE**: "...but only if..."

**Example:**
```sql
SELECT title, priority
FROM zendesk.tickets
WHERE status = 'open'
```
This is readable, simple, and WaveQL turns it into the complex API calls Zendesk expects.

---

## Step-by-Step: Your First Query

### 1. Installation
First, you need Python installed. Then, open your terminal (command prompt) and run:
```bash
pip install waveql
```
*(Note: If you are working on the source code, you'd `pip install -e .`)*

### 2. The Code
Here is a complete Python script to search for a file in specific directory:

```python
import waveql

# 1. Connect
conn = waveql.connect()

# 2. Query
# Let's say we want to list files in a local directory using the 'os' adapter
results = conn.query("SELECT filename, size FROM os.files WHERE path = './data'")

# 3. View
print(results)
```

### 3. Mixing Data
The real magic is **Joins**. You can combine data from different places.
```sql
SELECT
    t.ticket_id,
    u.email
FROM zendesk.tickets AS t
JOIN google_sheets.users AS u ON t.requester_id = u.id
```
WaveQL is smart enough to fetch the data from Zendesk, fetch the data from Google Sheets, and combine them for you.

---

## Common Terms Glossary

-   **URI**: A simpler URL. e.g., `s3://my-bucket/data.csv`. It tells WaveQL "Type of data://Location".
-   **Schema**: The structure of your data. "Column A is text, Column B is a number."
-   **Pushdown**: An optimization. If you say `WHERE id = 5`, WaveQL tries to send that rule to the database/API so it only sends back one row, instead of downloading a million rows and filtering them on your computer.
-   **Semantic Layer**: A fancy way of saying "Definitions". If you have a complex formula for "Gross Profit", you define it once in the Semantic Layer, and everyone else just uses `SELECT gross_profit`.

## Where to go next?
-   Check out `docs/quickstart.md` for a slightly more technical start.
-   Look at `docs/adapters.md` to see what services query right now.
