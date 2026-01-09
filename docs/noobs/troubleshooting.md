# Troubleshooting Guide for Beginners

Stuff breaks. It happens. Here is how to fix the most common errors in WaveQL.

## 1. "Authentication Failed" or "401 Error"
**Problem**: WaveQL can't log in to your service (like Zendesk or Salesforce).
**Fixes**:
-   Check your `waveql.config` file. Did you paste the API key correctly?
-   Did you accidentally copy a space at the end?
-   Is your API Token expired? Try generating a new one in the service's dashboard.

## 2. "Table Not Found"
**Problem**: `SELECT * FROM ghost_table` -> Error!
**Fixes**:
-   Check your spelling.
-   Run `conn.list_tables()` in Python to see what WaveQL *can* see.
-   Do you have the right adapter enabled? (e.g., trying to query `salesforce` but you only configured `zendesk`).

## 3. "Timeout" or "It's just hanging..."
**Problem**: The query is taking forever.
**Fixes**:
-   **Are you asking for too much?** `SELECT * FROM tickets` might try to download 1 million tickets. Try adding `LIMIT 5` to test.
-   **Add a Filter**. `WHERE status='open'` helps the API give you less data, which is faster.

## 4. "ImportError: No module named..."
**Problem**: You tried to run the script but Python is confused.
**Fixes**:
-   Did you run `pip install waveql`?
-   Are you in the virtual environment?
-   If you are on the team, did you run `pip install -e .`?

## 5. Getting Help
If you are still stuck:
1.  Read the error message carefully. Python tries to tell you what line failed.
2.  Check the `logs/` folder if you have one.
3.  Ask usage questions on our GitHub Discussions page.
