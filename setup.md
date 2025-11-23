# LLM Bank Data Assistant - Setup Instructions

## Overview
This setup integrates your existing Python backend with a modern React web interface. The architecture is:
- **Backend**: Flask API wrapping your existing LLM + DB logic
- **Frontend**: React chat interface (runs in this Claude artifact)

## Prerequisites
- Python 3.8+
- Your existing project structure with `db/`, `llm_layer/`, `analysis.py`, etc.
- Data files in `./data/` directory
- Internet connection (for LLM API calls)

## Step 1: Install Python Dependencies

```bash
# Install Flask and CORS support
pip install flask flask-cors

# If you don't have other dependencies already:
pip install pandas sqlalchemy requests matplotlib
```

Or use the requirements file:
```bash
pip install -r requirements.txt
```

## Step 2: Add the Flask Backend

Create a file called `app.py` in your project root (same level as `main.py`):

```python
# Copy the content from the "app.py - Flask Backend" artifact
```

## Step 3: Project Structure

Your project should look like this:

```
your-project/
├── app.py                          # NEW: Flask backend
├── requirements.txt                # NEW: Python dependencies
├── main.py                         # Your existing CLI
├── analysis.py
├── db/
│   ├── __init__.py
│   ├── etl.py
│   └── utils.py
├── llm_layer/
│   ├── __init__.py
│   ├── llm_client.py
│   └── reasoning.py
└── data/
    ├── account.csv
    ├── business_rel.csv
    ├── transactions.csv
    ├── schema.json
    ├── semantic_layer.json
    └── ... (other CSV files)
```

## Step 4: Start the Backend

```bash
python app.py
```

You should see:
```
Initializing database...
Database initialized successfully!
 * Running on http://127.0.0.1:5000
```

The backend will:
1. Load all CSV files from `./data/`
2. Create SQLite database (`bank_data.db`)
3. Start Flask API on port 5000

## Step 5: Use the Frontend

The React frontend is already running in this Claude artifact above! Once your Flask backend is running:

1. **The interface will automatically connect** to `http://localhost:5000`
2. Type questions like:
   - "List active accounts for BR-abc123"
   - "Show last 5 transactions for account X"
   - "analysis John Doe" (for transaction analysis with charts)

## Testing the Connection

1. **Backend health check**:
   ```bash
   curl http://localhost:5000/api/health
   ```
   Should return: `{"status":"ok","database":"connected"}`

2. **Test a query**:
   ```bash
   curl -X POST http://localhost:5000/api/query \
     -H "Content-Type: application/json" \
     -d '{"question":"List all accounts"}'
   ```

## How It Works

### Regular Questions
1. You type a question → Frontend sends to `/api/query`
2. Backend calls `answer_question()` → generates SQL → runs query
3. Returns: SQL, DataFrame results, LLM explanation
4. Frontend displays everything nicely

### Analysis Requests
1. Question starts with "analysis" (e.g., "analysis John Doe")
2. Backend calls `transactions_stats()` → generates matplotlib chart
3. Chart saved as PNG → converted to base64
4. Frontend displays the image inline

## Features

✅ **Chat-like interface** with conversation history  
✅ **SQL query display** to see what was generated  
✅ **Data tables** showing query results  
✅ **Image support** for analysis visualizations  
✅ **Error handling** with friendly messages  
✅ **Loading states** while processing  

## Troubleshooting

### CORS Errors
Make sure `flask-cors` is installed:
```bash
pip install flask-cors
```

### Database Not Found
The app auto-creates the database. If you see errors, delete `bank_data.db` and restart:
```bash
rm bank_data.db
python app.py
```

### LLM Connection Issues
Check that your LLM endpoint in `llm_layer/llm_client.py` is accessible:
```python
LLM_URL = "https://e6yzfqtids632a-8085.proxy.runpod.net/v1/chat/completions"
```

### Port Already in Use
If port 5000 is busy, change it in `app.py`:
```python
app.run(debug=True, port=5001)  # Use different port
```
Then update the frontend URL in the artifact (contact me if needed).

## Running Both Together

**Terminal 1** (Backend):
```bash
cd your-project
python app.py
```

**This Claude Artifact** (Frontend):
The React interface runs automatically in this conversation!

## Optional: Standalone Frontend

If you want to run the frontend separately (not needed, but possible):

1. Create `frontend/` directory
2. Copy the React component to a proper React app
3. Build and serve with:
   ```bash
   npm install
   npm start
   ```

But the artifact version works perfectly for your use case!

## Next Steps

- The interface is ready to use immediately
- Test with your sample questions
- For production: add authentication, better error handling, rate limiting
- Consider adding conversation memory/history persistence