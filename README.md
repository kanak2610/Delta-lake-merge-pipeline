# Delta Lake MERGE Implementation

Incremental data processing using **Delta Lake**, demonstrating an upsert (UPDATE + INSERT) workflow with PySpark's `MERGE INTO` operation.

## 📌 Objective

Perform incremental data processing using Delta Lake — load a base dataset, clean it, simulate incoming incremental data, and merge the two using Delta Lake's ACID-compliant `MERGE` operation.

## 📁 Project Structure

```
delta-lake-assignment/
├── data/
│   ├── customer_master.csv        # Base/master dataset
│   └── customer_incremental.csv   # Simulated incremental/new data
├── notebooks/
│   └── delta_scd_assignment.ipynb # Main notebook (all steps)
├── screenshots/
│   └── data_loading/              # Screenshots of notebook execution
├── requirements.txt
├── .gitignore
└── README.md
```

## ⚙️ Requirements

- Python 3.9+
- Java 8/11 (required by Spark)
- Packages listed in `requirements.txt`:
  ```
  pyspark==3.5.0
  delta-spark==3.1.0
  pandas>=2.0.0
  ```

Install with:
```bash
pip install -r requirements.txt
```

> 💡 You can also run this notebook directly on **Databricks Community Edition** or **Google Colab** — both have easy Delta Lake support.

## ▶️ How to Run

1. Clone the repo:
   ```bash
   git clone https://github.com/<your-username>/delta-lake-assignment.git
   cd delta-lake-assignment
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Launch Jupyter:
   ```bash
   jupyter notebook notebooks/delta_scd_assignment.ipynb
   ```
4. Run all cells top to bottom (**Cell → Run All**).

## 🧠 Explanation

This notebook demonstrates a complete incremental data pipeline using Delta Lake:

1. **Load** — `data/customer_master.csv` is loaded and written into a Delta table — this becomes our base dataset.
2. **Clean** — Duplicate rows are removed and null values (`email`, `tier`) are filled in, producing a clean baseline table.
3. **Simulate incremental data** — `data/customer_incremental.csv` represents new data arriving later: some rows **update** existing customers (changed city/tier/email), others are **brand-new** customers.
4. **MERGE** — Delta Lake's `MERGE INTO` matches source and target on `customer_id`:
   - Match found → row is **updated**
   - No match → row is **inserted**
5. **Validate** — Row count vs. distinct `customer_id` count confirms there are no duplicate keys after merging.
6. **Final output** — Merged table is displayed with a summary of how many rows were updated vs. inserted.

**Why this matters:** Rewriting an entire dataset every time new data arrives is wasteful and error-prone. Delta Lake's `MERGE` gives us an atomic, ACID-compliant upsert — the same pattern used in real-world CDC (change-data-capture) and slowly-changing-dimension (SCD) pipelines. As a bonus, Delta's transaction log provides free versioning ("time travel"), so every incremental load is auditable via `DeltaTable.history()`.

## 📊 Sample Data

**`customer_master.csv`** (base dataset, includes 1 duplicate row + 2 nulls to clean):

| customer_id | name    | email               | city    | tier   |
|-------------|---------|---------------------|---------|--------|
| 1           | Alice   | alice@example.com   | Jaipur  | *(null)* |
| 2           | Bob     | bob@example.com     | Mumbai  | Gold   |
| 3           | Charlie | *(null)*            | Delhi   | Silver |
| 4           | David   | david@example.com   | Chennai | Gold   |
| 5           | Eva     | eva@example.com     | Jaipur  | Silver |
| 5           | Eva     | eva@example.com     | Jaipur  | Silver | *(duplicate)* |

**`customer_incremental.csv`** (2 updates + 2 new customers):

| customer_id | name  | email                   | city      | tier     | type    |
|-------------|-------|-------------------------|-----------|----------|---------|
| 2           | Bob   | bob@example.com         | Pune      | Platinum | update  |
| 4           | David | david.new@example.com   | Chennai   | Gold     | update  |
| 6           | Farah | farah@example.com       | Bengaluru | Gold     | insert  |
| 7           | Gopal | gopal@example.com       | Kolkata   | Standard | insert  |

## ✅ Output (Expected Results When You Run the Notebook)

**Step 1 — Raw master data loaded (6 rows):**
```
 customer_id    name             email    city   tier
           1   Alice alice@example.com  Jaipur    NaN
           2     Bob   bob@example.com  Mumbai   Gold
           3 Charlie               NaN   Delhi Silver
           4   David david@example.com Chennai   Gold
           5     Eva   eva@example.com  Jaipur Silver
           5     Eva   eva@example.com  Jaipur Silver
```

**Step 2 — After cleaning (duplicate dropped, nulls filled) → 5 rows:**
```
 customer_id    name               email    city     tier
           1   Alice   alice@example.com  Jaipur Standard
           2     Bob     bob@example.com  Mumbai     Gold
           3 Charlie unknown@example.com   Delhi   Silver
           4   David   david@example.com Chennai     Gold
           5     Eva     eva@example.com  Jaipur   Silver
```

**Step 3 — Incremental dataset (4 rows: 2 updates, 2 new):**
```
 customer_id  name                 email      city     tier
           2   Bob       bob@example.com      Pune Platinum
           4 David david.new@example.com   Chennai     Gold
           6 Farah     farah@example.com Bengaluru     Gold
           7 Gopal     gopal@example.com   Kolkata Standard
```

**Step 4 — After MERGE (7 rows: 5 original + 2 inserted, 2 updated in place):**
```
 customer_id    name                 email      city     tier
           1   Alice     alice@example.com    Jaipur Standard
           2     Bob       bob@example.com      Pune Platinum   ← updated
           3 Charlie   unknown@example.com     Delhi   Silver
           4   David david.new@example.com   Chennai     Gold   ← updated
           5     Eva       eva@example.com    Jaipur   Silver
           6   Farah     farah@example.com Bengaluru     Gold   ← inserted
           7   Gopal     gopal@example.com   Kolkata Standard   ← inserted
```

**Step 5 — Validation:**
```
Total rows after MERGE : 7
Distinct customer_ids  : 7
Duplicates present     : False
```

**Step 6 — Summary:**
```
Rows in cleaned master dataset : 5
Rows in incremental dataset    : 4
Rows in final merged table     : 7
Updated existing records       : 2  (customer_id 2, 4)
Newly inserted records         : 2  (customer_id 6, 7)
```

## 🕒 Bonus: Time Travel

Delta Lake automatically versions every write. You can inspect table history with:
```python
delta_table.history().select("version", "timestamp", "operation").show(truncate=False)
```

## 📝 License

This project is for educational/assignment purposes.
