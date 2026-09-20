# Screenshots

After running the pipeline locally (see main README setup steps), add screenshots here:

1. `dag_graph_view.png` — Airflow UI, Graph view of `ecommerce_etl_pipeline` showing all 4 tasks green.
2. `dag_run_success.png` — Airflow UI, Grid view showing a successful daily run.
3. `task_logs_transform.png` — Logs of the `transform_orders` task showing the PySpark row counts (raw vs. cleaned).
4. `warehouse_query_result.png` — `psql` or a SQL client showing `SELECT * FROM fact_orders LIMIT 10;` after a successful load.

Optional: record a short demo GIF (e.g. with [Kap](https://getkap.co/) or [ScreenToGif](https://www.screentogif.com/)) of triggering the DAG in the Airflow UI end-to-end and name it `demo.gif`.

Once added, reference them in the main `README.md` like:

```markdown
![DAG Graph View](screenshots/dag_graph_view.png)
```
