# Notebook Integration

[notebooks/hatrie_sql_analysis.ipynb](notebooks/hatrie_sql_analysis.ipynb)
is a reproducible Jupyter/Python workflow for the monitoring SQL API. It uses
the NDJSON streaming response, preserves the received rows in a Pandas frame,
and writes a local Parquet artifact through `pyarrow`.

Install the Python dependencies in an isolated environment, start an
authenticated monitoring server, then launch Jupyter with only the endpoint and
token in the environment:

```sh
python -m pip install jupyter pandas pyarrow requests
HATRIE_URL=http://127.0.0.1:8080 \
HATRIE_CACHE_AUTH_TOKEN="$HATRIE_CACHE_AUTH_TOKEN" \
jupyter lab notebooks/hatrie_sql_analysis.ipynb
```

The notebook never embeds credentials. Change the SQL string in its final cell
to a query supported by [SQL.md](SQL.md); keep `stream: true` for bounded client
memory and retain the generated Parquet artifact with the analysis source.
