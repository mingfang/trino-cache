import marimo

__generated_with = "0.13.10"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(r"""# Trino Cache""")
    return


@app.cell
def _():
    import sqlalchemy
    import pandas as pd
    import datacompy
    import marimo as mo
    from datetime import datetime

    return datetime, mo, pd, sqlalchemy


@app.cell
def _(sqlalchemy):
    trino_tpch = sqlalchemy.create_engine(f"trino://trino@trino.trino-example:8080/tpch")
    return (trino_tpch,)


@app.cell
def _(sqlalchemy):
    trino_postgres = sqlalchemy.create_engine(f"trino://trino@trino.trino-example:8080/postgres")
    return (trino_postgres,)


@app.cell
def _(sqlalchemy):
    postgres = sqlalchemy.create_engine(f"postgresql://postgres:postgres@postgres.trino-example:5432/postgres")
    return (postgres,)


@app.cell
def _():
    foreign_server = 'foreign_server'
    reset = False
    return foreign_server, reset


@app.cell
def _(mo, postgres):
    _ = mo.sql(
        f"""
        CREATE EXTENSION IF NOT EXISTS postgres_fdw;
        CREATE EXTENSION IF NOT EXISTS dblink;
        CREATE SCHEMA IF NOT EXISTS cache;
        """,
        engine=postgres
    )
    return


@app.cell
def _(foreign_server, mo, postgres, reset):
    if reset:
        mo.sql(f"""
            DROP SERVER {foreign_server} CASCADE;
        """, 
        engine=postgres)

    mo.sql(f"""
        CREATE SERVER IF NOT EXISTS {foreign_server}
                FOREIGN DATA WRAPPER postgres_fdw
                OPTIONS (host 'postgres', port '5432', dbname 'postgres', sslmode 'allow');
        CREATE USER MAPPING IF NOT EXISTS FOR postgres
                SERVER foreign_server
                OPTIONS (user 'postgres', password 'postgres');
        """, engine=postgres)
    return


@app.cell(disabled=True)
def _(_pg_foreign_data_wrappers, information_schema, mo, postgres):
    _ = mo.sql(
        f"""
        SELECT * FROM information_schema._pg_foreign_data_wrappers
        """,
        output=False,
        engine=postgres
    )
    return


@app.cell(disabled=True)
def _(information_schema, mo, postgres, user_mappings):
    _ = mo.sql(
        f"""
        SELECT * FROM information_schema.user_mappings
        """,
        output=False,
        engine=postgres
    )
    return


@app.cell
def _(mo):
    mo.md(r"""---""")
    return


@app.cell
def _(mo):
    refresh = mo.ui.refresh(label='Refresh', options=["1s", "1m", '5m', '10m', '30m'], default_interval="5m")
    refresh
    return (refresh,)


@app.cell
def _(mo, pd):
    mo.output.append(mo.md("##Sources"))

    sources = pd.read_csv('./trino-cache/sources.csv')

    mo.output.append(sources)
    return (sources,)


@app.cell
def _(
    datetime,
    diffs,
    foreign_server,
    mo,
    postgres,
    refresh,
    reset,
    sources,
    trino_tpch,
):
    refresh
    diffs

    for _table in sources.itertuples():
        _df = mo.sql(f"""
            SELECT column_name, data_type 
            FROM {_table.catalog}.information_schema.columns 
            WHERE table_schema = '{_table.schema}' AND table_name = '{_table.table_name}'
            """, engine=trino_tpch, output=False)
        _column_names = ", ".join(_df["column_name"])
        _data_types = ", ".join(_df["column_name"] + " " + _df["data_type"])

        if reset:
            mo.sql(f"""
                DROP MATERIALIZED VIEW IF EXISTS cache.{_table.table_name};
                """, engine=postgres)

        _sql = f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS cache.{_table.table_name} AS
            SELECT *
            FROM dblink('{foreign_server}',$SQL$
                SELECT row_number() OVER () as __id, {_column_names}
                FROM {_table.schema}.{_table.table_name}
                {f'WHERE {_table.where}' if _table.where else ''}
            $SQL$) AS t1 (__id bigint, {_data_types});
            """
        # print(_sql)
        mo.sql(_sql, engine=postgres)

        if reset:
            mo.sql(f"""
                CREATE UNIQUE INDEX ON cache.{_table.table_name} (__id);
                """, engine=postgres)

        mo.sql(f"""
            REFRESH MATERIALIZED VIEW CONCURRENTLY cache.{_table.table_name};
            """, engine=postgres)
        mo.output.append(mo.md(f"""## Refreshed {_table.catalog}.{_table.schema}.{_table.table_name} - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"""))    

    return


@app.cell
def _(mo):
    mo.md(r"""---""")
    return


@app.cell
def _(mo):
    compare = mo.ui.refresh(label='Compare', options=["1s", "1m", '5m', '10m', '30m'], default_interval="1m")
    compare
    return (compare,)


@app.cell
def _(compare, datetime, mo, refresh, sources, trino_postgres, trino_tpch):
    refresh
    compare

    mo.output.append(mo.md(f"""## Comparing sources vs cache - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"""))

    diffs = 0
    for _table in sources.itertuples():
        _df = mo.sql(f"""
            SELECT column_name, data_type 
            FROM {_table.catalog}.information_schema.columns 
            WHERE table_schema = '{_table.schema}' AND table_name = '{_table.table_name}'
            """, engine=trino_tpch, output=False)
        _column_names = ", ".join(_df["column_name"])
        _data_types = ", ".join(_df["column_name"] + " " + _df["data_type"])

        _df1 = mo.sql(f"""
            SELECT {_column_names}
            FROM {_table.schema}.{_table.table_name}
            {f'WHERE {_table.where}' if _table.where else ''}
        
            EXCEPT
        
            SELECT {_column_names}
            FROM cache.{_table.table_name}
            {f'WHERE {_table.where}' if _table.where else ''}
        """, engine=trino_postgres, output=False).to_pandas()

        diffs = len(_df1.index)
    
        mo.output.append(mo.md(f"""    
        <details>
        <summary>{_table.catalog}.{_table.schema}.{_table.table_name} - Passed: {diffs < 1}</summary>
        <pre>
        {_df1.to_markdown(index=True)}
        </pre>
        </details>
        """))
        mo.output.append(mo.md(f"""## Total Diffs: {diffs} - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"""))

    return (diffs,)


if __name__ == "__main__":
    app.run()
