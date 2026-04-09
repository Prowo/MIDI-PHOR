import duckdb
con = duckdb.connect(r'data/musiccap.duckdb')
try:
    rows = con.execute("select song_id, coalesce(title,'') as title from songs order by song_id").fetchall()
    if not rows:
        print('NO_ROWS')
    else:
        for sid, title in rows:
            print(f"{sid}\t{title}")
except Exception as e:
    print('ERR', e)
