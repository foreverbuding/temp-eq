import os
from flask import Flask, jsonify
import sqlalchemy as sa
from flask_cors import CORS
# web app
app = Flask(__name__)
CORS(app)

# database engine
SQL_URI = sa.engine.URL.create(
    drivername='postgresql',
    username=os.getenv('PGUSER', ''),
    password=os.getenv('PGPASSWORD', ''),
    host=os.getenv('PGHOST', ''),
    database=os.getenv('PGDATABASE', ''),
    port=os.getenv('PGPORT', 5432),
)
engine = sa.create_engine(SQL_URI)


@app.route('/')
def index():
    return 'Welcome to EQ Works 😎'


@app.route('/search/<string:keyword>', methods=['GET'])
def search(keyword):
    results = engine.execute(f'''
            SELECT *
            FROM public.poi 
            WHERE name like '%%{keyword}%%'
        ''')
    datas = []
    for r in results:
        datas.append({
            "poi_id": r[0],
            "name": r[1],
            "lat": r[2],
            "lon": r[3]
        })
    return jsonify(datas)

@app.route('/events/hourly/<string:poi_id>/<string:start_date>/<string:end_date>')
def events_hourly(poi_id, start_date, end_date):
    results = engine.execute(f'''
        SELECT date, hour, events
        FROM public.hourly_events
        WHERE poi_id={poi_id} 
        AND date >= '{start_date}'
        AND date <= '{end_date}'
        ORDER BY date, hour
    ''')
    datas = []
    for r in results:
        datas.append({
            "date": r[0],
            "hour": r[1],
            "events": r[2]
        })
    return jsonify(datas)


@app.route('/events/daily/<string:poi_id>/<string:start_date>/<string:end_date>')
def events_daily(poi_id, start_date, end_date):
    results = engine.execute(f'''
            SELECT date, SUM(events) AS events
            FROM public.hourly_events
            WHERE poi_id={poi_id} 
            AND date >= '{start_date}'
            AND date <= '{end_date}'
            GROUP BY date
        ''')
    datas = []
    for r in results:
        datas.append({
            "date": r[0],
            "events": r[1]
        })
    return jsonify(datas)


@app.route('/stats/hourly/<string:poi_id>/<string:start_date>/<string:end_date>')
def stats_hourly(poi_id, start_date, end_date):
    results = engine.execute(f'''
                SELECT date, hour, impressions, clicks, revenue
                FROM public.hourly_stats
                WHERE poi_id={poi_id} 
                AND date >= '{start_date}'
                AND date <= '{end_date}'
                ORDER BY date, hour
            ''')
    datas = []
    for r in results:
        datas.append({
            "date": r[0],
            "hour": r[1],
            "impressions": r[2],
            "clicks": r[3],
            "revenue": r[4]
        })
    return jsonify(datas)


@app.route('/stats/daily/<string:poi_id>/<string:start_date>/<string:end_date>')
def stats_daily(poi_id, start_date, end_date):
    results = engine.execute(f'''
                    SELECT date,
                        SUM(impressions) AS impressions,
                        SUM(clicks) AS clicks,
                        SUM(revenue) AS revenue
                    FROM public.hourly_stats
                    WHERE poi_id={poi_id} 
                    AND date >= '{start_date}'
                    AND date <= '{end_date}'
                    GROUP BY date
                ''')
    datas = []
    for r in results:
        datas.append({
            "impressions": r[0],
            "clicks": r[1],
            "revenue": r[2]
        })
    return jsonify(datas)


@app.route('/poi')
def poi():
    results = engine.execute('''
        SELECT *
        FROM public.poi;
    ''')
    datas = []
    for r in results:
        datas.append({
            "poi_id": r[0],
            "name": r[1],
            "lat": r[2],
            "lon": r[3]
        })
    return jsonify(datas)

def query_helper(query):
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return jsonify([dict(row.items()) for row in result])

