from flask import Flask, jsonify
from datetime import datetime, timedelta
from sqlalchemy import create_engine, func, inspect, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.automap import automap_base

# Database Setup
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
metadata = MetaData(bind=engine)
metadata.reflect()
Base = automap_base(metadata=metadata)
Base.prepare()
Measurement = Base.classes.measurement
Station = Base.classes.station

# Flask Setup
app = Flask(__name__)
session = scoped_session(sessionmaker(bind=engine))

@app.teardown_appcontext
def remove_session(exception=None):
    session.remove()
#Welcome page route and table of contents
@app.route('/')
def welcome():
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date<br/>"
        f"/api/v1.0/start_date/end_date"
    )
#Precipitation route code
@app.route("/api/v1.0/precipitation")
def precipitation():
    try:
        #Find the most recent date in the data set per Precipitation and Station database creation and queries captured in separate Jupyter notebook (and which kicked off this track of work).
        most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
        if most_recent_date:
            most_recent_date = most_recent_date[0]

            #Calculate the date one year from the last date in data set
            one_year_before = datetime.strptime(most_recent_date, '%Y-%m-%d') - timedelta(days=365)

            #Perform a query to retrieve the date and precipitation scores
            results = session.query(Measurement.date, Measurement.prcp).\
                      filter(Measurement.date >= one_year_before).\
                      order_by(Measurement.date.asc()).all()

            precip = {date: prcp for date, prcp in results}
            return jsonify(precip)
        else:
            return jsonify({"error": "No data found in the database"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
#Stations route per Precipitation and Station database creation and queries.
@app.route("/api/v1.0/stations")
def stations():
    try:
        results = session.query(Station.station).all()
        stations = [result.station for result in results]
        return jsonify(stations)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
#TOBS route per assignment request using same database and query structure per above routes.
@app.route("/api/v1.0/tobs")
def tobs():
    try:
        #Find the most recent date in the data set.
        most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
        if most_recent_date:
            most_recent_date = most_recent_date[0]
            one_year_before = datetime.strptime(most_recent_date, '%Y-%m-%d') - timedelta(days=365)

            #Find the most active station
            most_active_station = session.query(Measurement.station, func.count(Measurement.station)).\
                                  group_by(Measurement.station).\
                                  order_by(func.count(Measurement.station).desc()).\
                                  first().station

            #Perform a query to retrieve the date and temperatures for the most active station
            results = session.query(Measurement.date, Measurement.tobs).\
                      filter(Measurement.station == most_active_station).\
                      filter(Measurement.date >= one_year_before).\
                      order_by(Measurement.date.asc()).all()

            tobs_data = [{date: tobs} for date, tobs in results]
            return jsonify(tobs_data)
        else:
            return jsonify({"error": "No data found in the database"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

#Start route code: Copy base URL + modify by adding a specified "Start date" in order to generate requested analysis. Warning: Failure to put in properly formatted data into URL string (ex: http://127.0.0.1:5001/api/v1.0/2017-08-22) will result in a "entry does not match date format" error.
@app.route("/api/v1.0/<start>")
def temp_stats_start(start):
    try:
        #Log the received start date
        print(f"Received start date: {start}")

        #Attempt to parse the start date from the URL
        start_date = datetime.strptime(start, '%Y-%m-%d')
        
        #Log the successfully parsed date
        print(f"Parsed start date: {start_date}")

        #Format for the database
        db_start_date = start_date.strftime('%Y-%m-%d')  # Adjust format as needed

        #Query using the database date format
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs))\
                  .filter(Measurement.date >= db_start_date)\
                  .all()

        #Check if results are found
        if not results:
            return jsonify({"error": "No temperature data found for the provided date."}), 404

        Tmin, Tavg, Tmax = results[0]
        return jsonify({
            "TMIN": Tmin,
            "TAVG": round(Tavg, 2),
            "TMAX": Tmax
        })

    except ValueError as ve:
        return jsonify({"error": f"Invalid date format received: '{start}'. Expected format YYYY-MM-DD. Details: {str(ve)}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

#Start/end date route code: Copy base URL + modify by adding a specified "Start date" and end date in order to generate requested analysis. Warning: Failure to put in properly formatted data into URL string (ex: http://127.0.0.1:5001/api/v1.0/2017-02-23/2017-08-23) will result in a "entry does not match date format" error.
@app.route("/api/v1.0/<start>/<end>")
def temp_stats_start_end(start, end):
    session = scoped_session(sessionmaker(bind=engine))  # Make sure to use your configured session
    try:
        #Convert start and end date from string to datetime objects
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')

        #Query to find the min, average, and max temperatures
        results = session.query(func.min(Measurement.tobs), 
                                func.avg(Measurement.tobs), 
                                func.max(Measurement.tobs))\
                          .filter(Measurement.date >= start_date)\
                          .filter(Measurement.date <= end_date)\
                          .one()  # Using one() to expect a single result tuple

        Tmin, Tavg, Tmax = results
        if Tmin is None or Tavg is None or Tmax is None:
            return jsonify({"error": "No temperature data found for the provided range."}), 404

        temp_data = {
            "TMIN": Tmin,
            "TAVG": round(Tavg, 2) if Tavg is not None else None,  # Safely rounding Tavg
            "TMAX": Tmax
        }
        return jsonify(temp_data)

    except ValueError as ve:
        return jsonify({"error": f"Invalid date format: {str(ve)}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        session.remove()  # Properly close session

if __name__ == "__main__":
    app.run(debug=True, port=5001)