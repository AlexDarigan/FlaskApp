from flask import Flask, session, render_template, request

import os
import data_utils


app = Flask(__name__)  # Creates a web server which can run your Flask code.and
app.secret_key = (
    "kjhfdskjhfsdghk;odgkjsdkjsdkgsdgjdsghdjgdghdfghdfgdfghfggdf;uaghdfgagf"
)
app.config["SESSION_TYPE"] = "filessystem"

@app.get("/")  # The @ symbol is a Python decorator.
def homepage():
    # Setting session data on page load (refresh page to update)
    session_dates = [
        row[0].isoformat().split("T")[0] for row in data_utils.get_list_of_sessions()
    ]
    return render_template(
        "index.html",
        title="Welcome to Swimclub",
        session_data=sorted(set(session_dates), reverse=True),
    )


# def get_data():
#     session.clear()  # Just being safe.
#     keys = ["age", "distance", "stroke", "average", "average_str", "times", "converts"]
#     session["swimmers"] = {}  # Empty dictionary.
#     files = os.listdir(swimclub.FOLDER)
#     files.remove(".DS_Store")
#     for file in files:  # Process each file one at a time.
#         name, *the_rest, _times, _converts = swimclub.get_swim_data(
#             file
#         )  # Get the data.
#         if name not in session["swimmers"]:
#             session["swimmers"][name] = []
#         session["swimmers"][name].append({k: v for k, v in zip(keys, the_rest)})
#         session["swimmers"][name][-1]["file"] = file


@app.post("/swimmers")
def get_swimmers_names():
    the_session = request.form["session"]
    session["the_session"] = the_session  # Let's remember this value.
    names = data_utils.get_swimmers_list_by_session(the_session)
    return render_template(
        "select.html",
        data=sorted(names),
        title="Swimmer",
        select_id="swimmer",
        url="/showevents",
        target="#showevents",
    )


@app.post("/showevents")
# @app.get("/showevents")
def show_swimmer_files():
    name = request.form["swimmer"]

    data = data_utils.get_swimmer_data(
        name, session["the_session"]
    )  # A list of three-tuples.
    events = [t[0] + "-" + t[1] for t in data]  # A list of events.
    ages = list(set([t[-1] for t in data]))  # A list of unique ages.

    session["swimmer"] = name
    session["age"] = ages[0]  # The first age from the unique list.

    return render_template(
        "select.html",
        data=events,
        title="Event",
        select_id="event",
        url="/showchart",
        target="#showchart",
    )


def convert2range(v, f_min, f_max, t_min, t_max):
    """Given a value (v) in the range f_min-f_max, convert the value
    to its equivalent value in the range t_min-t_max.

    Based on the technique described here:
        http://james-ramsden.com/map-a-value-from-one-number-scale-to-another-formula-and-c-code/
    """
    return round(t_min + (t_max - t_min) * ((v - f_min) / (f_max - f_min)), 2)


conversion = {
    "Fly": "butterfly",
    "Back": "backstroke",
    "Breast": "breaststroke",
    "Free": "freestyle",
    "IM": "individual medley",
}


@app.post("/showchart")
def show_event_chart():
    event = request.form["event"]
    distance, stroke = event.split("-")
    the_stroke = conversion[stroke]
    the_event = f"{distance} {the_stroke}"

    lcmen, lcwomen, scmen, scwomen = data_utils.get_world_records(the_event)

    the_session = session["the_session"]

    swimmer = session["swimmer"]
    age = session["age"]
    average_str, times, converts = data_utils.get_chart_data(
        swimmer, age, event, the_session
    )
    the_converts = [convert2range(c, 0, max(converts), 0, 350) for c in converts]
    chart_data = list(zip(times, the_converts))

    the_title = f"{swimmer} (Under {age}) {event} - {the_session}"

    return render_template(
        "chart.html",
        title=the_title,
        data=chart_data,
        average=average_str,
        worlds=[lcmen, lcwomen, scmen, scwomen],
    )


if __name__ == "__main__":
    app.run(debug=True)  # Starts the web server, and keeps going...
