import statistics

from records import records


FOLDER = "swimdata/"


def get_swim_data(fn):
    swimmer, age, distance, stroke = fn.removesuffix(".txt").split("-")

    with open(FOLDER + fn) as df:
        data = df.readlines()
    times = data[0].strip().split(",")
    converts = []  # This is an empty list.
    for t in times:
        if ":" in t:
            mins, the_rest = t.split(":")
            secs, hundredths = the_rest.split(".")  # Darius.
        else:
            mins = 0
            secs, hundredths = t.split(".")  # Dave.
        converts.append((int(mins) * 60 * 100) + (int(secs) * 100) + int(hundredths))
    average = statistics.mean(converts)
    mins = int((average // 60) // 100)
    remainder = average - (mins * 60 * 100)
    secs = int((remainder // 100))
    hundredths = int(round(remainder - (secs * 100), 0))
    average_str = f"{mins}:{secs}.{hundredths}"
    return swimmer, age, distance, stroke, average, average_str, times, converts


def convert2range(v, f_min, f_max, t_min, t_max):
    """Given a value (v) in the range f_min-f_max, convert the value
    to its equivalent value in the range t_min-t_max.

    Based on the technique described here:
        http://james-ramsden.com/map-a-value-from-one-number-scale-to-another-formula-and-c-code/
    """
    return round(t_min + (t_max - t_min) * ((v - f_min) / (f_max - f_min)), 2)


CHARTS = "charts/"


def produce_bar_chart(fn, location=CHARTS):

    conversion = {
        "Fly": "butterfly",
        "Back": "backstroke",
        "Breast": "breaststroke",
        "Free": "freestyle",
        "IM": "individual medley",
    }

    (
        swimmer,
        age,
        distance,
        stroke,
        average,
        average_str,
        times,
        converts,
    ) = get_swim_data(fn)

    title = f"{swimmer} (Under {age}) {distance} {stroke}"

    header = f""" 
    <!DOCTYPE html>
    <html>
        <head>
            <title>
                {title}
            </title>
            <link rel="stylesheet" href="/static/webapp.css">
        </head>
        <body>
            <h3>{title}</h3>
    """

    body = ""
    for c, t in zip(converts, times):
        bar_width = convert2range(c, 0, max(converts), 0, 350)
        svg = f"""   
                <svg height="30" width="400">
                    <rect height="30" width="{ bar_width }" style="fill:rgb(0,0,255);" />
                </svg>{ t }<br />
        """
        body = svg + body

    stroke = conversion[fn.removesuffix(".txt").split("-")[-1]]
    distance = fn.removesuffix(".txt").split("-")[-2]
    event = f"{distance} {stroke}"

    lcmen = records["LCMen"][event]
    lcwomen = records["LCWomen"][event]
    scmen = records["SCMen"][event]
    scwomen = records["SCWomen"][event]

    footer = f""" 
            <p>Average time: {average_str}</p>
            M: {lcmen} ({scmen})<br />W: {lcwomen} ({scwomen})
        </body>
    </html>
    """

    page = header + body + footer
    save_to = f"{location}{fn.removesuffix('.txt')}.html"
    with open(save_to, "w") as wf:
        print(page, file=wf)
    return save_to
