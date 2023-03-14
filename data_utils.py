import statistics
import DBcm
from records import records
import platform

where = platform.uname().release.find("aws")

if where == -1:
    # Local.
    config = {
        "host": "127.0.0.1",
        "database": "swimdataDB",
        "user": "swimuser",
        "password": "swimpasswd",
    }
else:
    # Not on PA.
    config = {
        "user": "AlexDarigan",
        "password": "FooBar!!",
        "host": "AlexDarigan.mysql.pythonanywhere-services.com",
        "database": "AlexDarigan$default",
    }

def populate():
    with DBcm.UseDatabase("mydata.sqlite3") as db:
        TableSQL = f"""
                    create table if not exists {table} (
                        event varchar(32) not null,
                        time varchar(16) not null 
                    )        
        """
        db.execute(TableSQL)

    for table in records.keys():
        with DBcm.UseDatabase(config) as db:
            TableSQL = f"""
                    create table if not exists {table} (
                        event varchar(32) not null,
                        time varchar(16) not null 
                    )        
            """
            db.execute(TableSQL)
            SQL = f"""
                        insert into {table} 
                        (event, time)
                        values 
                        ( %s, %s )
            """
            data = [row for row in records[table].items()]
            db.executemany(SQL, data)


def get_world_records(event):
    lcmen = records["LCMen"][event]
    lcwomen = records["LCWomen"][event]
    scmen = records["SCMen"][event]
    scwomen = records["SCWomen"][event]
    return lcmen, lcwomen, scmen, scwomen

def get_swimmer_data(name, the_session):
    SQL = """  
        select distinct strokes.distance, strokes.stroke, swimmers.age
        from swimmers, strokes, times
        where times.swimmer_id = swimmers.id and
        times.stroke_id = strokes.id and
        swimmers.name = %s and
        date_format(times.ts, "%Y-%m-%d") = %s
    """
    with DBcm.UseDatabase(config) as db:
        db.execute(
            SQL,
            (
                name,
                the_session,
            ),
        )
        results = db.fetchall()  # a list of tuples.
    return results


def get_chart_data(name, age, event, the_session):
    distance, stroke = event.split("-")
    SQL = """
        select swimmers.name, swimmers.age, times.time, strokes.distance, strokes.stroke, times.ts
        from swimmers, strokes, times
        where (swimmers.name = %s and swimmers.age = %s) and
        (strokes.distance = %s and strokes.stroke = %s) and 
        swimmers.id = times.swimmer_id and
        strokes.id = times.stroke_id and
        date_format(times.ts, "%Y-%m-%d") = %s
    """
    with DBcm.UseDatabase(config) as db:
        db.execute(
            SQL,
            (
                name,
                age,
                distance,
                stroke,
                the_session,
            ),
        )
        results = db.fetchall()
    times = [item[2] for item in results]
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
    return average_str, times, converts


def get_list_of_sessions():
    SQL = "select distinct ts from times"
    with DBcm.UseDatabase(config) as db:
        db.execute(SQL)
        results = db.fetchall()
    return results


def get_swimmers_list_by_session(the_session):
    SQL = """
        select distinct swimmers.name   
        from times, swimmers 
        where date_format(times.ts, "%Y-%m-%d") = %s and     
        times.swimmer_id = swimmers.id 
        order by name ;
    """
    with DBcm.UseDatabase(config) as db:
        db.execute(SQL, (the_session,))
        results = db.fetchall()
        return [row[0] for row in results]
