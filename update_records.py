import gazpacho
import pprint

URL = "https://en.wikipedia.org/wiki/List_of_world_records_in_swimming"
RECORDS = (0, 1, 3, 4)
COURSES = ("LCMen", "LCWomen", "SCMen", "SCWomen")
WHERE = "/home/swimmingcoach/webapp/"

html = gazpacho.get(URL)
soup = gazpacho.Soup(html)
tables = soup.find("table")

records = {}  # This is the "outer" dictionary.

for table, course in zip(RECORDS, COURSES):
    records[course] = {}  # The "inner" dictionary (nested inside "outer").
    for row in tables[table].find("tr")[1:]:
        cols = row.find("td")
        records[course][cols[0].text] = cols[1].text

with open(WHERE + "records.py", "w", encoding="utf-8") as wf:
    print("records = ", end="", file=wf)
    pprint.pprint(records, stream=wf)
