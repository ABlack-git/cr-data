import pandas as pd

YEARS = {
    "2006": ["2007", "2008", "2009"],
    "2010": ["2011", "2012"],
    "2013": ["2014", "2015", "2016"],
    "2017": ["2018", "2019", "2020"]

}
elections = pd.read_csv("election_with_coalition.csv")
elections["is_election_year"] = 1
new_records = []
for rec in elections.to_dict("records"):
    new_records.append(rec)
    for year in YEARS[str(rec['year'])]:
        new_rec = rec.copy()
        new_rec["year"] = year
        new_rec["is_election_year"] = 0
        new_records.append(new_rec)

extended_election = pd.DataFrame(new_records)
extended_election.to_csv("extended_elections.csv")
