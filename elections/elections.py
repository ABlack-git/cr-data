import requests
import pandas as pd

from simpledbf import Dbf5
from bs4 import BeautifulSoup
from dataclasses import dataclass

YEARS = (2006, 2010, 2013, 2017)
BASE_URL = "https://volby.cz/pls/ps{}/vysledky_okres?nuts={}"
PARTIES_BASE_PATH = "raw_data/party-register-{}/PSRKL.{}"
MAIN_CODELIST_PATH = '../municipality_codes/cr-structure-codes.csv'
COELIST_2006_PATH = 'raw_data/codelists/2006/CNUMNUTS.DBF'
MUNICIPALITY_NAMES_MAP = {
    "Frenštát pod Radhoštěm": "Frenštát p.Radhoštěm",
    "Brandýs nad Labem-Stará Boleslav": "Brandýs n.L.-St.Bol.",
    "Nové Město nad Metují": "Nové Město n.Metují",
    "Bystřice nad Pernštejnem": "Bystřice nad Pernšt.",
    "Bystřice pod Hostýnem": "Bystřice p.Hostýnem",
    "Dvůr Králové nad Labem": "Dvůr Králové n.Labem",
    "Frýdlant nad Ostravicí": "Frýdlant n.Ostravicí"
}
POHORELICE_DISTRICT_ID = "CZ0624"
BIG_CITIES = ["Brno", "Ostrava", "Plzeň"]


@dataclass
class ElectionRecord:
    year: int
    municipality_id: int
    municipality_name: str
    vote_percentage: float
    party_id: int
    party_name: str
    party_name_short: str


@dataclass
class PartyInfo:
    party_id: int
    party_name: str
    party_name_short: str


class CodelistRegister:
    def __init__(self, main_codelist_path, codelist_2006_path):
        self.main_codelist = pd.read_csv(main_codelist_path)
        self.codelist_2006_path = codelist_2006_path
        self.codelist_map = {
            2006: self._init_2006_codelist(),
            2010: self.main_codelist,
            2013: self.main_codelist,
            2017: self.main_codelist
        }

    def _init_2006_codelist(self):
        codelist_2006 = Dbf5(self.codelist_2006_path, codec="cp852").to_dataframe()
        merged = pd.merge(self.main_codelist, codelist_2006, left_on="distric", right_on="NAZEVNUTS")
        merged["municipality"] = merged["municipality"].replace(MUNICIPALITY_NAMES_MAP)
        merged = merged.drop(columns=["district_id", "NAZEVNUTS", "NUMNUTS"]).rename(columns={"NUTS": "district_id"})
        merged.loc[merged['municipality'] == "Pohořelice", "district_id"] = POHORELICE_DISTRICT_ID
        return merged

    def get_district_ids(self, year):
        return self.codelist_map[year]["district_id"].unique()

    def get_municipalities_by_district_id(self, year, district_id):
        return self.codelist_map[year].loc[self.codelist_map[year]['district_id'] == district_id] \
                   .loc[:, ['municipality_id', 'municipality']].values.tolist()


class PartyRegister:
    def __init__(self, base_path):
        self.base_path = base_path
        self.type_map = {
            2006: {'type': 'dbf', 'encoding': 'cp852'},
            2010: {'type': 'dbf', 'encoding': 'cp852'},
            2013: {'type': 'dbf', 'encoding': 'cp852'},
            2017: {'type': 'csv', 'encoding': 'Windows-1250'},
        }
        self.party_map = self._prepare_party_register()

    def _prepare_party_register(self):
        parties = {}
        for year in YEARS:
            if self.type_map[year]['type'] == 'dbf':
                party_register = self._read_dbf(self.base_path.format(year, 'dbf'), self.type_map[year]['encoding'])
            else:
                party_register = self._read_csv(self.base_path.format(year, 'csv'), self.type_map[year]['encoding'])
            parties[year] = party_register
        return parties

    def _read_dbf(self, path, encoding):
        return Dbf5(path, codec=encoding).to_dataframe()

    def _read_csv(self, path, encoding):
        return pd.read_csv(path, encoding=encoding, sep=';')

    def get_party_info(self, year, k_party):
        df = self.party_map[year]
        line = df.loc[df['KSTRANA'] == k_party].iloc[0]
        return PartyInfo(line['VSTRANA'], line['NAZEV_STRK'], line['ZKRATKAK8'])


def get_election_data(year, district_id):
    content = requests.get(BASE_URL.format(year, district_id)).content
    return BeautifulSoup(content, 'xml')


def get_results_for_municipalities(bs, year, municipalities, party_register: PartyRegister):
    records = []
    municipality_names = {name: m_id for m_id, name in municipalities}
    municipality_names_set = set(municipality_names.keys())
    found_municipalities = set()

    municipalities = bs.find_all('OBEC', {'NAZ_OBEC': municipality_names_set})
    municipality_name = None
    for i, name in enumerate(BIG_CITIES):
        if name in municipality_names_set:
            municipalities = bs.find_all('OKRES')
            municipality_name = BIG_CITIES[i]
            break

    for municipality in municipalities:
        if municipality_name is None:
            municipality_name = municipality.get('NAZ_OBEC')
        found_municipalities.add(municipality_name)

        for vote in municipality.find_all('HLASY_STRANA'):
            k_party = int(vote.get('KSTRANA'))
            percentage = float(vote.get('PROC_HLASU'))
            party_info: PartyInfo = party_register.get_party_info(year, k_party)
            record = ElectionRecord(year, municipality_names[municipality_name], municipality_name, percentage,
                                    party_info.party_id, party_info.party_name, party_info.party_name_short)
            records.append(record)
        municipality_name = None
    if len(found_municipalities) != len(municipality_names_set):
        print(f"Could not found following municipalities {municipality_names_set.difference(found_municipalities)}")
    return records


def get_records(year, district_id, party_register: PartyRegister, codelist_register: CodelistRegister):
    election_data = get_election_data(year, district_id)
    municipalities = codelist_register.get_municipalities_by_district_id(year, district_id)
    return get_results_for_municipalities(election_data, year, municipalities, party_register)


def main():
    election_records = []
    party_register = PartyRegister(PARTIES_BASE_PATH)
    codelist_register = CodelistRegister(MAIN_CODELIST_PATH, COELIST_2006_PATH)
    for year in YEARS:
        for district_id in codelist_register.get_district_ids(year):
            print(f"year: {year}, district_id: {district_id}")
            records = get_records(year, district_id, party_register, codelist_register)
            election_records.extend(records)

    df = pd.DataFrame(election_records)
    df.to_csv('elections2.csv', index=False, encoding='utf8')


main()
