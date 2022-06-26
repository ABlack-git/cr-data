import requests
import pandas as pd

from simpledbf import Dbf5
from bs4 import BeautifulSoup
from dataclasses import dataclass

years = (2006, 2010, 2013, 2017)
base_url = "https://volby.cz/pls/ps{}/vysledky_okres?nuts={}"
parties_base_path = "raw_data/party-register-{}/PSRKL.{}"

cr_structure = pd.read_csv('../municipality_codes/cr-structure-codes.csv')
district_ids = cr_structure['district_id'].unique()


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
        for year in years:
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


def get_municipalities_by_district(district_id):
    return cr_structure.loc[cr_structure['district_id'] == district_id] \
               .loc[:, ['municipality_id', 'municipality']].values.tolist()


def get_election_data(year, district_id):
    content = requests.get(base_url.format(year, district_id)).content
    return BeautifulSoup(content, 'xml')


def get_results_for_municipalities(bs, year, municipalities, party_register: PartyRegister):
    records = []
    municipality_names = {name: m_id for m_id, name in municipalities}
    for municipality in bs.find_all('OBEC', {'NAZ_OBEC': list(municipality_names.keys())}):
        municipality_name = municipality.get('NAZ_OBEC')
        for vote in municipality.find_all('HLASY_STRANA'):
            k_party = int(vote.get('KSTRANA'))
            percentage = float(vote.get('PROC_HLASU'))
            party_info: PartyInfo = party_register.get_party_info(year, k_party)
            record = ElectionRecord(year, municipality_names[municipality_name], municipality_name, percentage,
                                    party_info.party_id, party_info.party_name, party_info.party_name_short)
            records.append(record)

    return records


def get_records(year, district_id, party_register: PartyRegister):
    election_data = get_election_data(year, district_id)
    municipalities = get_municipalities_by_district(district_id)
    return get_results_for_municipalities(election_data, year, municipalities, party_register)


def main():
    election_records = []
    party_register = PartyRegister(parties_base_path)
    for year in years:
        for district_id in district_ids:
            records = get_records(year, district_id, party_register)
            election_records.extend(records)

    df = pd.DataFrame(election_records)
    df.to_csv('elections.csv', index=False, encoding='utf8')


main()
