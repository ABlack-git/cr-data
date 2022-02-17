import pandas as pd
import os


def validate_region_positions(df: pd.DataFrame, region_names: dict, path):
    for i, name in region_names.items():
        region = df.iloc[i, 0]
        if name not in region:
            raise ValueError(f"Encountered wrong value on row {i} in file {path}. Expected {name}, got {region}")


def add_region_col(df: pd.DataFrame, region_names: dict):
    region_data = []
    region_names_iter = iter(region_names)
    last_row = 217

    prev_row = next(region_names_iter)
    prev_region = region_names[prev_row]

    for current_row in region_names_iter:
        num_rows = current_row - prev_row
        region_data.extend([prev_region] * num_rows)

        prev_row = current_row
        prev_region = region_names[current_row]

    num_rows = last_row - prev_row + 1
    region_data.extend([prev_region] * num_rows)

    df['region'] = region_data


def read_excel_file(path: str):
    df = pd.read_excel(path, skiprows=7, header=None)
    col_names = ('code_of_unit', 'unit_name', 'population_total', 'population_males', 'population_females',
                 'avg_age_total', 'avg_age_males', 'avg_age_females')
    df.columns = col_names
    return df


def delete_region_rows(df, region_names: dict):
    return df.drop(list(region_names.keys()))


def add_year_column(df, year):
    df['date'] = [year] * len(df.index)
    df['date'] = pd.to_datetime(df['date'])


def drop_empty_rows(df: pd.DataFrame):
    return df.dropna(subset=['code_of_unit']).reset_index(drop=True)


def process_excel(path, date):
    region_names = {0: 'Středočeský kraj', 27: 'Jihočeský kraj', 45: 'Plzeňský kraj', 61: 'Karlovarský kraj',
                    69: 'Ústecký kraj', 86: 'Liberecký kraj', 97: 'Královéhradecký kraj',
                    113: 'Pardubický kraj', 129: 'Vysočina', 145: 'Jihomoravský kraj', 167: 'Olomoucký kraj',
                    181: 'Zlínský kraj', 195: 'Moravskoslezský kraj'}
    df = read_excel_file(path)
    df = drop_empty_rows(df)

    validate_region_positions(df, region_names, path)
    add_region_col(df, region_names)
    df = delete_region_rows(df, region_names)
    add_year_column(df, date)
    return df


def main():
    base_dir = 'raw_data/'
    df_list = []
    for path in os.listdir(base_dir):
        date = os.path.basename(path).split(".")[0]
        date = date[2:].replace("-", '/')
        df = process_excel(base_dir + path, date)
        df_list.append(df)

    final_df = pd.concat(df_list).sort_values(['date', 'region'], ascending=[True, False])
    final_df.to_csv("population_by_regions.csv", index=False)


if __name__ == '__main__':
    main()
