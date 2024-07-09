import pandas as pd
import os
import pathlib
import datetime as dt
import argparse


def fill_quarts(
        data: pd.DataFrame,
        type_appareil: str,
) -> pd.DataFrame:
    if type_appareil == 'C2-C6':

        shift_data = data.shift(freq='15min')
        data = pd.concat([data, shift_data], axis=0)

    if type_appareil == 'C6-C20':

        shift_data = data.shift(freq='15min')
        data = pd.concat([data, shift_data], axis=0)

        for i in range(2):

            shift_data = shift_data.shift(freq='15min')
            data = pd.concat([data, shift_data], axis=0)

    return (data.sort_index())


def filter_month(
        data: pd.DataFrame,
        year: int,
        month: int,
        day: int = 1,
):

    start_date = dt.datetime(year, month, day)
    day_before_start_date = start_date - dt.timedelta(days=1)

    if month != 12:
        month = month + 1

    data = data[data.index > day_before_start_date]
    return (data)


def test_directories(
        indir: str,
        outdir: str,
):
    if os.path.isdir(indir) is not True:
        exit(
            " ".join(
                f"Can not find {indir},",
                "please check if folder",
                "exists and path syntax."
                )
        )
    if os.path.isdir(outdir) is not True:
        exit(
            " ".join(
                f"Can not find {outdir},",
                "please check if folder",
                "exists and path syntax."
                )
        )


def get_years(
        year: str,
        data_dir: str,
):
    if year is None:
        years = [int(x) for x in os.listdir(data_dir)]
        if years is []:
            exit(f"No years folders in directory : {data_dir}")

    return (years)


def get_months(
    year_data_dir: str,
    month: str,
):
    if month is None:
        months = [int(x) for x in os.listdir(year_data_dir)]
        if months is []:
            exit("No months to process...")
    else:
        months = month

    return (months)


APPAREILS = [
        'C2-C6',
        'C6-C20',
    ]

if os.path.isfile("./facteurs_de_conversions_C2-C20.csv"):
    FACTEURS = pd.read_csv("./facteurs_de_conversions_C2-C20.csv")
else:
    exit("./facteurs_de_conversions_C2-C20.csv not found.")


def apply_conversion(
        data: pd.DataFrame,
) -> pd.DataFrame:
    cols = data.drop(['Volume'], axis=1).columns
    for head in cols:
        poll = FACTEURS[FACTEURS['Composé'] == head]
        x_facteur = float(poll["Facteur de conversion"].values[0])
        data[head] = data[head].apply(
            lambda x:
                x*x_facteur
            )
    return (data)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        """
        This script concatenate asc files from COV measurements equipment
        (C2-C6 and C6-C20)

        use as: python airmovoc.py -i ./path/to/in/folder -o ./path/to/out/folder

        - Place chronological data as folder/years/months : ex. data/2022/01.
        - The script will look up in path/folder the years and months and will create a csv file per year.

        options:
        """,
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "-m", "--month",
        type=int,
        help="Month to process",
        default=None,
        metavar="\b",
    )

    parser.add_argument(
        "-y", "--year",
        type=int,
        help="year to process",
        default=None,
        metavar="\b",
    )

    parser.add_argument(
        "-i", "--input",
        type=str,
        help="Path to the data to process",
        default="./data",
        metavar="\b",
    )

    parser.add_argument(
        "-o", "--output",
        type=str,
        help="Output directory for concatenated files",
        default="./",
        metavar="\b",
    )

    args = parser.parse_args()

    test_directories(
        indir=args.input,
        outdir=args.output,
    )

    years = get_years(
        year=args.year,
        data_dir=args.input,
    )

    for year in years:
        xair_data = pd.DataFrame()
        indir = f"{args.input}/{year}"

        months = get_months(
            year_data_dir=indir,
            month=args.month
            )

        xair_data = pd.DataFrame()
        for month in months:
            data = pd.DataFrame()
            for type_de_appareil in APPAREILS:

                search_string = "".join([
                    f"{indir}",
                    f"/{str(month).zfill(2)}",
                    f"/*{type_de_appareil}.Asc"
                ])

                file_directories = [
                    f for f in pathlib.Path().glob(search_string)]
                if file_directories == []:
                    exit(f"No files in {indir}/{str(month).zfill(2)}/")

                for file in file_directories:

                    asc_data = pd.read_table(file, on_bad_lines='skip')
                    asc_data['Sampling date'] = pd.to_datetime(
                        asc_data['Sampling date']
                        ).dt.round('15min')
                    asc_data.set_index('Sampling date', inplace=True)

                    asc_data = asc_data.drop(
                        list(asc_data.filter(regex='Unnamed').columns),
                        axis=1,
                        )

                    asc_data = apply_conversion(
                        data=asc_data
                        )

                    if "CAL60" in str(file):
                        asc_data = asc_data.shift(periods=-1, freq='30min')

                    data = pd.concat([data, asc_data])
                    data = filter_month(
                        data=data,
                        year=year,
                        month=month
                        )

                data.sort_index(inplace=True)

                data = data[~data.index.duplicated(keep='first')]

                data = fill_quarts(
                    data=data,
                    type_appareil=type_de_appareil
                    )

            xair_data = pd.concat([xair_data, data], axis=0)

        xair_data.to_csv(f'{args.output}/xair_{year}.csv')
