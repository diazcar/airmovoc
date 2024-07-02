import pandas as pd
import os
import pathlib
import datetime as dt
import calendar

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

    last_day_of_month = calendar.monthrange(year, month)[1]
    end_date = dt.datetime(
        year,
        month,
        last_day_of_month
        )
    data = data[day_before_start_date:end_date]
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

FACTEURS = pd.read_csv("conversion_factors.csv")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        """_summary_
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
        default="./test_data",
        metavar="\b",
    )

    parser.add_argument(
        "-o", "--output",
        type=str,
        help="Output directory for concatenated files",
        default="./output",
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

    xair_data = pd.DataFrame()

    for year in years:

        indir = f"{args.input}/{year}"

        months = get_months(
            year_data_dir=indir,
            month=args.month
            )

        for month in months:
            month_data = pd.DataFrame()
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

                    data = pd.DataFrame()

                    asc_data = pd.read_table(file)
                    asc_data['Sampling date'] = pd.to_datetime(
                        asc_data['Sampling date']
                        ).dt.round('15min')
                    asc_data.set_index('Sampling date', inplace=True)

                    if "CAL60" in str(file):
                        asc_data = asc_data.shift(periods=-1, freq='15min')

                    data = pd.concat([data, asc_data])
                    data.sort_index(inplace=True)
                    data = fill_quarts(
                        data=data,
                        type_appareil=type_de_appareil
                        )
                    data = data[~data.index.duplicated(keep='first')]

                month_data = pd.concat(
                    [month_data, data],
                    axis=1
                    )
                month_data = month_data.sort_index()

            xair_data = pd.concat(
                [xair_data, month_data],
                axis=0
                )

            xair_data = filter_month(
                data=xair_data,
                year=year,
                month=2
                )

        xair_data.to_csv(f'./output/xair_{year}.csv')
