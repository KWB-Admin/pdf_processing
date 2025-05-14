from img2table.document import PDF
from img2table.ocr import TesseractOCR
import os
import pandas as pd
import pypdf
from typing import Union
from datetime import datetime
from itertools import compress

columns = [
    "lab_analyzed_date",
    "lab_analyzed_time",
    "analyte",
    "result",
    "max_contam_limit",
    "units",
    "min_detectable_limit",
]

columns_for_saving = [
    "report_number",
    "date_added",
    "state_well_number",
    "sample_date",
    "sample_time",
    "lab_analyzed_date",
    "lab_analyzed_time",
    "analyte",
    "result",
    "units",
    "max_contam_limit",
    "min_detectable_limit",
]


def get_files_to_read(folder: str) -> list:
    files_to_read = []
    for root, dir, files in os.walk(folder):
        if not files:
            continue
        for file in files:
            if not dir:
                file_path = f"{root}/{file}"
            else:
                file_path = f"{root}/{dir}/{file}"
            files_to_read.append(file_path)
    return files_to_read


def get_well_name(page):
    lines = page.extract_text().split("\n")
    well_names = []
    for line in lines:
        if "30S/24E" in line:
            well_names.append(line)
        elif "30S/25E" in line:
            well_names.append(line)
        elif "30S/26E" in line:
            well_names.append(line)
        else:
            continue
    if len(well_names) == 1:
        if " " in well_names[0]:
            return well_names[0].split(" ")[1]
        else:
            return well_names[0]
    else:
        corrected_names = []
        for well_name in well_names:
            well_name = well_name.split(" ")[1] if " " in well_name else well_name
            corrected_names.append(well_name)
        return corrected_names


def get_table_from_pdf(file, page_num: int, ocr):
    pdf = PDF(
        f"{file}", pages=[page_num], detect_rotation=False, pdf_text_extraction=True
    )
    return pdf.extract_tables(
        ocr=ocr, implicit_rows=True, implicit_columns=True, borderless_tables=True
    )


def table_transformation(
    columns: list,
    file: str,
    table_df: pd.DataFrame,
    well_name: Union[str, list],
    old_well_name: str,
    sample_date: str,
    sample_time: str,
) -> pd.DataFrame:
    if table_df.shape[1] == 8:
        table_df.drop(columns=[3], inplace=True)
    table_df.columns = columns
    if isinstance(well_name, list):
        locs = [
            table_df.index[table_df["analyte"].str.contains(well)].to_list()[0]
            for well in well_name
        ]
        if locs[0] > 1:
            table_df.loc[0 : locs[0], "state_well_number"] = old_well_name
        table_df.loc[locs[0] : locs[1], "state_well_number"] = well_name[0]
        table_df.loc[locs[1] :, "state_well_number"] = well_name[1]
    else:
        ind = table_df.index[table_df["analyte"].str.contains(well_name)].to_list()
        if not ind:
            table_df["state_well_number"] = well_name
        elif ind[0] > 1:
            table_df.loc[: ind[0], "state_well_number"] = old_well_name
            table_df.loc[ind[0] :, "state_well_number"] = well_name
        else:
            table_df["state_well_number"] = well_name
    table_df["report_number"] = file.split("Lab Reports 2021-2023/Eurofins/")[1]
    table_df["lab"] = "EuroFins"
    table_df["date_added"] = datetime.today()
    table_df = table_df[table_df["lab_analyzed_date"] != "Analyzed"]
    table_df["sample_date"] = sample_date
    table_df["sample_time"] = sample_time
    return table_df.dropna(
        subset=["lab_analyzed_date", "lab_analyzed_time"]
    ).reset_index(drop=True)


def extract_data_from_pdfs(columns, files_to_read, ocr):
    tables = []
    sample_times = []
    sample_dates = []
    old_well_name = "None"
    for file in files_to_read:
        sample_date = None
        reader = pypdf.PdfReader(file)
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            text = page.extract_text()
            if sample_date:
                pass
            else:
                if "Sample Date" in text:
                    text_with_sample_date = (
                        text.split("Sample Date\n")[1].splitlines()[0].split(" ")
                    )
                    sample_date = text_with_sample_date[1]
                    sample_time = text_with_sample_date[2]
                    sample_time = "%s:%s" % (sample_time[:2], sample_time[-2:])
            if "Laboratory Hits" not in text:
                continue
            table = get_table_from_pdf(file, page_num, ocr)
            if not table[page_num]:
                continue
            table_df = table[page_num][0].df
            well_name = get_well_name(page)
            well_name = old_well_name if not well_name else well_name
            table_df = table_transformation(
                columns,
                file,
                table_df,
                well_name,
                old_well_name,
                sample_date,
                sample_time,
            )
            tables.append(table_df)
            old_well_name = well_name[-1] if isinstance(well_name, list) else well_name
    print(sample_times)
    pd.concat(tables).reset_index(drop=True)[columns_for_saving].to_csv(
        "processed_eurofins_data.csv"
    )


if __name__ == "__main__":

    folder = "Lab Reports 2021-2023/Eurofins"

    files_to_read = get_files_to_read(folder)
    ocr = TesseractOCR(n_threads=1, lang="eng", psm=6)
    extract_data_from_pdfs(columns, files_to_read, ocr)
