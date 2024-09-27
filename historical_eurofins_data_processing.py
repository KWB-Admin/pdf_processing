from img2table.document import PDF
from img2table.ocr import TesseractOCR
import os
import pandas as pd
import pypdf
from typing import Union


def get_files_to_read(folder: str) -> list:
    files_to_read = []
    for root, dir, files in os.walk(folder):
        if not files:
            continue
        for file in files:
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


def get_table_from_pdf(file, page_num: int):
    ocr = TesseractOCR(n_threads=1, lang="eng", psm=6)
    pdf = PDF(
        f"{file}", pages=[page_num], detect_rotation=False, pdf_text_extraction=True
    )
    return pdf.extract_tables(
        ocr=ocr, implicit_rows=True, implicit_columns=True, borderless_tables=True
    )


def table_transformation(
    columns: list, file: str, table_df: pd.DataFrame, well_name: Union[str, list]
) -> pd.DataFrame:
    if table_df.shape[1] == 8:
        table_df.drop(columns=[3], inplace=True)
    table_df.columns = columns
    if isinstance(well_name, list):
        locs = [
            table_df.index[table_df["analyte"].str.contains(well)].to_list()[0]
            for well in well_name
        ]
        table_df.loc[locs[0] : locs[1], "well_name"] = well_name[0]
        table_df.loc[locs[1] :, "well_name"] = well_name[1]
    else:
        ind = table_df.index[table_df["analyte"].str.contains(well_name)].to_list()
        if not ind:
            table_df["well_name"] = well_name
        elif ind[0] > 1:
            table_df.loc[: ind[0], "well_name"] = old_well_name
            table_df.loc[ind[0] :, "well_name"] = well_name
        else:
            table_df["well_name"] = well_name
    table_df["file"] = file
    table_df["lab"] = "BSK" if "BSK" in file else "Eurofins"
    return table_df.dropna(subset=["date", "time"])


if __name__ == "__main__":
    columns = ["date", "time", "analyte", "result", "federal_mcl", "units", "mrl"]

    folder = "Lab Reports 2021-2023/Eurofins"

    files_to_read = get_files_to_read(folder)

    tables = []

    for file in files_to_read:
        reader = pypdf.PdfReader(file)
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            if "Laboratory Hits" not in page.extract_text():
                continue
            table = get_table_from_pdf(file, page_num)
            if not table[page_num]:
                continue
            table_df = table[page_num][0].df
            well_name = get_well_name(page)
            well_name = old_well_name if not well_name else well_name
            table_df = table_transformation(columns, file, table_df, well_name)
            tables.append(table_df)
            old_well_name = well_name[-1] if isinstance(well_name, list) else well_name

    columns_for_saving = [
        "well_name",
        "date",
        "analyte",
        "result",
        "units",
        "federal_mcl",
        "mrl",
    ]
    full_data = pd.concat(tables).reset_index(drop=True)[columns_for_saving]
