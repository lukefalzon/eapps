import os
import pandas as pd


"""Determine file path"""
path = os.path.dirname(os.path.abspath("eapps_processing.ipynb"))
print(path)


"""Create dataframes from new and previous results"""

raw_path = rf"{path}\raw.csv"
processed_path = rf"{path}\processed.csv"

raw_df = pd.read_csv(raw_path)
processed_df = pd.read_csv(processed_path)


raw_df = raw_df.dropna(
    subset=["case_number", "architect", "applicant", "description"]
)


# Remove "Dr" and "Perit" and clean up whitespace
raw_df["architect"] = (
    raw_df["architect"].str.replace(r"\s*Dr\s*|\s*Perit\s*", "", regex=True).str.strip()
)


def reverse_names(name):
    exemptions = [
        "X,Y,Z Architecture & Design",
        "Mangion, Mangion & Partners",
        """Nois currently responsible for this case. X,Y,Z Architecture & Design relinquished responsibility on 20 July 2020""",
        "Innovative Design Architects (iDA) 23, Triq Titu B",
    ]
    if name not in exemptions:
        parts = name.split(",")
        if len(parts) == 2:
            return f"{parts[1].strip()} {parts[0].strip()}"
        else:
            return name


raw_df["architect"] = raw_df["architect"].apply(reverse_names)

raw_df["applicant"] = raw_df["applicant"].str.replace(".*Attn: ", "", regex=True)
raw_df["applicant"] = raw_df["applicant"].str.replace(".*Attn. ", "", regex=True)
raw_df["applicant"] = raw_df["applicant"].str.replace(".*Attn ", "", regex=True)
raw_df["applicant"] = raw_df["applicant"].str.replace(
    ".*represented by ", "", regex=True
)
raw_df["applicant"] = raw_df["applicant"].str.replace(" obo .*", "", regex=True)
raw_df["applicant"] = raw_df["applicant"].str.replace(" o.b.o .*", "", regex=True)
raw_df["applicant"] = raw_df["applicant"].str.replace(" o.b.o. .*", "", regex=True)
raw_df["applicant"] = raw_df["applicant"].str.replace(" o.b.o. .*", "", regex=True)
raw_df["applicant"] = raw_df["applicant"].str.replace(r"\s*\(o\.b\.o.*", "", regex=True)
raw_df["applicant"] = raw_df["applicant"].str.replace(
    r"^\s*(?:Mr\.|Mrs\.|Mr |Ms\.|Ms |Dr\.|Dr |Ing\.|Ing |Fr\.|Fr|Rev\.|Rev\. )\s*",
    "",
    regex=True,
)


raw_df["applicant"] = raw_df["applicant"].apply(
    lambda x: x.title() if isinstance(x, str) else x
)


raw_df[["application_type", "application_number", "application_year"]] = raw_df[
    "case_number"
].str.split("/", expand=True)


columns_to_process = ["description", "applicant", "architect", "location"]
raw_df[columns_to_process] = raw_df[columns_to_process].applymap(
    lambda x: x.replace("â€™", "'") if isinstance(x, str) else x
)
raw_df[columns_to_process] = raw_df[columns_to_process].applymap(
    lambda x: x.replace("\u2019", "'") if isinstance(x, str) else x
)


# Concatenate the dataframes
concatenated_df = pd.concat([processed_df, raw_df], ignore_index=True)

# Keep only unique rows based on the 'Name' column
unique_df = concatenated_df.drop_duplicates(subset="case_number", keep="last")

unique_df = unique_df.sort_values(
    by=["application_year", "application_type", "application_number"],
    ascending=[True, True, True],
)

unique_df.to_csv(processed_path, encoding="utf-8", index=False)
