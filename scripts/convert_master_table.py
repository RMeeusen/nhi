import pandas as pd
from pathlib import Path
import jinja2

# %% Define templates
INDEX_CONTENT = jinja2.Template(
    "Introductie\n"
    "###########\n"
    "\n"
    "Deze webpagina documenteert alle gereedschappen \n"
    "binnen de NHI gereedschapskist.\n"
    "\n"
    ".. image:: _static/NHI-logo.svg\n"
    "\n"

)

TEMPLATE_CONTENT = jinja2.Template(
    "\n"
    "{{gereedschap}}\n"
    "{% set underline = '#' * gereedschap|length %}"
    "{{underline}}\n"
    "\n"
    "Beschrijving\n"
    "============"
    "\n"
    "{{beschrijving}}\n"
    "\n"
    "Specificaties\n"
    "=============\n"
    "\n"
    ".. csv-table:: \n"
    "   :file: {{csv_file}}\n"
    "   :stub-columns: 1\n"
    "\n"
    "{% if toctree is not none %}"
    "Gereedschappen\n"
    "==============\n"
    "{{toctree}}"
    "{% endif %}"
)

TOCTEMPLATE = jinja2.Template(
    "\n\n"
    ".. toctree:: \n"
    "{% if hidden is true %}"
    "   :hidden:\n"
    "{% endif %}"
    "{% if numbered is true %}"
    "   :numbered:\n"
    "{% endif %}"
    "   :maxdepth: 1\n\n"
    "{% for tool in tools %}"
    "   {{tool}}\n"
    "{% endfor %}"
)

# %% Read
script_dir = Path(__file__).parent

df = pd.read_csv(script_dir / "NHI-gereedschapskist_inventarisatie.csv", sep=';')

# %% Fill nans
# Ffill group name to convert hierarchy into long table format.
df["Gereedschapskist"] = df["Gereedschapskist"].fillna(method="ffill")

df["Gereedschap"] = df["Gereedschap"].fillna("index")
df["Beschrijving"] = df["Beschrijving"].fillna("Geen beschrijving beschikbaar")

# %% Loop over toolboxes and write

kisten = df["Gereedschapskist"].unique()

exclude_in_csv = ["Gereedschapskist", "Gereedschap", "Beschrijving"]
col_select = [colname for colname in df.columns if colname not in exclude_in_csv]

# %% Write index file
toolboxes = [f"./kisten/{kist}/index" for kist in kisten]
index_text = INDEX_CONTENT.render() + TOCTEMPLATE.render(tools = toolboxes, numbered=False, hidden=True)

index_file = script_dir / ".." / "docs" / "index.rst"

with open(index_file, mode="w") as f:
    f.write(index_text)

# %% Write other rst files
folder = script_dir / ".." / "docs" / "kisten"

for kist in kisten:
    kist_folder = folder / kist
    kist_folder.mkdir(exist_ok=True, parents=True)

    df_kist = df.loc[df["Gereedschapskist"] == kist]
    tools = df_kist["Gereedschap"]
    for tool in df_kist["Gereedschap"]:
        beschrijving = df_kist.loc[tools == tool, "Beschrijving"].values[0]
        
        # Prepare dataframe for csv
        df_csv = df_kist.loc[tools == tool, col_select]
        # Drop indexes and transpose
        df_csv = df_csv.reset_index(drop=True).T[0]
        # Set name, will be header in table
        df_csv = df_csv.rename("Specificatie")

        # Prepare filenames
        file_stem = kist_folder / tool
        csv_file = file_stem.with_suffix(".csv")
        rst_file = file_stem.with_suffix(".rst")

        # Rst string
        if tool == "index":
            toolname = kist
        else:
            toolname = tool
        
        if (tool == "index") & (tools.size > 1):
            toctools = [i for i in tools.values if i != "index"]
            toctree = TOCTEMPLATE.render(tools=toctools, hidden=False, numbered=True)
        else:
            toctree = None

        rst_text = TEMPLATE_CONTENT.render(
            beschrijving=beschrijving, toctree=toctree, csv_file=csv_file.name, gereedschap=toolname,
            )

        # Write
        df_csv.to_csv(csv_file, sep=",")

        with open(rst_file, mode="w") as f:
            f.write(rst_text)

 
# %%
