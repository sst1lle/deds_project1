```markdown
# DEDS Project 1

Dit project is een sjabloon voor een PDM-pakket, ontworpen om gegevens te beheren en te analyseren met behulp van de meegeleverde databases.

## Inhoud

- **Databases**: De map `databases/` bevat verschillende datasets, waaronder CSV-bestanden uit de AdventureWorks-database.
- Creëer in SSMS een db genaamd: NorthWind en maak een nieuwe query aan en plak daarin het NorthWind.txt bestand
- **Broncode**: Het bestand `sourceDataModel.ipynb` bevat een Jupyter Notebook voor gegevensmodellering en analyse.
- **Configuratie**: Het project maakt gebruik van PDM voor pakketbeheer en Python 3.13.

## Vereisten

- Python 3.13.*
- PDM (Python Dependency Manager)

## Installatie

### 1. Clone de repository

```bash
git clone <repository-url>
cd deds_project1
```

### 2. Installeer de afhankelijkheden

```bash
pip install pdm  # of pdm install
pdm init
```

### 3. Installeer benodigde packages voor datamanipulatie

```bash
pip install pandas pyodbc
```

## Gebruik

Start het Jupyter Notebook om met de analyse te beginnen:

```bash
jupyter notebook sourceDataModel.ipynb
```

