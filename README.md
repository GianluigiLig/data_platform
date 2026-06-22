# Pacchetto `data_platform`

`data_platform` è un pacchetto Python generico per costruire pipeline dati organizzate secondo architettura **medallion**: `bronze`, `silver` e `gold`.

Il pacchetto non contiene logica specifica di dominio. Non deve conoscere nomi di API applicative, regole di business, calcoli specifici, mapping di dispositivi, clienti o casi d'uso verticali. Il suo ruolo è fornire una base tecnica riutilizzabile per:

- leggere e scrivere dati su uno storage a oggetti;
- costruire path coerenti su data lake;
- separare i layer `bronze`, `silver` e `gold`;
- orchestrare pipeline generiche delegando la logica specifica ad adapter e processor esterni;
- gestire finestre temporali e timezone;
- produrre report tecnici di qualità dati;
- centralizzare logging ed eccezioni comuni.

La regola principale è:

```text
data_platform = infrastruttura dati generica
pacchetto applicativo = logica di dominio specifica
```

In altre parole, `data_platform` non deve diventare il punto in cui inserire regole legate a una sorgente reale, a un cliente o a un settore specifico. Deve rimanere uno strato comune sopra cui progetti diversi possono costruire le proprie pipeline.

---

## Obiettivo del pacchetto

Il pacchetto nasce per evitare che ogni progetto implementi da zero gli stessi blocchi tecnici:

- upload e download da storage;
- serializzazione JSON, JSON compresso e parquet;
- costruzione manuale dei path del data lake;
- gestione delle date di esecuzione;
- orchestrazione ripetuta delle fasi `bronze`, `silver` e `gold`;
- gestione frammentata di errori, log e controlli base di qualità.

Un progetto che usa `data_platform` dovrebbe concentrarsi solo su ciò che è specifico del proprio contesto:

- come interrogare una sorgente esterna;
- quali richieste eseguire nella fase bronze;
- come trasformare i payload raw in dataframe normalizzati;
- come costruire i dataset finali del layer gold.

`data_platform` fornisce quindi una base comune, stabile e riutilizzabile.

---

## Struttura del pacchetto

```text
data_platform/
├── datalake/
│   ├── __init__.py
│   ├── lake.py
│   └── storage.py
├── pipelines/
│   ├── __init__.py
│   ├── adapters.py
│   └── medallion.py
├── utils/
│   ├── __init__.py
│   ├── diagnostics.py
│   ├── quality.py
│   └── time_utils.py
├── __init__.py
└── README.md
```

La struttura è divisa per responsabilità:

| Area | Responsabilità principale |
|---|---|
| `datalake/` | Storage fisico e costruzione dei path logici del data lake |
| `pipelines/` | Contratti generici e orchestrazione bronze/silver/gold |
| `utils/` | Utility comuni per date, qualità dati, logging ed eccezioni |

---

## Architettura generale

Il pacchetto separa quattro livelli:

```text
1. Storage fisico
   ↓
2. Data lake logico
   ↓
3. Pipeline medallion generiche
   ↓
4. Adapter / processor del progetto chiamante
```

In pratica:

- `ObjectStorage` sa leggere e scrivere bytes;
- `DataLake` usa `ObjectStorage` e offre metodi più comodi per JSON, JSON gzip e parquet;
- `BronzePipeline`, `SilverPipeline` e `GoldPipeline` usano `DataLake`;
- la logica specifica viene iniettata tramite `DataSource`, `SourceAdapter` e `GoldProcessor`.

Questa separazione consente di riutilizzare la stessa infrastruttura tecnica con sorgenti, domini e output diversi.

---

## Flusso medallion

Il flusso previsto è il seguente:

```text
Sorgente esterna
      ↓
BronzePipeline
      ↓
Payload raw salvati su data lake
      ↓
SilverPipeline
      ↓
Dataset normalizzati in parquet
      ↓
GoldPipeline
      ↓
Dataset finali, aggregati o arricchiti
```

### Bronze

Il layer bronze contiene dati raw, cioè dati il più possibile vicini alla risposta originale della sorgente.

La pipeline bronze:

1. chiede all'adapter quali richieste eseguire;
2. usa il client sorgente per recuperare i dati;
3. salva ogni payload come `.json.gz` sul data lake;
4. restituisce statistiche su elementi processati, salvati, vuoti o falliti.

La bronze non deve normalizzare, aggregare o interpretare i dati. Deve preservare il contenuto originario in modo tracciabile.

### Silver

Il layer silver contiene dati normalizzati e più facili da usare da altri processi.

La pipeline silver:

1. legge i payload bronze già salvati;
2. passa i payload all'adapter;
3. riceve un `DataFrame` pandas normalizzato;
4. salva il risultato in formato parquet.

La silver non conosce le regole specifiche del dominio: la trasformazione raw → dataframe è responsabilità dell'adapter del progetto chiamante.

### Gold

Il layer gold contiene dataset finali, arricchiti, aggregati o pronti per l'uso da parte di processi successivi.

La pipeline gold:

1. chiede al processor quali input leggere;
2. carica i dataframe necessari;
3. delega al processor la costruzione del dataset finale;
4. salva il risultato in parquet.

La gold è volutamente generica: legge input, chiama un processor e salva l'output. La logica di business resta fuori dal pacchetto base.

---

# Moduli del pacchetto

## `datalake/storage.py`

Questo modulo contiene il livello più basso di I/O verso lo storage.

### Classi principali

| Oggetto | Descrizione |
|---|---|
| `ObjectStorage` | Interfaccia astratta minima per uno storage a oggetti |
| `LocalObjectStorage` | Implementazione basata su filesystem locale |
| `GCSObjectStorage` | Implementazione per Google Cloud Storage |

### Funzioni principali

| Funzione | Descrizione |
|---|---|
| `upload_json_gz` | Serializza un dizionario in JSON, lo comprime con gzip e lo carica sullo storage |
| `upload_json` | Serializza un dizionario in JSON e lo carica sullo storage |
| `read_json_gz` | Legge un file `.json.gz`, lo decomprime e restituisce un dizionario Python |
| `read_json` | Legge un JSON dallo storage e restituisce un dizionario Python |
| `upload_parquet` | Scrive un `DataFrame` pandas in formato parquet |
| `read_parquet` | Legge un parquet e restituisce un `DataFrame` pandas |

### Responsabilità

`storage.py` gestisce aspetti fisici e tecnici:

- upload di bytes;
- download di bytes;
- verifica dell'esistenza di un oggetto;
- serializzazione e deserializzazione;
- conversione da e verso JSON, JSON gzip e parquet;
- gestione degli errori tecnici di storage.

Non decide dove salvare i file dal punto di vista logico. La costruzione dei path bronze/silver/gold è responsabilità di `lake.py`.

### Uso locale

`LocalObjectStorage` è utile per test locali, dry-run e debug senza dipendere da un bucket remoto.

```python
from data_platform.datalake.storage import LocalObjectStorage

storage = LocalObjectStorage(root="./local_lake")
```

In questo caso i file vengono scritti sul filesystem locale.

### Uso con Google Cloud Storage

`GCSObjectStorage` è pensato per ambienti che scrivono su Google Cloud Storage.

Il client GCS viene iniettato dall'esterno:

```python
from google.cloud import storage as gcs_storage
from data_platform.datalake.storage import GCSObjectStorage

client = gcs_storage.Client()
storage = GCSObjectStorage(client=client)
```

Questa scelta mantiene il pacchetto più testabile: il modulo resta importabile anche quando la creazione del client viene gestita altrove, per esempio da un orchestratore, da credenziali di ambiente o da un servizio applicativo.

---

## `datalake/lake.py`

Questo modulo contiene il livello logico del data lake.

### Oggetti principali

| Oggetto | Descrizione |
|---|---|
| `DEFAULT_LAKE_ROOT` | Root di default del data lake |
| `MedallionLayer` | Enum dei layer `BRONZE`, `SILVER`, `GOLD` |
| `DatasetRef` | Dataclass che descrive un dataset nel data lake |
| `build_medallion_prefix` | Funzione per costruire prefissi medallion coerenti |
| `BasePathsConfig` | Configurazione base per costruire path ricorrenti |
| `DataLake` | Facade ad alto livello per leggere e scrivere oggetti dati |

### Convenzione dei path medallion

I path vengono costruiti con una struttura standard e parametrica:

```text
{root}/source={source}/{layer}/{asset_partition_name}={asset}/dataset={dataset}/year=YYYY/month=MM/day=DD/filename
```

Esempio bronze/silver, usando una partizione generica `asset`:

```text
data-platform/source=example_api/bronze/asset=asset_001/year=2026/month=06/day=21/raw.json.gz
data-platform/source=example_api/silver/asset=asset_001/year=2026/month=06/day=21/measurements.parquet
```

Esempio gold:

```text
data-platform/source=example_api/gold/asset=asset_001/year=2026/month=06/final_dataset.parquet
```

Nel layer gold il path arriva fino a `year/month`, mentre bronze e silver includono anche il `day`.

### Nota sulla partizione dell'asset

Il concetto funzionale del pacchetto è `asset`, cioè l'entità su cui viene eseguita la pipeline.

Nel codice attuale il parametro della partizione è configurabile tramite `asset_partition_name`. Il valore di default è ancora:

```python
asset_partition_name: str = "plant"
```

Questo default va interpretato come una nomenclatura legacy del codice, non come un vincolo di dominio. Per nuovi contesti applicativi è consigliabile impostare esplicitamente un nome più generico, ad esempio:

```python
from data_platform.datalake.lake import BasePathsConfig

paths = BasePathsConfig(
    source_name="example_api",
    root="data-platform",
    asset_partition_name="asset",
)
```

In questo modo i path prodotti useranno `asset=...` invece della partizione legacy.

### Root del data lake

Il pacchetto ha un default neutro:

```python
DEFAULT_LAKE_ROOT = "data-platform"
```

Questo valore viene usato solo se il chiamante non passa una root esplicita.

In un'applicazione reale è preferibile passare la root da configurazione:

```python
run_pipeline(..., lake_root="data-platform-dev")
```

In questo modo:

- il pacchetto resta generico;
- l'ambiente decide dove scrivere;
- si possono avere root diverse per sviluppo, test e produzione.

### `DataLake`

`DataLake` è una facade sopra `ObjectStorage`.

Invece di chiamare direttamente funzioni di serializzazione e upload, il codice applicativo può usare metodi più leggibili:

```python
lake.write_json_gz(key, payload)
lake.read_json_gz(key)
lake.write_parquet(key, df)
lake.read_parquet(key)
```

Esempio:

```python
from data_platform.datalake.lake import DataLake
from data_platform.datalake.storage import LocalObjectStorage

storage = LocalObjectStorage(root="./local_lake")
lake = DataLake(storage=storage, bucket="dev-bucket")

lake.write_json_gz(
    "data-platform/source=example_api/bronze/asset=asset_001/year=2026/month=06/day=21/raw.json.gz",
    {"records": []},
)
```

---

## `pipelines/adapters.py`

Questo modulo definisce i contratti tra le pipeline generiche e la logica specifica del progetto chiamante.

È uno dei moduli più importanti per un nuovo sviluppatore, perché chiarisce cosa deve implementare un pacchetto applicativo per usare `data_platform`.

### Oggetti principali

| Oggetto | Tipo | Descrizione |
|---|---|---|
| `PipelineContext` | dataclass | Contesto di esecuzione condiviso tra le pipeline |
| `BronzeRequest` | dataclass | Singola richiesta bronze da eseguire verso la sorgente |
| `BronzePayload` | dataclass | Payload bronze letto o scaricato, associato alla richiesta originale |
| `DatasetPayload` | dataclass | Dataset prodotto da un processor, con dataframe e key opzionale |
| `DataSource` | abstract class | Client astratto di una sorgente dati esterna |
| `BaseSourceAdapter` | classe base | Funzioni comuni per adapter specifici |
| `SourceAdapter` | protocol | Contratto che un adapter deve rispettare |
| `GoldProcessor` | protocol | Contratto che un processor gold deve rispettare |

### `PipelineContext`

`PipelineContext` contiene le informazioni comuni usate dalle pipeline:

```python
PipelineContext(
    asset="asset_001",
    source="example_api",
    time_range=(start, end, year, month, day),
    config=config_model,
    raw_config=config_dict,
    metadata={"run_id": "manual_test"},
)
```

Campi principali:

| Campo | Significato |
|---|---|
| `asset` | Entità elaborata dalla pipeline |
| `source` | Nome logico della sorgente dati |
| `time_range` | Finestra temporale usata dalla pipeline |
| `config` | Configurazione modellata o validata, se disponibile |
| `raw_config` | Configurazione in formato dizionario |
| `metadata` | Metadati opzionali della run |

La proprietà `start_datetime` restituisce l'inizio finestra, mentre `end_datetime` restituisce la fine finestra.

### `DataSource`

`DataSource` rappresenta il client che comunica con una sorgente esterna.

Deve occuparsi di:

- configurare endpoint, header, autenticazione e timeout;
- chiamare la sorgente;
- restituire il payload raw.

Non deve normalizzare i dati. La normalizzazione è responsabilità dell'adapter.

Metodi da implementare:

```python
def fetch_data(self, start_timestamp, end_timestamp, parameters=None):
    ...

def get_source_name(self) -> str:
    ...
```

Il metodo `api_get_request` è una utility comune per effettuare richieste HTTP GET con gestione centralizzata degli errori di connessione.

### `BronzeRequest`

`BronzeRequest` descrive una singola chiamata da fare alla sorgente.

```python
BronzeRequest(
    instrument="measurements",
    fetch_parameters={"entity_id": "A001"},
    metadata={"dataset_name": "measurements_A001"},
)
```

La pipeline bronze non costruisce queste richieste da sola: le riceve dall'adapter.

### `SourceAdapter`

`SourceAdapter` è il contratto che un adapter specifico deve rispettare per usare bronze e silver.

Un adapter deve sapere:

- quali richieste bronze generare;
- come costruire le key bronze;
- quali dataset silver produrre;
- quali bronze leggere per ogni silver;
- come trasformare i payload raw in dataframe normalizzati.

Metodi richiesti:

```python
def iter_bronze_requests(self, config: dict, time_range: tuple):
    ...

def fetch_bronze(self, client: DataSource, request: BronzeRequest, time_range: tuple):
    ...

def bronze_key(self, *, config: dict, request: BronzeRequest, end_datetime):
    ...

def available_silver_instruments(self, config: dict) -> list[str]:
    ...

def iter_silver_requests_for_instrument(self, config: dict, instrument: str, time_range: tuple):
    ...

def silver_key(self, *, config: dict, instrument: str, end_datetime):
    ...

def transform_bronze_payloads_to_silver(
    self,
    *,
    config: dict,
    instrument: str,
    bronze_payloads: list[BronzePayload],
    time_range: tuple,
) -> pd.DataFrame:
    ...
```

Nel codice il nome `instrument` indica una categoria logica di dati da produrre o trasformare. In un nuovo progetto può rappresentare un dataset, una tabella, una metrica tecnica, una famiglia di record o qualsiasi unità coerente di elaborazione.

### `GoldProcessor`

`GoldProcessor` è il contratto usato dalla pipeline gold.

Serve quando la logica gold è specifica del progetto e non può stare nel pacchetto generico.

Metodi richiesti:

```python
def get_inputs(self, context: PipelineContext) -> dict[str, str]:
    ...

def build(self, datasets: dict[str, pd.DataFrame], context: PipelineContext) -> DatasetPayload:
    ...

def output_key(self, context: PipelineContext) -> str:
    ...
```

La pipeline gold si limita a leggere gli input, chiamare il processor e salvare l'output.

---

## `pipelines/medallion.py`

Questo modulo contiene le pipeline generiche.

### Oggetti principali

| Oggetto | Descrizione |
|---|---|
| `BronzePipeline` | Recupera payload raw e li salva come JSON gzip |
| `SilverPipeline` | Legge bronze, normalizza tramite adapter e salva parquet |
| `GoldPipeline` | Legge input parquet, costruisce output tramite processor e salva parquet |

---

## `BronzePipeline`

La bronze pipeline esegue il seguente flusso:

```text
adapter.iter_bronze_requests()
        ↓
adapter.fetch_bronze()
        ↓
adapter.bronze_key()
        ↓
lake.write_json_gz()
```

La pipeline non conosce la sorgente reale. Non sa se i dati arrivano da API REST, database, file CSV o altro. Questa informazione è incapsulata nel `DataSource` e nell'adapter.

Restituisce un dizionario di statistiche:

```python
{
    "processed": 10,
    "uploaded": 10,
    "empty": 0,
    "failed": 0,
}
```

Significato:

| Campo | Significato |
|---|---|
| `processed` | Numero di richieste bronze elaborate |
| `uploaded` | Numero di payload salvati correttamente |
| `empty` | Numero di risposte vuote |
| `failed` | Numero di richieste fallite |

---

## `SilverPipeline`

La silver pipeline esegue il seguente flusso:

```text
adapter.available_silver_instruments()
        ↓
adapter.iter_silver_requests_for_instrument()
        ↓
lake.read_json_gz()
        ↓
adapter.transform_bronze_payloads_to_silver()
        ↓
adapter.silver_key()
        ↓
lake.write_parquet()
```

La pipeline non contiene logica di pulizia o normalizzazione specifica. La trasformazione dei dati raw in dataframe silver è delegata all'adapter.

Se l'output silver è vuoto, il parquet non viene scritto e viene incrementato il contatore `empty`.

---

## `GoldPipeline`

La gold pipeline esegue il seguente flusso:

```text
processor.get_inputs()
        ↓
lake.read_parquet()
        ↓
processor.build()
        ↓
processor.output_key()
        ↓
lake.write_parquet()
```

Di default, se il file di output esiste già, la pipeline può accodare i nuovi dati a quelli vecchi:

```python
GoldPipeline(lake).run(
    context=context,
    processor=processor,
    append_if_exists=True,
)
```

La logica usata è:

```text
vecchio parquet + nuovo dataframe → concat → drop_duplicates → scrittura parquet
```

Se non si vuole accodare al file esistente:

```python
GoldPipeline(lake).run(
    context=context,
    processor=processor,
    append_if_exists=False,
)
```

---

## `utils/time_utils.py`

Questo modulo contiene utility temporali generiche.

### Oggetti principali

| Oggetto | Descrizione |
|---|---|
| `TimeWindow` | Dataclass che rappresenta una finestra temporale UTC |
| `build_day_window_utc` | Converte un giorno locale in intervallo UTC |
| `get_target_date` | Restituisce una data target di default |
| `build_time_window` | Costruisce una `TimeWindow` completa |
| `time_range` | Funzione di compatibilità con il formato tuple |
| `split_utc_range` | Divide un intervallo UTC in blocchi più piccoli |

### Gestione timezone

La funzione più importante è `build_day_window_utc`.

Esempio:

```python
from data_platform.utils.time_utils import build_day_window_utc

start_utc, end_utc = build_day_window_utc(
    "2026-06-21",
    tz_local="Europe/Rome",
)
```

Il giorno viene interpretato nella timezone locale e poi convertito in UTC.

Questo è importante perché molte sorgenti lavorano in UTC, mentre la logica applicativa spesso ragiona per giorno locale.

### Compatibilità con `time_range`

`time_range` restituisce una tupla nel formato:

```text
(start_utc, end_utc, year, month, day)
```

Questo formato è usato da `PipelineContext` e dagli adapter.

Esempio:

```python
from data_platform.utils.time_utils import time_range

tr = time_range("2026-06-21", tz_local="Europe/Rome")
```

### Split di intervalli lunghi

`split_utc_range` è utile quando una sorgente non accetta richieste troppo grandi.

Esempio:

```python
from data_platform.utils.time_utils import split_utc_range

chunks = split_utc_range(
    "2026-06-01T00:00:00Z",
    "2026-06-03T00:00:00Z",
    max_hours=24,
)
```

Output concettuale:

```python
[
    ("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z"),
    ("2026-06-02T00:00:00Z", "2026-06-03T00:00:00Z"),
]
```

---

## `utils/quality.py`

Questo modulo contiene utility generiche per controlli base di qualità dati.

### Funzioni principali

| Funzione | Descrizione |
|---|---|
| `build_dataframe_quality_report` | Produce un report generico su un dataframe |
| `build_pre_gold_nan_quality_report` | Wrapper di compatibilità per report pre-gold |

### `build_dataframe_quality_report`

Questa funzione produce un dizionario con informazioni utili per debug e monitoraggio:

- numero di righe;
- elenco colonne;
- numero di colonne;
- conteggio valori mancanti per colonna;
- percentuale di valori mancanti per colonna;
- colonne completamente vuote;
- righe duplicate;
- metadati opzionali.

Esempio:

```python
from data_platform.utils.quality import build_dataframe_quality_report

report = build_dataframe_quality_report(
    df,
    name="silver_measurements",
    metadata={"source": "example_api", "asset": "asset_001"},
)
```

Output concettuale:

```python
{
    "name": "silver_measurements",
    "metadata": {"source": "example_api", "asset": "asset_001"},
    "rows": 96,
    "columns": ["datetime", "value"],
    "column_count": 2,
    "missing_count": {"datetime": 0, "value": 3},
    "missing_ratio": {"datetime": 0.0, "value": 0.03125},
    "empty_columns": [],
    "duplicated_rows": 0,
}
```

### Significato di `missing_count`

`missing_count` indica quanti valori mancanti sono presenti per ogni colonna.

Nel caso di pandas, normalmente si parla di valori `NaN`, `None` o valori riconosciuti come mancanti.

---

## `utils/diagnostics.py`

Questo modulo centralizza eccezioni e logging comuni.

### Eccezioni principali

| Eccezione | Quando usarla |
|---|---|
| `DataPlatformError` | Eccezione base del pacchetto |
| `ConfigurationError` | Configurazione mancante o non valida |
| `DataValidationError` | Dati in input non validi |
| `MissingColumnsError` | Colonne obbligatorie mancanti in un dataframe |
| `EmptyDataFrameError` | Dataframe vuoto quando non dovrebbe esserlo |
| `SerializationError` | Errore nella serializzazione/deserializzazione |
| `ObjectStorageError` | Errore generico di storage |
| `ObjectNotFoundError` | Oggetto non trovato nello storage |
| `ObjectDownloadError` | Errore durante il download |
| `ObjectUploadError` | Errore durante l'upload |
| `SourceConnectionError` | Errore di connessione a una sorgente |
| `SourceResponseError` | Risposta della sorgente non valida o inattesa |
| `PipelineExecutionError` | Errore durante l'esecuzione di una pipeline |

### Perché centralizzare le eccezioni

Centralizzare le eccezioni permette di:

- distinguere errori di configurazione, dati, storage, sorgente e pipeline;
- rendere i log più leggibili;
- evitare `Exception` generiche sparse nel codice;
- semplificare la gestione degli errori in orchestratori o job schedulati.

### Logger comune

La funzione `get_logger` restituisce un logger con formato standard:

```python
from data_platform.utils.diagnostics import get_logger

logger = get_logger(__name__)
logger.info("Pipeline started")
```

Il formato di default è:

```text
2026-06-21 10:00:00 | INFO | module.name | Pipeline started
```

---

# Come usare il pacchetto

## Esempio minimo locale

Esempio di creazione di un data lake locale:

```python
from data_platform.datalake.lake import DataLake
from data_platform.datalake.storage import LocalObjectStorage

storage = LocalObjectStorage(root="./local_lake")
lake = DataLake(storage=storage, bucket="test-bucket")

lake.write_json(
    "data-platform/source=example_api/bronze/asset=asset_001/year=2026/month=06/day=21/sample.json",
    {"status": "ok"},
    indent=2,
)

payload = lake.read_json(
    "data-platform/source=example_api/bronze/asset=asset_001/year=2026/month=06/day=21/sample.json"
)
```

Questo esempio non richiede GCS ed è utile per verificare velocemente che serializzazione e path funzionino.

---

## Esempio concettuale con pipeline

Le pipeline generiche si usano così:

```python
from data_platform.datalake.lake import DataLake
from data_platform.pipelines.adapters import PipelineContext
from data_platform.pipelines.medallion import BronzePipeline, SilverPipeline, GoldPipeline
from data_platform.utils.time_utils import time_range

context = PipelineContext(
    asset="asset_001",
    source="example_api",
    time_range=time_range("2026-06-21"),
    config=config_model,
    raw_config=config_dict,
)

BronzePipeline(lake).run(
    context=context,
    adapter=source_adapter,
    client=source_client,
)

SilverPipeline(lake).run(
    context=context,
    adapter=source_adapter,
)

GoldPipeline(lake).run(
    context=context,
    processor=gold_processor,
)
```

In questo esempio:

- `lake` è un'istanza di `DataLake`;
- `source_client` implementa `DataSource`;
- `source_adapter` implementa `SourceAdapter`;
- `gold_processor` implementa `GoldProcessor`;
- `config_dict` contiene la configurazione raw del progetto applicativo.

---

# Come estendere il pacchetto per una nuova sorgente

Per aggiungere una nuova sorgente dati, normalmente non serve modificare `data_platform`.

Bisogna creare nel pacchetto applicativo:

1. un client che estende `DataSource`;
2. un adapter che implementa `SourceAdapter`;
3. opzionalmente un processor che implementa `GoldProcessor`.

## 1. Creare un client sorgente

Esempio concettuale:

```python
from data_platform.pipelines.adapters import DataSource

class ExampleSourceClient(DataSource):
    base_url = "https://api.example.com"
    timeout = 30

    def fetch_data(self, start_timestamp, end_timestamp, parameters=None):
        response = self.api_get_request(
            endpoint="/records",
            params={
                "start": start_timestamp,
                "end": end_timestamp,
                **(parameters or {}),
            },
        )
        return response.json()

    def get_source_name(self) -> str:
        return "example_api"
```

## 2. Creare un adapter

Esempio concettuale:

```python
import pandas as pd

from data_platform.datalake.lake import BasePathsConfig
from data_platform.pipelines.adapters import BaseSourceAdapter, BronzeRequest

class ExampleSourceAdapter(BaseSourceAdapter):
    source_name = "example_api"
    paths = BasePathsConfig(
        source_name="example_api",
        root="data-platform",
        asset_partition_name="asset",
        silver_filename_template="{instrument}.parquet",
    )

    def iter_bronze_requests(self, config, time_range):
        for entity in config["entities"]:
            yield BronzeRequest(
                instrument="measurements",
                fetch_parameters={"entity_id": entity["id"]},
                metadata={"dataset_name": entity["name"]},
            )

    def bronze_key(self, *, config, request, end_datetime):
        asset = config["metadata"]["asset_name"]
        filename = f"{request.metadata['dataset_name']}.json.gz"
        return self.paths.bronze_prefix(plant=asset, dt=end_datetime) + filename

    def available_silver_instruments(self, config):
        return ["measurements"]

    def iter_silver_requests_for_instrument(self, config, instrument, time_range):
        return self.iter_bronze_requests(config, time_range)

    def transform_bronze_payloads_to_silver(self, *, config, instrument, bronze_payloads, time_range):
        rows = []
        for item in bronze_payloads:
            rows.extend(item.payload.get("records", []))
        return pd.DataFrame(rows)
```

Nota: alcuni helper di `BasePathsConfig` usano ancora il nome parametro `plant` per compatibilità interna. Nei nuovi progetti può essere passato un valore generico come `asset`, soprattutto se `asset_partition_name="asset"`.

## 3. Creare un processor gold

Esempio concettuale:

```python
from data_platform.pipelines.adapters import DatasetPayload

class ExampleGoldProcessor:
    def get_inputs(self, context):
        return {
            "measurements": "data-platform/source=example_api/silver/asset=asset_001/year=2026/month=06/day=21/measurements.parquet",
        }

    def build(self, datasets, context):
        df = datasets["measurements"]
        df_gold = df.groupby("datetime", as_index=False).sum(numeric_only=True)
        return DatasetPayload(name="gold_measurements", dataframe=df_gold)

    def output_key(self, context):
        return "data-platform/source=example_api/gold/asset=asset_001/year=2026/month=06/gold_measurements.parquet"
```

---

# Convenzioni consigliate

## Separazione delle responsabilità

Quando si sviluppa sopra `data_platform`, mantenere questa separazione:

| Logica | Dove dovrebbe stare |
|---|---|
| Upload/download generico | `data_platform.datalake.storage` |
| Costruzione path medallion | `data_platform.datalake.lake` |
| Orchestrazione bronze/silver/gold | `data_platform.pipelines.medallion` |
| Chiamata a una specifica sorgente | Pacchetto applicativo, classe `DataSource` |
| Mapping configurazione → richieste bronze | Pacchetto applicativo, `SourceAdapter` |
| Normalizzazione raw → silver | Pacchetto applicativo, `SourceAdapter` |
| Costruzione dataset finali | Pacchetto applicativo, `GoldProcessor` |

## Cosa non mettere in `data_platform`

Evitare di inserire nel pacchetto generico:

- nomi di API specifiche;
- nomi di entità specifiche di dominio;
- calcoli di business;
- mapping applicativi;
- logica legata a un cliente;
- credenziali o path di ambiente;
- query SQL o endpoint specifici;
- regole valide solo per un singolo progetto.

## Cosa può stare in `data_platform`

È corretto inserire qui:

- utility generiche;
- astrazioni comuni;
- interfacce e protocolli;
- funzioni di serializzazione;
- gestione storage;
- gestione path;
- gestione timezone;
- eccezioni comuni;
- report tecnici generici.

---

# Dipendenze principali

Il pacchetto usa principalmente:

- `pandas` per dataframe, timestamp e parquet;
- `pyarrow` come engine parquet;
- `requests` per chiamate HTTP base;
- `python-dateutil` per utility timezone/date;
- `google-cloud-storage` nel progetto chiamante quando si usa GCS.

La dipendenza da Google Cloud Storage è gestita tramite client iniettato in `GCSObjectStorage`.

---

# Note operative per nuovi sviluppatori

## Prima cosa da controllare

Quando si prende in mano un progetto che usa `data_platform`, verificare:

1. quale `ObjectStorage` viene usato: locale o GCS;
2. quale bucket viene passato a `DataLake`;
3. quale root viene usata nei path;
4. quale adapter implementa `SourceAdapter`;
5. quale client implementa `DataSource`;
6. quale processor implementa `GoldProcessor`;
7. quale timezone viene usata per costruire la finestra temporale;
8. se la gold deve accodare ai dati esistenti o sovrascrivere l'output;
9. quale nome di partizione viene usato per l'asset (`asset_partition_name`).

## Debug dei path

Se un file non viene trovato, controllare sempre:

- `root`;
- `source`;
- layer (`bronze`, `silver`, `gold`);
- nome della partizione dell'asset;
- valore dell'asset;
- anno, mese e giorno;
- nome file finale.

Molti problemi di lettura/scrittura derivano da una differenza anche minima tra la key prodotta in bronze e quella cercata in silver.

## Debug della bronze

Se la bronze non scrive i file attesi:

- verificare che `iter_bronze_requests` generi almeno una richiesta;
- controllare i parametri passati a `fetch_data`;
- verificare autenticazione, endpoint e timeout nel `DataSource`;
- controllare se la sorgente restituisce payload vuoti;
- verificare che `bronze_key` costruisca una key valida e coerente.

## Debug della silver

Se la silver non produce file:

- verificare che la bronze abbia scritto i `.json.gz` attesi;
- verificare che `iter_silver_requests_for_instrument` generi le stesse request attese;
- verificare che `bronze_key` sia deterministica;
- controllare se `transform_bronze_payloads_to_silver` restituisce un dataframe vuoto;
- controllare i log della pipeline per `empty` e `failed`.

## Debug della gold

Se la gold fallisce:

- verificare le key restituite da `processor.get_inputs`;
- controllare che i parquet input esistano realmente;
- verificare che i dataframe letti abbiano le colonne attese dal processor;
- controllare se `append_if_exists=True` sta accodando dati storici già presenti;
- verificare eventuali duplicati dopo il concat.

---

# Sintesi per onboarding

Un nuovo sviluppatore può leggere il pacchetto in questo ordine:

1. `utils/diagnostics.py` per capire eccezioni e logging;
2. `datalake/storage.py` per capire come avvengono lettura e scrittura;
3. `datalake/lake.py` per capire la costruzione dei path;
4. `pipelines/adapters.py` per capire i contratti da implementare;
5. `pipelines/medallion.py` per capire il flusso bronze/silver/gold;
6. `utils/time_utils.py` e `utils/quality.py` per le utility di supporto.

Il punto più importante è che `data_platform` deve rimanere un pacchetto infrastrutturale e riutilizzabile. Le regole specifiche di una sorgente, di un cliente o di un dominio devono stare nei pacchetti applicativi che lo usano.
