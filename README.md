# Telco Network Logs Analytics Hand-on Lab

As part of this lab, you'll build Network Logs Analytics scenario using Cell KPIs and Network alarms live streaming data. You'll ingest, transform and analytize this data in real-time so when an incident happens, you can get alerts immediately and you can do faster troubleshooting and RCA on those incidents, and take actions in real-time.

Cell KPIs are the performance counters each cell reports and industry groups them into five families, and your dataset has all five:

<img width="1191" height="946" alt="image" src="https://github.com/user-attachments/assets/a0da444d-8801-4da4-a22e-aa64b1622522" /><br>

Static reference data for a **Lakehouse** plus a notebook that streams live network logs to an
**Eventstream** custom endpoint.

```

├── NetworkLogsData/                          # 6 CSVs → upload to Lakehouse
└── telco_network_eventstream_producer.ipynb  #live streaming data generator
└── load_csv_to_lakehouse.ipynb               #code to load 6 CSV sample data files to Lakehouse
  
```

Scope: 50 sites, 150 cells (5G + 4G), 1,000 subscribers across 5 Australian cities.

---

## Static data (6 CSVs)

### Dimensions

| File | Rows | Columns |
|---|---:|---|
| `dim_site.csv` | 50 | site_id, site_name, city, state, site_type, morphology, backhaul_type, vendor |
| `dim_cell.csv` | 150 | cell_id, site_id, city, technology (4G/5G), band, bandwidth, sector, max_throughput, status |
| `dim_subscriber.csv` | 1,000 | subscriber_id, plan_name, price, customer_type, home_site_id, city, device_type, vip_flag |
| `dim_alarm_catalog.csv` | 12 | alarm_code, alarm_name, severity, domain, service_affecting, mttr_target_min |

### Facts

| File | Rows | Contents |
|---|---:|---|
| `fact_cell_kpi_daily.csv` | 4,500 | 30 days × 150 cells — PRB, RRC success, drop rate, throughput, users, volume, availability |
| `fact_alarm_history.csv` | 2,000 | 30 days of alarms with raise/clear times, duration, status, root cause |

Keys join on `cell_id`, `site_id`, `city` and `alarm_code`.

---

## Live streaming notebook

13 cells. Paste one connection string, run everything.

| `event_type` | Share | Fields |
|---|---:|---|
| `cell.kpi` | ~85% | PRB utilisation, RRC success, drop rate, throughput, active users, RSRP, SINR |
| `network.alarm` | ~15% | alarm_code, name, severity, domain, cell, state (raised/cleared) |

Every event also carries a **`payload` column holding a nested JSON object** — it lands in
Eventhouse as a `dynamic` column you can query with dot notation, no pre-flattening needed.

| Stream | Payload groups | Fields |
|---|---|---:|
| `cell.kpi` | `radio`, `traffic`, `resources`, `health` | 15 |
| `network.alarm` | `source`, `detail`, `handling` | 12 |

```kusto
CellKpi
| extend cqi = toreal(payload.radio.cqi),
         cpu = toreal(payload.resources.cpu_load_pct)
| where cpu > 70
```

Cells carry a persistent `load` value that drifts between ticks, so the time-series charts look
like a real network rather than random noise.

The notebook **streams continuously until you stop it** — hit the stop button or *Interrupt
kernel*. It prints progress every 10 seconds and a summary on exit.

### Built-in incident

After 60 seconds, 10 Sydney cells go into congestion. Events are tagged `incident_id = "INC-001"`.
Measured effect on those cells:

| Metric | Before | During |
|---|---:|---:|
| PRB utilisation | 37.8% | **82.2%** |
| Throughput | 210 Mbps | **91 Mbps** |
| Drop rate | 0.31% | **1.93%** |
| RRC success | 99.5% | **96.2%** |

Matching alarms (`ALM-002`, `ALM-003`, `ALM-010`, `ALM-012`) fire at the same time.

---

## Setup

### 1. Generate and upload the CSVs
```bash
python generate_telco_data.py
```
Fabric → Lakehouse → **Files** → *Upload folder* → select `data`, then load to Delta tables.

### 2. Create the Eventstream
**New Eventstream** → **Add source** → *Custom endpoint* → **Event hub** tab → copy the
*Connection string-primary key*.

### 3. Run the notebook
Upload `notebooks/telco_network_eventstream_producer.ipynb`, paste the connection string into
cell 2, run all cells. It streams until you stop it.

### 4. Route to Eventhouse
Add a **Filter** transformation on `event_type`:

| Filter | Table |
|---|---|
| `event_type == "cell.kpi"` | `CellKpi` |
| `event_type == "network.alarm"` | `NetworkAlarm` |

---

## Included KQL

Cell 6 ships eight queries: congested cells, the incident timechart, alarms by severity and city,
alarm-burst detection, live KPIs joined to `dim_site`, subscribers affected by congestion (with VIP
count), and two that query into the nested `payload` column — radio/resource counters and alarm
threshold breaches needing field dispatch.

---

*Synthetic data for demonstration only. No real subscriber identifiers are generated.*
