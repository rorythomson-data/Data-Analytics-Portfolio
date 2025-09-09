# Internship Dashboard – Handover

![Internship Dashboard Pipeline](https://github.com/rorythomson-data/Internship_Dashboard/actions/workflows/main.yml/badge.svg)

Este repositorio contiene un **pipeline de datos modular y listo para producción**.  
El pipeline **automatiza la extracción, transformación y almacenamiento de métricas de negocio** desde dos plataformas SaaS:

- **ChartMogul** (métricas de suscripción, clientes, planes, MRR).
- **Holded** (facturas, pagos, contactos, gastos, tesorería).

Los datos procesados están listos para su visualización en **Power BI**, con un enfoque dirigido a la toma de decisiones ejecutivas.

---

## Funcionalidades

- **Extracción automática** desde APIs REST (ChartMogul y Holded).
- **Transformación y limpieza** de estructuras JSON anidadas.
- **Almacenamiento organizado**:
  - Datos crudos en `data/INPUT/<endpoint>/raw/`.
  - Datos transformados en `data/INPUT/<endpoint>/clean/`.
  - Métricas finales en `data/OUTPUT/`.
- **Automatización completa** con `run_all.py`.
- **Logs detallados** en `logs/` (`pipeline.log`, `metrics_pipeline.log`).
- **Integración opcional con CI/CD** (GitHub Actions o similar).

---

## Estructura del Proyecto

```plaintext
Internship_Dashboard/
│
├── data/
│   ├── INPUT/            # Datos crudos y limpios (por endpoint)
│   └── OUTPUT/           # Métricas finales para BI
│
├── data_pipeline/
│   ├── CM/               # Scripts para ChartMogul
│   │   ├── Extract/
│   │   └── Transform/
│   ├── HD/               # Scripts para Holded
│   │   ├── Extract/
│   │   └── Transform/
│
├── dashboard/            # Dashboard de Power BI (.pbix)
├── notebooks/            # Notebooks de análisis y validación
├── reports/              # Informes adicionales
├── logs/                 # Registros de ejecución
├── run_all.py            # Script principal (ejecuta todo el pipeline)
├── metrics_pipeline.py   # Construcción de métricas
├── requirements.txt      # Dependencias de Python
└── (crear `.env`)        # Claves de API (no subir a Git)
```

---

## Configuración Inicial

1) **Clonar el repositorio**

```bash
git clone <COMPANY_REPO_URL> Internship_Dashboard
cd Internship_Dashboard
```

2) **Crear entorno virtual**

```bash
# Windows (PowerShell)
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1

# macOS/Linux
python3 -m venv .venv
source .venv/bin/activate
```

3) **Instalar dependencias**

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

4) **Configurar variables de entorno**

Cree un archivo `.env` en la raíz con sus claves (no lo comparta ni lo suba a Git):

```ini
CHARTMOGUL_API_KEY=su_clave_chartmogul
HOLDED_API_KEY=su_clave_holded
```

El proyecto usa `os.getenv("CHARTMOGUL_API_KEY")` y `os.getenv("HOLDED_API_KEY")`.

---

## Ejecución del Pipeline (local)

```bash
python run_all.py
```

El pipeline:
1. Extrae datos crudos → `data/INPUT/<endpoint>/raw/*.json`
2. Transforma y limpia → `data/INPUT/<endpoint>/clean/*.csv/.parquet`
3. Construye métricas → `data/OUTPUT/final_metrics_latest.csv` (y `.parquet`)
4. Registra actividad → `logs/`

---

## Dashboard de Power BI

Archivo: `dashboard/metrics_dashboard.pbix`

- Abrir en **Power BI Desktop**.
- Si aparece un error de ruta, actualizar la conexión a `data/OUTPUT/final_metrics_latest.csv`.
- Pulsar **Actualizar (Refresh)** para cargar las métricas más recientes.

Opcional: publicar en **Power BI Service** y configurar actualizaciones automáticas.

---

## Integración Continua (opcional)

El pipeline puede integrarse con **GitHub Actions** u otro orquestador CI/CD:

- Instalación de dependencias desde `requirements.txt`.
- Ejecución de `run_all.py`.
- Publicación de artefactos desde `data/OUTPUT/`.

> La empresa debe configurar su propio flujo CI/CD en su repositorio interno.  
> Este README proporciona las instrucciones, pero la configuración depende del entorno de la compañía.

---

## Notas Importantes

- **Python version:** desarrollado en **3.12.0** (usar 3.12.x recomendado).
- **Secretos:** no incluir `.env` en Git. Usar variables de entorno o gestores de secretos.
- **Paths:** si el proyecto se mueve a OneDrive u otra carpeta, actualizar las rutas de conexión en Power BI.
- **Logs:** revisar `logs/` para diagnosticar errores.
- **Deliverables oficiales:**  
  - `dashboard/metrics_dashboard.pbix`  
  - `data/OUTPUT/final_metrics_latest.csv` (entrada al dashboard)

---

## Contacto / Ownership

Este repositorio se transfiere ahora a la organización de la empresa.  



