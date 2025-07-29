# 📊 Panel de Control de la Práctica (Internship Dashboard)

![Internship Dashboard Pipeline](https://github.com/rorythomson-data/Internship_Dashboard/actions/workflows/main.yml/badge.svg)

Este repositorio contiene un **pipeline de datos modular y listo para producción**.  
El pipeline **automatiza la extracción, transformación y almacenamiento de métricas de negocio** desde dos plataformas SaaS:

- **ChartMogul** (métricas de suscripción, clientes y planes).
- **Holded** (facturas, pagos, contactos y gastos).

Los datos procesados están listos para visualización en **Power BI**, con un enfoque dirigido a ejecutivos y accionistas.

---

## 🚀 Funcionalidades

- **Extracción automática** desde APIs REST (ChartMogul y Holded).
- **Transformación y limpieza** de estructuras JSON anidadas.
- **Almacenamiento organizado**:
  - Datos crudos en `data/INPUT/raw/`.
  - Datos transformados en `data/INPUT/clean/` listos para BI.
- **Automatización completa** con `run_all.py`.
- **Logs detallados** con marcas de tiempo (`logs/pipeline.log`).
- **Integración CI/CD** con GitHub Actions:
  - Ejecución automática del pipeline.
  - Descarga de resultados (`data/OUTPUT/`) como artefactos.

---

## 📁 Estructura del Proyecto

```plaintext
Internship_Dashboard/
│
├── data/
│   ├── INPUT/          # Datos crudos y limpios
│   └── OUTPUT/         # Métricas finales y resúmenes
│
├── data_pipeline/
│   ├── CM/             # Scripts para ChartMogul
│   │   ├── Extract/
│   │   └── Transform/
│   ├── HD/             # Scripts para Holded
│   │   ├── Extract/
│   │   └── Transform/
│
├── notebooks/          # Notebooks de análisis y validación
├── reports/            # Informes y Power BI (.pbix)
├── logs/               # Registros de ejecución
├── run_all.py          # Script principal
├── requirements.txt    # Dependencias de Python
└── .env                # Claves de API (no subir a Git)
```

---

## ⚙️ Configuración Inicial

Clona este repositorio y crea un entorno virtual:

```bash
git clone https://github.com/rorythomson-data/Internship_Dashboard.git
cd Internship_Dashboard
python -m venv venv
./venv/Scripts/activate       # Windows
source venv/bin/activate      # macOS/Linux
pip install -r requirements.txt
```

Crea un archivo `.env` en la raíz con tus claves:

```ini
CHARTMOGUL_API_KEY=tu_clave_chartmogul
HOLDED_API_KEY=tu_clave_holded
```

---

## ▶ Ejecución del Pipeline

```bash
python run_all.py
```

Los datos serán descargados, transformados y guardados en `data/OUTPUT/`.  
Puedes revisar el registro de ejecución en `logs/pipeline.log`.

---

## 🔄 Integración Continua (CI/CD)

Cada vez que se hace un **push** a la rama `main`, GitHub Actions:

1. Instala dependencias de `requirements.txt`.
2. Ejecuta `run_all.py`.
3. Genera artefactos descargables con los archivos de `data/OUTPUT/`.

Puedes ver el estado de la última ejecución en el **badge** al inicio del README.

---

## 📄 Guía de Entrega

Para una referencia rápida sobre la configuración, ejecución del pipeline y visualización de resultados, consulta el documento:

[📥 handover_guide.pdf](handover_guide.pdf)

Este documento incluye:
- Pasos de configuración del entorno.
- Ejecución del pipeline (`run_all.py`).
- Integración con GitHub Actions.
- Conexión de los datos en Power BI.


