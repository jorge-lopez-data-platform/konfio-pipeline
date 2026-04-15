# Exchange Rates Pipeline (Databricks Notebook + Job Automatizado )




## Descripción general

Este proyecto implementa un pipeline de datos en Databricks para procesar información de tipos de cambio (exchange rates) desde una API externa.

El flujo sigue una arquitectura tipo Medallion (Bronze, Silver, Gold) para organizar los datos de forma escalable y fácil de mantener.

---

## Arquitectura


Bronze → Silver → Events → Gold


- Bronze → datos crudos desde la API  
- Silver → datos limpios + lógica de negocio + CDC  
- Events → generación de eventos (simulación de streaming)  
- Gold → métricas, agregaciones y detección de anomalías  

---

## Bronze (Raw Data)

Se consumen datos desde la API de Frankfurter:

- Base: USD  
- Monedas: MXN, EUR, BRL, COP  
- Rango de fechas definido  

Los datos se almacenan sin transformación en:


finance.exchange_rates.currency_bronze


---

## Silver (Transform + CDC)

En esta capa:

- Se limpian los datos (valores inválidos)
- Se convierten tipos (date)
- Se agregan columnas (year, month)
- Se implementa CDC con MERGE usando Delta Lake

Objetivo:
Evitar duplicados y mantener datos actualizados.

Tabla:


finance.exchange_rates.currency_silver


---

## Events (Eventos)

Se generan eventos a partir de Silver:

- event_type
- event_timestamp
- entity_id

Esto permite simular un flujo tipo streaming.

Tabla:


finance.exchange_rates.currency_events


---

## Gold (Analytics + Anomalías)

En esta capa:

- Se calculan métricas mensuales:
  - promedio
  - mínimo
  - máximo
  - volatilidad
- Se detectan anomalías usando desviación estándar
- Se agregan columnas de control:
  - fechacarga
  - fechacorte (último día del mes)

Tabla:


finance.exchange_rates.currency_gold


---

## Orquestación

El pipeline se ejecuta como un Job en Databricks, lo que permite:

- ejecución automática (schedule diario)
- reintentos en caso de fallo
- integración con clusters

---

## Tecnologías usadas

- PySpark  
- Delta Lake  
- Databricks  
- API REST (Frankfurter)  

---

## Objetivo del proyecto

Demostrar un flujo completo de datos que incluye:

- ingestión desde API  
- procesamiento incremental (CDC)  
- generación de eventos  
- modelado analítico  
- arquitectura escalable  

---

## Notas finales

- Se utiliza MERGE para evitar sobrescritura de datos  
- Se sigue arquitectura medallion para separar responsabilidades  
- El pipeline es fácilmente escalable a producción  


---

## Resumen

Se extraen datos desde una API, se procesan en una arquitectura Medallion,
se aplican transformaciones y CDC en Silver, se generan eventos,
y finalmente se construyen métricas y detección de anomalías en Gold.