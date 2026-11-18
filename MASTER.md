# MASTER DESIGN SYSTEM — DAV Mobile Station v5.0 (`--persist`)

> **Proyecto:** DAV Mobile Station v5.0 — Telemetría de Microclimas Agrícolas en Tiempo Real  
> **Institución:** Liceo Bicentenario de Excelencia Polivalente San Nicolás  
> **Equipo Creador:** **GeoGo** — Área Premilitar  
> **Audiencia Objetivo:** Agricultores locales (lectura rápida y accionable en terreno/móvil y escritorio) + Comunidad Educativa  
> **Problema que Resuelve:** Las estaciones meteorológicas oficiales cubren zonas macro y no capturan los microclimas reales dentro de cada predio o cultivo (heladas locales, corredores de viento, caídas barométricas y desnivel del mástil en terreno).  
> **Stack Tecnológico:** HTML5 + Tailwind CSS + Apache ECharts 5 + CSS Spatial Glass & Neumorphism + Vanilla JS (ArcGIS FeatureServer REST API).

---

## 1. Diales de Configuración (`UI/UX Pro Max Dials`)

| Dial | Valor (1–10) | Definición aplicada |
| :--- | :---: | :--- |
| **`--variance`** | **6 / 10** | Asimetría controlada: grilla Bento espacial con tarjetas principales de telemetría (4 columnas) + módulo doble de Orientación GY-9250 (Brújula Espacial + Clinómetro de Nivelación) + panel ancho de Tendencias Históricas. |
| **`--motion`** | **7 / 10** | Transiciones fluidas tipo física de resorte (`cubic-bezier(0.22, 1, 0.36, 1)`): **Glassy Navigation Tabs V7** con píldora deslizante traslúcida y brillo especular, rotación suave de brújula, burbuja de nivelación en vivo y micro-elevación `translateY(-4px)`. |
| **`--density`** | **5 / 10** | Equilibrio espacial: números grandes de alto contraste para lectura rápida en el campo, respiraderos generosos (`24px`–`32px` de radio) y metadatos técnicos secundarios bien jerarquizados. |

---

## 2. Dirección Estética: Spatial UI + Dark Soft Neumorphism

Combinación de **Spatial Computing UI** (superficies de vidrio esmerilado con profundidad multicapa y bordes iluminados) y **Neumorfismo táctil oscuro** (cavidades suaves `inset` para diales, pistas de progreso, brújula, nivel de burbuja y contenedores de pestañas):

- **Superficie Base (Fondo Espacial):**
  - `#0b1120` $\rightarrow$ `#0f172a` con halos radiales sutiles en cian ártico (`rgba(56, 189, 248, 0.10)`), esmeralda agrícola (`rgba(16, 185, 129, 0.08)`) e índigo (`rgba(99, 102, 241, 0.08)`).
- **Superficie Primaria (`.spatial-card`):**
  - Fondo: `linear-gradient(145deg, rgba(30, 41, 59, 0.62), rgba(15, 23, 42, 0.78))`
  - Blur: `backdrop-filter: blur(20px) saturate(160%)`
  - Borde especular: `1px solid rgba(255, 255, 255, 0.10)` (superior más brillante `rgba(255, 255, 255, 0.22)`)
  - Sombra neumórfica externa + interna:
    `box-shadow: 12px 16px 32px rgba(2, 6, 23, 0.65), -6px -6px 20px rgba(255, 255, 255, 0.025), inset 0 1px 1px rgba(255, 255, 255, 0.15)`
- **Cavidad Neumórfica (`.neo-inset`):**
  - Fondo: `rgba(9, 14, 26, 0.65)`
  - Sombra interna profunda:
    `box-shadow: inset 4px 4px 10px rgba(2, 6, 23, 0.85), inset -2px -2px 8px rgba(255, 255, 255, 0.04)`
  - Borde: `1px solid rgba(255, 255, 255, 0.06)`

---

## 3. Componente Insignia: Glassy Navigation Tabs V7 (Inspirado en Video)

- **Contenedor de Pista (`.glass-tabs-track`):**
  - Píldora cápsula (`border-radius: 9999px`) con cavidad neumórfica + vidrio traslúcido (`backdrop-filter: blur(18px)`), borde especular `1px solid rgba(255, 255, 255, 0.14)` y sombra interior suave.
- **Píldora Activa Deslizante (`.glass-tab-pill`):**
  - Se posiciona dinámicamente mediante `transform: translateX(...)` y `width` calculados con `getBoundingClientRect()`, usando transición elástica `transition: transform 0.45s cubic-bezier(0.22, 1, 0.36, 1), width 0.45s cubic-bezier(0.22, 1, 0.36, 1)`.
  - Estilo de lente de vidrio iluminado:
    `background: linear-gradient(135deg, rgba(255, 255, 255, 0.18), rgba(255, 255, 255, 0.06))`
    `box-shadow: 0 4px 18px rgba(0, 0, 0, 0.35), inset 0 1px 1.5px rgba(255, 255, 255, 0.45), inset 0 -1px 1px rgba(255, 255, 255, 0.08)`

---

## 4. Semántica de Color Agrícola y Telemetría

| Variable / Sensor | Color Principal | Uso en UI |
| :--- | :--- | :--- |
| **Temperatura (`temperatura`)** | `#fb923c` (Ámbar/Naranja) / `#38bdf8` (< 5°C Helada) | Valor principal, barra neumórfica y alerta de helada agrícola. |
| **Humedad (`humedad`)** | `#38bdf8` (Cian Agua) | Humedad relativa + cálculo de Punto de Rocío (Magnus). |
| **Presión (`presion`)** | `#a78bfa` (Violeta Barométrico) | Presión en hPa + tendencia de 3 horas (`▲`, `●`, `▼`). |
| **Viento (`velocidad_viento`)** | `#2dd4bf` (Turquesa/ Cian) | Velocímetro espacial y alerta de ráfagas para pulverización/riego. |
| **Brújula (`rumbo`, `punto_cardinal`)** | `#34d399` (Esmeralda GeoGo) | Rosa náutica espacial, aguja animada y badge de rumbo cardinal. |
| **Inclinación (`inclinacion_x`, `inclinacion_y`)** | `#60a5fa` (Azul Horizonte) | Nivel de burbuja 2D (Pitch/Roll) y diagnóstico de aplome del mástil. |
