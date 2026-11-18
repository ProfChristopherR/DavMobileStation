# Especificación Técnica de Modelado CAD: Escudo de Radiación Solar para BME280 (Optimizado para Fusion 360 MCP)

## 1. Contexto e Instrucciones para el Agente IA (Fusion 360 MCP)
Actúa como un Ingeniero de Diseño Mecánico automatizado operando sobre la API/MCP de **Autodesk Fusion 360**. Tu objetivo es modelar un **Escudo de Radiación Solar Pasivo (Stevenson Screen)** optimizado para impresión 3D FDM en material **ABS**, diseñado a medida para alojar un sensor **BME280**.

### Reglas de Diseño para Manufactura Aditiva (DFAM - ABS):
1. **Cero Soportes (Support-Free):** Todos los voladizos deben tener un ángulo $\ge 45^\circ$ respecto a la horizontal (plano XY) cuando la pieza se orienta para impresión (invertida a $180^\circ$ respecto a su posición de trabajo).
2. **Compensación de Contracción (ABS):** Los orificios para tornillos M3 y encajes incluyen tolerancias holgadas ($+0.2\text{ mm}$ a $+0.3\text{ mm}$) para compensar la contracción térmica del ABS.
3. **Sistema de Coordenadas y Unidades:** 
   - Unidades del documento: **Milímetros ($\text{mm}$)**.
   - Plano base: **XY** (Horizontal). Eje **Z** es el eje vertical de apilamiento y revolución.
   - Origen $(0,0,0)$: Centro de la base inferior del ensamblaje.

---

## 2. Parámetros Dimensionales del Sensor BME280 (Datos de Entrada)
Nota técnica de verificación: Con un ancho de placa $W = 12.0\text{ mm}$ y un diámetro de orificio $D_h = 3.1\text{ mm}$ cuyo borde está a $1.0\text{ mm}$ de las aristas laterales y superior, el radio del orificio es $r_h = 1.55\text{ mm}$. Por lo tanto, el centro de cada orificio se ubica a $1.0 + 1.55 = 2.55\text{ mm}$ de los bordes. La distancia real entre centros en el eje de ancho es $12.0 - 2 \times 2.55 = 6.9\text{ mm}$ (si el módulo tiene los 2 agujeros a lo largo del borde de $15.5\text{ mm}$, la distancia entre centros es $10.0\text{ mm}$). **Para garantizar compatibilidad absoluta sin errores de rotación de placa**, el soporte usará un encaje de guías laterales deslizantes (Snap-in Slot) que sujeta los bordes de $1.6\text{ mm}$ de espesor más un tope para los agujeros.

* **Largo de PCB ($L_{pcb}$):** $15.5\text{ mm}$
* **Ancho de PCB ($W_{pcb}$):** $12.0\text{ mm}$
* **Espesor de PCB ($T_{pcb}$):** $1.6\text{ mm}$
* **Espesor total con componentes ($T_{comp}$):** $3.0\text{ mm}$
* **Longitud de pines sobresalientes ($L_{pins}$):** $8.0\text{ mm}$ (hacia arriba para conexión Dupont/soldadura)
* **Espacio libre total requerido en cámara central:** Cilindro de $\varnothing 36.0\text{ mm} \times 45.0\text{ mm}$ de alto.

---

## 3. Arquitectura del Ensamblaje (4 Componentes Modulares)
El diseño evita el uso de varillas roscadas metálicas largas mediante un sistema de **columnas de encaje por torsión o tornillos M3 cortos**, y se divide en 4 cuerpos/componentes en el árbol de Fusion 360:

1. `Comp_1_Base_Mount`: Base anular inferior + Cartucho central vertical para el BME280 + Brazo de fijación exterior.
2. `Comp_2_Open_Ring` (x5 instancias): Anillos intermedios ventilados (abiertos en el centro).
3. `Comp_3_Blind_Ring`: Plato deflector intermedio-superior (semi-cerrado con bloqueo de radiación directa).
4. `Comp_4_Top_Cap`: Tapa superior cónica ciega (doble techo aislante).

### Dimensiones Globales del Escudo:
* **Diámetro exterior de los platos ($D_{ext}$):** $90.0\text{ mm}$
* **Diámetro interior de la cámara de aire ($D_{int}$):** $42.0\text{ mm}$
* **Diámetro del círculo de pilares de unión ($D_{pitch}$):** $50.0\text{ mm}$ (3 pilares a $120^\circ$)
* **Paso vertical entre platos ($P_{z}$):** $12.0\text{ mm}$
* **Solapamiento vertical de bloqueo óptico:** $4.0\text{ mm}$ (impide que cualquier rayo de luz directo o gota de lluvia en ángulo $< 35^\circ$ entre a la cámara del sensor).

---

## 4. Secuencia Algorítmica de Operaciones para Fusion 360 MCP

### COMPONENTE 1: `Comp_2_Open_Ring` (Anillo Ventilado Estándar)
*Instrucción MCP: Crear nuevo componente `Comp_2_Open_Ring` y activarlo.*

#### Paso 1.1: Revolución de la Falda Cónica (Louver Blade)
1. Crear un `Sketch` en el plano **XZ**.
2. Dibujar un polígono cerrado (sección transversal del ala a $45^\circ$) definido por los siguientes puntos $(X, Z)$ en $\text{mm}$:
   - $P_1 = (21.0, 10.0)$ *(Labio interior superior - evita que el agua entre)*
   - $P_2 = (25.0, 14.0)$ *(Vértice superior del cono)*
   - $P_3 = (45.0, -6.0)$ *(Borde exterior inferior de goteo)*
   - $P_4 = (43.0, -6.0)$ *(Espesor de pared horizontal de $2.0\text{ mm}$)*
   - $P_5 = (25.0, 12.0)$ *(Vértice interior inferior)*
   - $P_6 = (22.6, 9.6)$ *(Cierre del labio interior)*
   - Conectar $P_1 \to P_2 \to P_3 \to P_4 \to P_5 \to P_6 \to P_1$.
3. Ejecutar `Revolve` del perfil $360^\circ$ alrededor del **Eje Z** (`Operation: New Body`).

#### Paso 1.2: Pilares Espaciadores Integrados (Standoffs)
1. Crear un `Sketch` en el plano **XY** en $Z = 0.0\text{ mm}$.
2. Dibujar un círculo con centro en $(X = 25.0, Y = 0.0)$ de diámetro $D_{pilar} = 8.0\text{ mm}$.
3. Dibujar un círculo concéntrico en $(X = 25.0, Y = 0.0)$ de diámetro $D_{hole} = 3.4\text{ mm}$ (paso libre para tornillo o varilla M3).
4. Ejecutar `Extrude` del anillo resultante (entre $\varnothing 8.0$ y $\varnothing 3.4$):
   - Dirección: Dos lados (`Two Sides`).
   - Hacia arriba ($+Z$): hasta $Z = 14.0\text{ mm}$.
   - Hacia abajo ($-Z$): hasta $Z = 2.0\text{ mm}$ (Altura neta del pilar $= 12.0\text{ mm}$, que define el paso exacto entre anillos).
   - `Operation: Join` con el cuerpo del anillo.
5. Crear una costilla de refuerzo (nervio radial) que una el pilar con la falda cónica para no requerir soportes al imprimir boca abajo:
   - En el plano **XZ**, dibujar un triángulo entre $(25.0, 12.0)$, $(29.0, 12.0)$ y $(29.0, 8.0)$, extruir simétricamente $2.4\text{ mm}$ (`Join`).
6. Ejecutar `Circular Pattern` de las operaciones del pilar (Paso 1.2) alrededor del **Eje Z**, `Quantity: 3`, ángulo $360^\circ$.

---

### COMPONENTE 2: `Comp_4_Top_Cap` (Tapa Superior Ciega con Escudo Térmico)
*Instrucción MCP: Crear nuevo componente `Comp_4_Top_Cap` y activarlo.*

1. Crear un `Sketch` en el plano **XZ**.
2. Dibujar el perfil del techo cerrado hasta el eje central $X = 0$:
   - $P_1 = (0.0, 16.0)$
   - $P_2 = (25.0, 16.0)$
   - $P_3 = (45.0, -4.0)$
   - $P_4 = (43.0, -4.0)$
   - $P_5 = (24.2, 13.6)$
   - $P_6 = (0.0, 13.6)$ *(Espesor de techo de $2.4\text{ mm}$ para opacidad UV total)*
   - Conectar $P_1 \to P_2 \to P_3 \to P_4 \to P_5 \to P_6 \to P_1$.
3. Ejecutar `Revolve` $360^\circ$ alrededor del **Eje Z** (`Operation: New Body`).
4. Crear los 3 pilares inferiores ciegos para atornillar desde abajo (o insertar tuercas M3):
   - `Sketch` en plano **XY** ($Z = 13.6\text{ mm}$).
   - Círculo en $(X = 25.0, Y = 0.0)$ de $\varnothing 8.0\text{ mm}$ y círculo interno de $\varnothing 2.8\text{ mm}$ (para que el tornillo M3 roscar directamente en el ABS).
   - Ejecutar `Extrude` hacia $-Z$ una distancia de $11.6\text{ mm}$ (llega hasta $Z = 2.0\text{ mm}$). `Operation: Join`.
   - Ejecutar `Circular Pattern` alrededor del **Eje Z**, `Quantity: 3`.

---

### COMPONENTE 3: `Comp_3_Blind_Ring` (Deflector Térmico Intermedio)
*Instrucción MCP: Crear nuevo componente `Comp_3_Blind_Ring`.*
*(Este plato va justo debajo de `Top_Cap` para crear una cámara de aire sacrificada que bloquee el calor radiante del techo).*

1. Copiar la geometría base de `Comp_2_Open_Ring`.
2. Añadir un disco deflector central en $Z = 10.0\text{ mm}$:
   - `Sketch` en **XY** ($Z = 10.0\text{ mm}$): Círculo en $(0,0)$ de diámetro $36.0\text{ mm}$.
   - `Extrude` $+Z$ por $2.0\text{ mm}$.
   - Conectar este disco central a los 3 pilares en $(r = 25.0\text{ mm})$ mediante 3 puentes radiales de $4.0\text{ mm}$ de ancho y $2.0\text{ mm}$ de espesor (`Circular Pattern: 3`). Esto bloquea el calor del techo pero deja una ranura anular de $3.0\text{ mm}$ alrededor para que el aire caliente escape.

---

### COMPONENTE 4: `Comp_1_Base_Mount` (Base, Soporte de Sensor BME280 y Brazo)
*Instrucción MCP: Crear nuevo componente `Comp_1_Base_Mount` y activarlo.*

#### Paso 4.1: Anillo Base y Cruceta Inferior
1. Repetir la revolución del cono del **Paso 1.1** (para que la base también tenga falda protectora contra radiación reflejada del suelo).
2. En el plano **XY** ($Z = 2.0\text{ mm}$), crear un anillo estructural que conecte los 3 pilares:
   - Círculo exterior $\varnothing 58.0\text{ mm}$, círculo interior $\varnothing 36.0\text{ mm}$, extruido de $Z = 2.0\text{ mm}$ a $Z = 6.0\text{ mm}$.
   - Añadir 3 perforaciones pasantes de $\varnothing 3.4\text{ mm}$ con avellanado inferior de $\varnothing 6.5\text{ mm} \times 3.0\text{ mm}$ de profundidad en el radio $R = 25.0\text{ mm}$ (cada $120^\circ$) para pasar los 3 tornillos o varillas M3 que unen toda la torre.
3. Crear un puente diametral (cruceta central) en el plano **XY** ($Z = 2.0\text{ mm}$):
   - Rectángulo centrado en $(0,0)$ de $X = 44.0\text{ mm}$, $Y = 16.0\text{ mm}$, extruido de $Z = 2.0\text{ mm}$ a $Z = 6.0\text{ mm}$ (`Join`).
   - Perforar en el centro $(0,0)$ un orificio pasacables ovalado (Slot) de $10.0\text{ mm} \times 6.0\text{ mm}$ desplazado a $Y = -4.0\text{ mm}$ para que bajen los 4 cables I2C.

#### Paso 4.2: Torreta Porta-Sensor BME280 (Posicionamiento Vertical Invertido)
*Objetivo: Sostener la placa BME280 ($15.5 \times 12.0 \times 1.6\text{ mm}$) en posición **vertical** en el centro geométrico del escudo ($Z = 20.0\text{ mm}$ a $Z = 45.0\text{ mm}$), con los pines apuntando hacia abajo (hacia el pasacables) para evitar acumulación de agua en los contactos y dejar el chip expuesto al flujo de aire laminar.*

1. Crear un `Sketch` en el plano **XY** sobre la cara superior del puente central ($Z = 6.0\text{ mm}$).
2. Dibujar dos columnas en forma de "U" enfrentadas (Guías laterales tipo *Card Edge Slot*) centradas en $X = 0, Y = 2.0\text{ mm}$:
   - Distancia entre las caras internas de fondo de las ranuras: $12.3\text{ mm}$ ($12.0\text{ mm}$ de ancho de PCB $+ 0.3\text{ mm}$ de holgura ABS).
   - Ancho de la ranura (canal para el espesor de la PCB): $1.85\text{ mm}$ ($1.6\text{ mm}$ de espesor de PCB $+ 0.25\text{ mm}$ de holgura).
   - Profundidad de agarre de cada pestaña lateral: $1.2\text{ mm}$ (deja libre toda la zona central de $9.6\text{ mm}$ donde está el chip BME280 y sus componentes de $3.0\text{ mm}$ de altura, además de no tapar los orificios de $3.1\text{ mm}$).
   - Espesor exterior de las paredes de las columnas: $2.0\text{ mm}$.
3. Ejecutar `Extrude` de ambas columnas en dirección $+Z$ desde $Z = 6.0\text{ mm}$ hasta $Z = 34.0\text{ mm}$ (Altura de columna $= 28.0\text{ mm}$).
4. **Topes de Altura para el PCB:**
   - Desde $Z = 6.0\text{ mm}$ hasta $Z = 18.0\text{ mm}$ ($12.0\text{ mm}$ de altura libre en la base), el canal permanece libre para alojar holgadamente los pines de $8.0\text{ mm}$ y los conectores hembra o soldaduras.
   - En $Z = 18.0\text{ mm}$, añadir un pequeño escalón interno de $1.0\text{ mm}$ dentro de las dos ranuras donde apoyará el borde inferior de la PCB.
   - De $Z = 18.0\text{ mm}$ a $Z = 33.5\text{ mm}$ ($15.5\text{ mm}$ exactos), se aloja la placa del BME280.
5. **Pines de Retención en los Orificios de $3.1\text{ mm}$ (Snap-Lock):**
   - La placa tiene dos orificios de $\varnothing 3.1\text{ mm}$ cuyos centros están a $2.55\text{ mm}$ del borde superior ($Z = 33.5 - 2.55 = 30.95\text{ mm}$) y a $2.55\text{ mm}$ de los bordes laterales ($X = \pm 3.45\text{ mm}$ si está orientada con el lado de $12\text{ mm}$ horizontal, o usar un travesaño superior con un clip a $45^\circ$ en $Z = 33.8\text{ mm}$ que abrace el borde superior de la PCB).
   - *Instrucción recomendada por simplicidad de impresión:* Colocar dos pestañas elásticas (cantilever snap-fit) en el extremo superior de las columnas ($Z = 33.6\text{ mm}$) con un saliente de $0.8\text{ mm}$ y rampa a $45^\circ$. Al deslizar el BME280 desde arriba, hace "clic" y queda bloqueado sin necesidad de tornillos metálicos que oxiden la PCB.

#### Paso 4.3: Brazo de Montaje a Mástil o Pared
1. En el plano **XY** ($Z = 2.0\text{ mm}$), extender desde el anillo base en dirección $+Y$ un brazo rectangular de ancho $24.0\text{ mm}$ y espesor $6.0\text{ mm}$ (con nervios triangulares a $45^\circ$ hacia $Z = 12.0\text{ mm}$), saliendo hasta $Y = 70.0\text{ mm}$ (fuera del radio de $45.0\text{ mm}$ de los platos).
2. En el extremo del brazo ($Y = 60.0\text{ mm}$ a $70.0\text{ mm}$), crear una placa vertical de $30.0\text{ mm} \times 30.0\text{ mm}$ con:
   - Dos orificios de $\varnothing 4.5\text{ mm}$ para tornillos a pared.
   - Dos ranuras verticales de $12.0\text{ mm} \times 3.0\text{ mm}$ para pasar una abrazadera metálica o brida plástica (precinto UV) para sujetarlo a un tubo o mástil.

---

## 5. Ensamblaje Final en Fusion 360 (Joints)
Para visualizar el conjunto completo en el espacio de trabajo, el agente debe posicionar los componentes en el eje Z con un desplazamiento (`Offset Z`) de $12.0\text{ mm}$ por nivel:

1. `Comp_1_Base_Mount`: $Z = 0.0\text{ mm}$ (Fijo / Grounded)
2. `Comp_2_Open_Ring_1`: $Z = 12.0\text{ mm}$
3. `Comp_2_Open_Ring_2`: $Z = 24.0\text{ mm}$ *(Nivel medio del sensor BME280)*
4. `Comp_2_Open_Ring_3`: $Z = 36.0\text{ mm}$
5. `Comp_3_Blind_Ring`: $Z = 48.0\text{ mm}$ *(Escudo térmico inferior)*
6. `Comp_4_Top_Cap`: $Z = 60.0\text{ mm}$ *(Techo exterior)*

*Nota de Tornillería de Unión:* Todo el paquete se atraviesa desde la base hasta la tapa con **3 varillas roscadas M3 de $70\text{ mm}$ de largo** (o 3 tornillos largos M3 $\times 70\text{ mm}$ enroscados directamente en los pilares ciegos del `Comp_4_Top_Cap`).

---

## 6. Parámetros de Exportación e Impresión (Slicer)
* **Orientación de exportación STL/3MF:** Rotar `Comp_2`, `Comp_3` y `Comp_4` **$180^\circ$ en el eje X** (boca abajo) para que la cara plana de los pilares o el techo repose sobre la cama caliente y los conos a $45^\circ$ suban sin usar soportes.
* **Altura de capa:** $0.20\text{ mm}$
* **Perímetros:** $4$ paredes (garantiza opacidad total y rigidez en las aletas).
* **Relleno (Infill):** $25\%$ Gyroid o Cúbico.
* **Material:** ABS Blanco (con post-procesado de barniz acrílico blanco brillante con protección UV).