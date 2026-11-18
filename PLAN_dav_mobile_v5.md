# PLAN DE CAMBIOS — `dav_mobile/dav_mobile_v5.ino`

> **REGLAS PARA EL AGENTE EJECUTOR**
> 1. Edita **SOLO** `dav_mobile/dav_mobile_v5.ino`. **NO toques `dav_mobile_v4.ino`** ni otros archivos.
> 2. Haz las fases en orden (A → B → C → D). Después de cada fase, compila si hay `arduino-cli` (placa `esp32:esp32:esp32`). Si no hay, revisa a mano llaves, `;` y declaraciones antes de uso.
> 3. No cambies lo que no se pide (anemómetro, DHT22, ArcGIS, buffer offline, pines). Cambios mínimos.
> 4. Código comentado en español, nombres sin tildes ni ñ.
> 5. No uses librerías externas nuevas: solo las del core ESP32 (`Preferences.h`, `WebServer.h`, `DNSServer.h`).

---

## 0. Resultado de la revisión (estado actual de v5)

### BME280 — ✅ bien implementado (con 2 mejoras menores)
- Constructor SPI por software `Adafruit_BME280 bme(BME_CS, BME_SDI, BME_SDO, BME_SCK)` → orden correcto (cs, mosi, miso, sck). Pines 25/26/27/32 coinciden con `test_bmp280.ino`.
- Lectura de presión validada en 600–1150 hPa, y si falla se envía sin presión. Correcto.
- ⚠️ `bme.begin()` solo se intenta **una vez** en `setup()`. Si el sensor tarda o se conecta mal, queda sin presión hasta reiniciar → agregar reintento (Fase A1).
- ⚠️ El test usa `Adafruit_BMP280`. Si el módulo físico fuera BMP280 (chip ID 0x58) `Adafruit_BME280.begin()` fallaría (espera 0x60). Se avisa por Serial para diagnosticar (Fase A1).

### MPU-9250 / GY-9250 — ⚠️ detecta bien, pero hay BUGS en la lectura
1. 🐞 **Orden de evaluación indefinido**: `(Wire.read() << 8) | Wire.read()` (líneas ~173-175 y ~198-200). En C++ el orden de evaluación de los operandos no está garantizado → los bytes alto/bajo pueden invertirse y dar valores basura. Hay que leer cada byte en una variable separada.
2. 🐞 **Ejes del magnetómetro desalineados**: el AK8963 tiene sus ejes distintos al acelerómetro (X↔Y intercambiados y Z invertido). El rumbo actual usa `atan2(my, mx)` crudo → el norte saldrá rotado/espejado.
3. ⚠️ Sin calibración hard-iron (offsets) ni compensación de inclinación → rumbo impreciso si la estación se inclina.
4. ⚠️ No usa los valores de ajuste de sensibilidad del AK8963 (opcional, se omite).
5. ⚠️ Si el MPU se desconecta, `magDisponible` queda en `true` (estado viejo). Resetearlo.
6. Formato LCD: `X:%d` vs `Y:%02d` inconsistentes.

### Otros hallazgos
- Textos dicen "V4.6" en v5 → cambiar a "V5.0".
- `pestañeoLED` usa `ñ` en un identificador → renombrar a `parpadeoLED` (riesgo de compilación).
- **WiFi**: credenciales fijas en código, sin escaneo, sin portal. `procesarEnvio()` hace reconexión bloqueante a las 2 redes fijas. Todo esto se reemplaza (Fase B/C).

---

## FASE A — Correcciones de sensores

### A1. BME280: reintento y diagnóstico
Agrega variable global `unsigned long tiempoReintentoBME = 0;` y una función:
```cpp
void iniciarBME() {
  if (bme.begin()) {
    bme.setSampling(Adafruit_BME280::MODE_NORMAL,
                    Adafruit_BME280::SAMPLING_X2,
                    Adafruit_BME280::SAMPLING_X16,
                    Adafruit_BME280::SAMPLING_X1,
                    Adafruit_BME280::FILTER_X16,
                    Adafruit_BME280::STANDBY_MS_500);
    bmeDisponible = true;
    Serial.println("[BME280] OK en SPI (25/26/27/32).");
  } else {
    bmeDisponible = false;
    Serial.println("[BME280] No responde en SPI. Revisar cableado. (Si el modulo es BMP280 el ID es 0x58 y esta libreria no lo acepta)");
  }
}
```
- En `setup()` reemplaza el bloque "4. Iniciar BME280" por `iniciarBME();`.
- En `leerPresionBME()`, al inicio: si `!bmeDisponible`, y `millis() - tiempoReintentoBME > 30000`, hacer `tiempoReintentoBME = millis(); iniciarBME();` y luego `if (!bmeDisponible) return NAN;`.

### A2. MPU-9250: lecturas seguras (reemplazar los bloques de lectura)
**Acelerómetro** (después de `Wire.requestFrom(MPU_ADDR, 6)` y el chequeo `available() < 6`):
```cpp
uint8_t b[6];
for (int i = 0; i < 6; i++) b[i] = Wire.read();
int16_t ax = (int16_t)((b[0] << 8) | b[1]);
int16_t ay = (int16_t)((b[2] << 8) | b[3]);
int16_t az = (int16_t)((b[4] << 8) | b[5]);
```
**Magnetómetro** (después de `Wire.requestFrom(MAG_ADDR, 7)` y `available() == 7`). El AK8963 entrega byte bajo primero:
```cpp
uint8_t m[7];
for (int i = 0; i < 7; i++) m[i] = Wire.read();
int16_t mxa = (int16_t)((m[1] << 8) | m[0]);   // eje X del AK8963
int16_t mya = (int16_t)((m[3] << 8) | m[2]);   // eje Y del AK8963
uint8_t st2 = m[6];
if (!(st2 & 0x08)) {
  // Alinear ejes AK8963 -> ejes del acelerometro MPU: X_mpu = Y_ak, Y_mpu = X_ak
  float mx = (float)mya - MAG_OFF_X;
  float my = (float)mxa - MAG_OFF_Y;
  float heading = atan2(my, mx) * 180.0 / PI + DECLINACION_MAG;
  while (heading < 0) heading += 360.0;
  while (heading >= 360.0) heading -= 360.0;
  rumbo = (int)round(heading) % 360;
}
```
Agrega constantes globales (cerca de `MAG_ADDR`):
```cpp
// Calibracion magnetometro (hard-iron). Dejar en 0 hasta calibrar.
float MAG_OFF_X = 0.0, MAG_OFF_Y = 0.0;
// Declinacion magnetica local en grados (Chile central aprox. -2 a -3). Ajustar.
float DECLINACION_MAG = 0.0;
```
- Al inicio del bloque de falla de `Wire.endTransmission` del acelerómetro (donde pone `mpuDisponible = false`) agrega también `magDisponible = false;`.
- Formato LCD línea 2 de orientación: usar `"X:%3d%c Y:%3d%c"` en ambos casos (pitch y roll con `%3d`).

> **Verificación en banco (hacerla el usuario, no el agente):** apuntar la estación al norte real con un celular; si el rumbo sale invertido (sentido contrario), cambiar el signo: `heading = -heading` antes de normalizar. Para calibrar, anotar min/max de mx y my girando 360° y poner `OFF = (max+min)/2`. Esto no se puede validar sin hardware; el agente solo deja las constantes.

---

## FASE B — Gestor de redes WiFi (almacenamiento + conexión)

### Comportamiento requerido (resumen del usuario)
1. Al encender: **escanear** redes.
2. Si hay una red guardada visible → conectarse. Prioridad: **la principal** (índice 0); si no está visible, la siguiente guardada visible (en orden de lista).
3. Si no hay ninguna guardada visible → **crear WiFi propio** con un panel simple: lista de redes disponibles + contraseña.
4. La red elegida pasa a ser **principal** y se guarda en memoria no volátil (persiste al reiniciar). Las anteriores quedan en la lista como respaldo (máximo 5, se descarta la más antigua).

### B1. Includes y globales (al inicio del archivo, tras los includes existentes)
```cpp
#include <Preferences.h>
#include <WebServer.h>
#include <DNSServer.h>
```
Elimina las constantes `WIFI_SSID_PRIORITARIO/PASS_PRIORITARIO/RESPALDO` pero **guárdalas como valores semilla** (ver B2):
```cpp
#define MAX_REDES 5
struct RedGuardada { char ssid[33]; char pass[64]; };
RedGuardada redes[MAX_REDES];   // redes[0] = PRINCIPAL
int numRedes = 0;
Preferences prefs;
bool modoPortal = false;
unsigned long tiempoUltimoIntentoWiFi = 0;
bool avisoUltimoFallo = false;  // para mostrar mensaje en el portal
```

### B2. Persistencia (Preferences / NVS)
Namespace `"wifi"`, **siempre** `prefs.begin("wifi", false)` (modo escritura, evita error en primer arranque), y `prefs.end()` al terminar. Claves: `n` (UChar, cantidad), `s0..s4`, `p0..p4` (String), y pendiente: `ps`, `pp` (String).
```cpp
void guardarRedes() {
  prefs.begin("wifi", false);
  prefs.putUChar("n", numRedes);
  for (int i = 0; i < numRedes; i++) {
    prefs.putString(("s" + String(i)).c_str(), redes[i].ssid);
    prefs.putString(("p" + String(i)).c_str(), redes[i].pass);
  }
  prefs.end();
}

void cargarRedes() {
  prefs.begin("wifi", false);
  numRedes = prefs.getUChar("n", 0);
  if (numRedes > MAX_REDES) numRedes = 0;
  for (int i = 0; i < numRedes; i++) {
    prefs.getString(("s" + String(i)).c_str(), "").toCharArray(redes[i].ssid, 33);
    prefs.getString(("p" + String(i)).c_str(), "").toCharArray(redes[i].pass, 64);
  }
  prefs.end();
  if (numRedes == 0) {   // primer arranque: sembrar redes originales de v4
    strcpy(redes[0].ssid, "geogo_IoT");     strcpy(redes[0].pass, "2iot2025");
    strcpy(redes[1].ssid, "Bicentenario");  redes[1].pass[0] = '\0';
    numRedes = 2;
    guardarRedes();
  }
}

// Pone una red como PRINCIPAL (indice 0). Si ya existe, la mueve; si no, la agrega. Maximo MAX_REDES.
void definirPrincipal(const char* ssid, const char* pass) {
  int pos = -1;
  for (int i = 0; i < numRedes; i++) if (strcmp(redes[i].ssid, ssid) == 0) { pos = i; break; }
  if (pos == -1) {
    if (numRedes < MAX_REDES) numRedes++;
    pos = numRedes - 1;                    // si la lista esta llena, sobreescribe la ultima (la mas antigua)
  }
  for (int i = pos; i > 0; i--) redes[i] = redes[i - 1];   // desplazar hacia abajo
  strncpy(redes[0].ssid, ssid, 32); redes[0].ssid[32] = '\0';
  strncpy(redes[0].pass, pass, 63); redes[0].pass[63] = '\0';
  guardarRedes();
}
```
Red **pendiente** (la que el usuario eligió en el portal, aún sin probar; ver Fase C):
```cpp
void guardarPendiente(const String& s, const String& p) {
  prefs.begin("wifi", false); prefs.putString("ps", s); prefs.putString("pp", p); prefs.end();
}
void borrarPendiente() {
  prefs.begin("wifi", false); prefs.remove("ps"); prefs.remove("pp"); prefs.end();
}
```

### B3. Conexión
```cpp
bool intentarConectar(const char* ssid, const char* pass, unsigned long timeoutMs) {
  Serial.printf("[WIFI] Conectando a %s...\n", ssid);
  lcd.clear(); lcd.setCursor(0, 0); lcd.print("Conectando WiFi");
  lcd.setCursor(0, 1); lcd.print(String(ssid).substring(0, 16));
  WiFi.disconnect();
  delay(100);
  WiFi.begin(ssid, pass);
  unsigned long t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < timeoutMs) { delay(300); Serial.print("."); }
  Serial.println();
  return WiFi.status() == WL_CONNECTED;
}

// Escanea, y conecta a la primera guardada visible (orden: principal primero). true si conecto.
bool conectarGuardadas() {
  lcd.clear(); lcd.setCursor(0, 0); lcd.print("Buscando redes..");
  int n = WiFi.scanNetworks();
  Serial.printf("[WIFI] %d redes encontradas.\n", n);
  bool ok = false;
  for (int i = 0; i < numRedes && !ok; i++) {
    for (int j = 0; j < n; j++) {
      if (WiFi.SSID(j) == String(redes[i].ssid)) {
        ok = intentarConectar(redes[i].ssid, redes[i].pass, 10000);
        break;   // pasar a la siguiente guardada si fallo
      }
    }
  }
  WiFi.scanDelete();
  return ok;
}
```
> Importante: conectarse a una red de respaldo **no** cambia la principal.

### B4. Flujo en `setup()` (reemplaza el bloque "5. Conexión WiFi")
```cpp
cargarRedes();
WiFi.mode(WIFI_STA);
WiFi.disconnect();
delay(100);

bool conectado = false;

// 1) Red pendiente (recien elegida en el portal): probarla primero
prefs.begin("wifi", false);
String ps = prefs.getString("ps", ""), pp = prefs.getString("pp", "");
prefs.end();
if (ps.length() > 0) {
  conectado = intentarConectar(ps.c_str(), pp.c_str(), 12000);
  if (conectado) definirPrincipal(ps.c_str(), pp.c_str());   // queda como principal
  else avisoUltimoFallo = true;                              // clave mala o fuera de alcance
  borrarPendiente();
}
// 2) Redes guardadas visibles
if (!conectado) conectado = conectarGuardadas();
// 3) Nada: portal cautivo
if (!conectado) iniciarPortal();
```
Luego mantén el mensaje LCD "WiFi Conectado!/Modo Offline" pero, si `modoPortal`, muestra `"AP:DAV-Mobile"` / `"192.168.4.1"`.

### B5. Reconexión en marcha (reemplaza la reconexión bloqueante)
- En `procesarEnvio()` **borra** todo el bloque `if (WiFi.status() != WL_CONNECTED) {...}` y pon en su lugar:
```cpp
if (WiFi.status() != WL_CONNECTED) return false;   // el buffer offline guarda el dato
```
- Nueva función y llamada al inicio de `loop()` (antes de `leerDHT22()`):
```cpp
void mantenerWiFi() {
  if (modoPortal) return;
  if (WiFi.status() == WL_CONNECTED) return;
  if (millis() - tiempoUltimoIntentoWiFi < 30000) return;   // cada 30 s
  tiempoUltimoIntentoWiFi = millis();
  Serial.println("[WIFI] Conexion perdida. Buscando redes guardadas...");
  conectarGuardadas();
}
```
(El anemómetro usa interrupciones y `dt_ms` real, así que el bloqueo breve no corrompe la medición.) **No abrir el portal en caliente**; solo al encender.

---

## FASE C — Portal cautivo (punto de acceso + panel)

Nombre AP: `DAV-Mobile-Setup` (abierto, sin clave). IP: `192.168.4.1`.

### C1. Globales
```cpp
WebServer servidor(80);
DNSServer dns;
String listaRedesHTML = "";
unsigned long tiempoPortalInicio = 0, tiempoUltimoChequeoPortal = 0;
```

### C2. Funciones
```cpp
String escaparHTML(String s) {
  s.replace("&", "&amp;"); s.replace("<", "&lt;"); s.replace(">", "&gt;"); s.replace("\"", "&quot;");
  return s;
}

void escanearParaPortal() {
  int n = WiFi.scanNetworks();
  listaRedesHTML = "";
  for (int i = 0; i < n; i++) {
    String s = WiFi.SSID(i);
    if (s.length() == 0) continue;                 // omitir ocultas
    bool abierta = (WiFi.encryptionType(i) == WIFI_AUTH_OPEN);
    listaRedesHTML += "<option value=\"" + escaparHTML(s) + "\">" + escaparHTML(s) +
                      " (" + String(WiFi.RSSI(i)) + " dBm)" + (abierta ? " [abierta]" : "") + "</option>";
  }
  WiFi.scanDelete();
}

String paginaBase(const String& cuerpo) {
  return "<!DOCTYPE html><html><head><meta charset='utf-8'>"
         "<meta name='viewport' content='width=device-width,initial-scale=1'>"
         "<title>DAV Mobile</title><style>body{font-family:sans-serif;max-width:420px;margin:20px auto;padding:0 12px}"
         "select,input,button{width:100%;padding:10px;margin:6px 0;font-size:16px;box-sizing:border-box}"
         "button{background:#0a7;color:#fff;border:0}.err{color:#b00}</style></head><body>"
         "<h2>DAV Mobile Station</h2>" + cuerpo + "</body></html>";
}

void paginaPrincipal() {
  if (servidor.hasArg("r") || listaRedesHTML.length() == 0) escanearParaPortal();   // ?r=1 = reescanear
  String c = "";
  if (avisoUltimoFallo) c += "<p class='err'>No se pudo conectar con la ultima red. Revisa la clave.</p>";
  c += "<form method='POST' action='/save'>"
       "<label>Red WiFi</label><select name='ssid'>" + listaRedesHTML + "</select>"
       "<label>Contrase&ntilde;a (vacia si es abierta)</label><input type='password' name='pass'>"
       "<button type='submit'>Guardar y conectar</button></form>"
       "<p><a href='/?r=1'>Buscar redes de nuevo</a></p>";
  servidor.send(200, "text/html", paginaBase(c));
}

void paginaGuardar() {
  String ssid = servidor.arg("ssid");
  String pass = servidor.arg("pass");
  if (ssid.length() == 0 || ssid.length() > 32 || pass.length() > 63) {
    servidor.send(400, "text/html", paginaBase("<p class='err'>Datos invalidos.</p><a href='/'>Volver</a>"));
    return;
  }
  guardarPendiente(ssid, pass);   // se prueba al reiniciar (el AP interferiria con la prueba aqui)
  servidor.send(200, "text/html", paginaBase(
    "<p>Guardado. La estacion se reinicia y probara conectarse a <b>" + escaparHTML(ssid) +
    "</b>.</p><p>Si falla, volvera a aparecer la red <b>DAV-Mobile-Setup</b>.</p>"));
  delay(1500);
  ESP.restart();
}

void iniciarPortal() {
  modoPortal = true;
  WiFi.mode(WIFI_AP_STA);
  WiFi.softAP("DAV-Mobile-Setup");
  dns.start(53, "*", WiFi.softAPIP());                 // portal cautivo: todo dominio -> 192.168.4.1
  escanearParaPortal();
  servidor.on("/", HTTP_GET, paginaPrincipal);
  servidor.on("/save", HTTP_POST, paginaGuardar);
  servidor.onNotFound([]() {                           // redirigir cualquier URL al panel
    servidor.sendHeader("Location", "http://192.168.4.1/", true);
    servidor.send(302, "text/plain", "");
  });
  servidor.begin();
  tiempoPortalInicio = tiempoUltimoChequeoPortal = millis();
  Serial.println("[PORTAL] AP DAV-Mobile-Setup activo en 192.168.4.1");
}

void atenderPortal() {   // llamar en cada vuelta de loop()
  if (!modoPortal) return;
  dns.processNextRequest();
  servidor.handleClient();
  // Cada 2 min, si nadie esta conectado al AP, ver si reaparecio una red guardada
  if (millis() - tiempoUltimoChequeoPortal > 120000 && WiFi.softAPgetStationNum() == 0) {
    tiempoUltimoChequeoPortal = millis();
    int n = WiFi.scanNetworks();
    for (int i = 0; i < numRedes; i++)
      for (int j = 0; j < n; j++)
        if (WiFi.SSID(j) == String(redes[i].ssid)) { Serial.println("[PORTAL] Red guardada visible. Reiniciando."); delay(300); ESP.restart(); }
    WiFi.scanDelete();
  }
}
```
Agrega `atenderPortal();` en `loop()` como **primera línea** (antes del `if (dt_ms < 1000) return;`) para que atienda web aunque el resto espere.

### C3. LCD en modo portal
En `actualizarLCD()`: si `modoPortal`, usar 3 pantallas (`% 3`) y la pantalla 2 muestra:
```cpp
// l1 = "WiFi: DAV-Mobile-"  (16 chars)   l2 = "IP 192.168.4.1   "
```
Si no `modoPortal`, mantener `% 2`. El resto (sensores, buffer offline) sigue funcionando mientras el portal está activo.

---

## FASE D — Limpieza final y pruebas

1. Reemplazar todos los textos "V4.6" por "V5.0" (cabecera, `bienvenidaImpacto`, banner de `setup`).
2. Renombrar `pestañeoLED` → `parpadeoLED` (definición y uso).
3. Comentario de cabecera: documentar el flujo WiFi nuevo.
4. Compilar (`esp32:esp32:esp32`). Revisar que no queden referencias a `WIFI_SSID_PRIORITARIO`, etc. (`Select-String`).

### Pruebas manuales (las hace el usuario con el ESP32)
| # | Prueba | Esperado |
|---|--------|----------|
| 1 | Encender con `geogo_IoT` cerca | Conecta a geogo_IoT |
| 2 | Apagar geogo_IoT, `Bicentenario` visible | Conecta a Bicentenario (principal no cambia) |
| 3 | Ninguna guardada visible | LCD muestra AP; aparece WiFi `DAV-Mobile-Setup`; el celular abre el panel solo |
| 4 | Elegir red + clave correcta | Reinicia, conecta, queda principal; al reiniciar de nuevo conecta a ella primero |
| 5 | Elegir red + clave incorrecta | Reinicia, falla, vuelve el portal con aviso de error; la principal anterior no se pierde |
| 6 | Perder WiFi en marcha | Buffer offline guarda datos; reconecta sola a las 30 s cuando haya red guardada |
| 7 | BME280 desconectado y reconectado | Reintenta cada 30 s y recupera presión |
| 8 | Pantalla 2: girar la estación | Rumbo cambia de forma coherente (ajustar signo/offsets si no) |

## Notas / riesgos conocidos
- Contraseñas WiFi quedan en NVS en texto plano (aceptable para este proyecto). El portal es abierto: cualquiera cerca podría cambiar la red durante el modo portal. Opcional: `WiFi.softAP("DAV-Mobile-Setup", "davmobile")`.
- Las redes con SSID oculto no aparecen en la lista ni se detectan por escaneo.
- `http.begin(https://...)` sin certificado funciona igual que en v4; no tocar.
