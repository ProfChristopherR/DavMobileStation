#include <WiFi.h>
#include <HTTPClient.h>
#include <Wire.h>
#include <SPI.h>
#include <DHT.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_BME280.h>
#include <LiquidCrystal_I2C.h>
#include <time.h>
#include <math.h>
#include <Preferences.h>
#include <WebServer.h>
#include <DNSServer.h>

// =============================================================================
// SISTEMA DAV MOBILE V5.0 — ESTACION METEOROLOGICA PORTATIL
// Liceo Bicentenario de Excelencia Polivalente San Nicolas
// Sensores: BME280 en SPI (Principal: Temp/Hum/Presion) + DHT22 (Respaldo Temp/Hum) + Anemometro + MPU-9250
// Hardware: LCD y MPU-9250 en I2C (GPIO 21/22) | BME280 en SPI (GPIO 25/26/27/32)
// WiFi: Escaneo de redes guardadas en NVS (principal + respaldo) y portal cautivo
//       DAV-Mobile-Setup (192.168.4.1) si no hay ninguna red guardada disponible.
// =============================================================================

const char* serverName = "https://services4.arcgis.com/RZghiOOKLo2VeJGO/arcgis/rest/services/Estacion_Metereologica/FeatureServer/0/addFeatures";

// --- ASIGNACIÓN DE PINES DE HARDWARE ---
#define DHTPIN 13             // Pin de datos del DHT22
#define DHTTYPE DHT22
#define ENCODER_PIN 18        // Pin OUT del anemómetro
#define LED_STATUS 2

// Pines BME280 en modo SPI por software
#define BME_SCK 25            // Conectar al pin SCL / SCK del sensor
#define BME_SDI 26            // Conectar al pin SDA / SDI del sensor
#define BME_SDO 27            // Conectar al pin SDO del sensor
#define BME_CS  32            // Conectar al pin CSB / CS del sensor

// Dirección I2C del MPU-9250 / AK8963
#define MPU_ADDR 0x68
#define MAG_ADDR 0x0C

// Calibracion magnetometro (hard-iron). Dejar en 0 hasta calibrar.
float MAG_OFF_X = 0.0, MAG_OFF_Y = 0.0;
// Declinacion magnetica local en grados (Chile central aprox. -2 a -3). Ajustar.
float DECLINACION_MAG = 0.0;

DHT dht(DHTPIN, DHTTYPE);
LiquidCrystal_I2C lcd(0x27, 16, 2);                      // LCD en I2C nativo (Pines 21 y 22)
Adafruit_BME280 bme(BME_CS, BME_SDI, BME_SDO, BME_SCK); // BME280 en modo SPI

bool bmeDisponible = false;
bool mpuDisponible = false;
bool magDisponible = false;
unsigned long tiempoReintentoBME = 0;

// --- GESTOR DE REDES WIFI Y PORTAL CAUTIVO ---
#define MAX_REDES 5
struct RedGuardada {
  char ssid[33];
  char pass[64];
};
RedGuardada redes[MAX_REDES];   // redes[0] = PRINCIPAL
int numRedes = 0;
Preferences prefs;
bool modoPortal = false;
unsigned long tiempoUltimoIntentoWiFi = 0;
bool avisoUltimoFallo = false;  // para mostrar mensaje en el portal

WebServer servidor(80);
DNSServer dns;
String listaRedesHTML = "";
unsigned long tiempoPortalInicio = 0;
unsigned long tiempoUltimoChequeoPortal = 0;

// --- BUFFER CIRCULAR OFFLINE ---
struct Medicion {
  float temp;
  float hum;
  float presion;
  float viento;
  int rumbo;
  int pitch;
  int roll;
  bool mpuOk;
  long long timestamp;
};
const int BUFFER_SIZE = 100;
Medicion buffer[BUFFER_SIZE];
int head = 0, tail = 0, count = 0;

// --- VARIABLES ---
volatile int pulses = 0;
static volatile unsigned long debounce = 0;
unsigned long tiempoUltimoCalculo = 0;
unsigned long tiempoUltimaLecturaDHT = 0;
unsigned long tiempoCambioPantalla = 0;
int pantallaActual = 0; // 0: Meteorología | 1: Orientación
const unsigned long INTERVALO_PANTALLA_MS = 5000; // Alterna cada 5 segundos

int segundosContados = 0;
float sumaVelocidades = 0;
float ultimaTemp = NAN;
float ultimaHum = NAN;
const char* TZ_INFO = "<-04>4<-03>,M9.1.6/24,M4.1.6/24"; // Horario oficial Chile continental

// --- CALIBRACION ANEMOMETRO (v = anemA * RPS + anemB) ---
const float RADIO_ANEMOMETRO_M = 0.0693; // 46mm brazo + 23.3mm radio ext. cazoleta
const float FACTOR_ANEMOMETRO  = 2.5;    // Factor aerodinamico inicial
const float ANEM_A_DEFAULT = 2.0 * 3.1415926 * RADIO_ANEMOMETRO_M * FACTOR_ANEMOMETRO * 3.6;
const float ANEM_B_DEFAULT = 0.0;
float anemA = ANEM_A_DEFAULT;
float anemB = ANEM_B_DEFAULT;

float rpsHist[3] = {0, 0, 0};
int rpsHistIdx = 0, rpsHistCount = 0;
const int MAX_PUNTOS_CAL = 15;
float calRPS[MAX_PUNTOS_CAL];
float calVel[MAX_PUNTOS_CAL];
int calPuntos = 0;
String cmdSerial = "";

void IRAM_ATTR counter() {
  if (digitalRead(ENCODER_PIN) && (micros() - debounce > 500)) {
    debounce = micros();
    pulses++;
  }
}

void parpadeoLED() {
  digitalWrite(LED_STATUS, HIGH);
  delay(30);
  digitalWrite(LED_STATUS, LOW);
}

// --- CALIBRACION INTERACTIVA DEL ANEMOMETRO POR SERIAL + NVS ---
void guardarCalibracionAnem() {
  prefs.begin("anem", false);
  prefs.putFloat("a", anemA);
  prefs.putFloat("b", anemB);
  prefs.end();
}

void cargarCalibracionAnem() {
  prefs.begin("anem", false);
  float a = prefs.getFloat("a", ANEM_A_DEFAULT);
  float b = prefs.getFloat("b", ANEM_B_DEFAULT);
  prefs.end();
  if (!isnan(a) && a > 0.1 && a < 50.0 && !isnan(b) && b > -20.0 && b < 20.0) {
    anemA = a;
    anemB = b;
  } else {
    anemA = ANEM_A_DEFAULT;
    anemB = ANEM_B_DEFAULT;
  }
  Serial.printf("[ANEM] Calibracion cargada: v = %.4f * RPS + %.2f km/h\n", anemA, anemB);
}

float obtenerRPSPromedio() {
  if (rpsHistCount == 0) return 0.0;
  float suma = 0.0;
  for (int i = 0; i < rpsHistCount; i++) suma += rpsHist[i];
  return suma / (float)rpsHistCount;
}

void recalcularAnemometro() {
  if (calPuntos == 1) {
    if (calVel[0] > 0.1 && calRPS[0] > 0.05) {
      anemA = calVel[0] / calRPS[0];
      anemB = 0.0;
      guardarCalibracionAnem();
      Serial.printf("[CALIB] Ajuste proporcional (1 punto): v = %.4f * RPS km/h\n", anemA);
    } else {
      Serial.println("[CALIB] Punto guardado. Agrega al menos un punto con velocidad > 0 para ajustar.");
    }
    return;
  }
  if (calPuntos >= 2) {
    double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
    for (int i = 0; i < calPuntos; i++) {
      sumX  += calRPS[i];
      sumY  += calVel[i];
      sumXY += calRPS[i] * calVel[i];
      sumX2 += calRPS[i] * calRPS[i];
    }
    double denom = (double)calPuntos * sumX2 - sumX * sumX;
    if (denom > 0.0001) {
      double a = ((double)calPuntos * sumXY - sumX * sumY) / denom;
      double b = (sumY - a * sumX) / (double)calPuntos;
      if (a > 0.1) {
        anemA = (float)a;
        anemB = (float)b;
        guardarCalibracionAnem();
        Serial.printf("[CALIB] Recta actualizada (%d pts) y guardada: v = %.4f * RPS + %.2f km/h\n",
                      calPuntos, anemA, anemB);
      } else {
        Serial.println("[CALIB] Pendiente invalida; revisa los puntos ingresados.");
      }
    } else {
      Serial.println("[CALIB] Los puntos tienen el mismo RPS; agrega otra velocidad distinta.");
    }
  }
}

void imprimirInfoCalibracion() {
  Serial.println("--- CALIBRACION ANEMOMETRO ---");
  Serial.printf("Formula activa : v = %.4f * RPS + %.2f km/h\n", anemA, anemB);
  Serial.printf("RPS promedio   : %.2f\n", obtenerRPSPromedio());
  Serial.printf("Puntos sesion  : %d/%d\n", calPuntos, MAX_PUNTOS_CAL);
  for (int i = 0; i < calPuntos; i++) {
    Serial.printf("  #%d -> RPS=%.2f | V_real=%.1f km/h\n", i + 1, calRPS[i], calVel[i]);
  }
  Serial.println("Comandos: v=0, v=10, v=20, v=30, v=40 | calinfo | calclear | calreset");
}

void procesarComandoSerial(String cmd) {
  cmd.trim();
  cmd.toLowerCase();
  if (cmd.length() == 0) return;

  if (cmd.startsWith("v=")) {
    float velReal = cmd.substring(2).toFloat();
    if (velReal < 0.0 || velReal > 200.0) {
      Serial.println("[CALIB] Velocidad fuera de rango (0 a 200 km/h).");
      return;
    }
    if (calPuntos >= MAX_PUNTOS_CAL) {
      Serial.println("[CALIB] Lista llena (15 pts). Usa 'calclear' para iniciar nueva sesion.");
      return;
    }
    float rpsProm = obtenerRPSPromedio();
    calRPS[calPuntos] = rpsProm;
    calVel[calPuntos] = velReal;
    calPuntos++;
    Serial.printf("[CALIB] Punto #%d agregado: RPS=%.2f -> V_GPS=%.1f km/h\n", calPuntos, rpsProm, velReal);
    recalcularAnemometro();
  } else if (cmd == "calinfo" || cmd == "?") {
    imprimirInfoCalibracion();
  } else if (cmd == "calclear") {
    calPuntos = 0;
    Serial.println("[CALIB] Puntos de sesion limpiados (se mantiene la formula guardada).");
  } else if (cmd == "calreset") {
    calPuntos = 0;
    anemA = ANEM_A_DEFAULT;
    anemB = ANEM_B_DEFAULT;
    guardarCalibracionAnem();
    Serial.printf("[CALIB] Restaurado por defecto: v = %.4f * RPS + %.2f km/h\n", anemA, anemB);
  } else {
    Serial.println("[CALIB] Comando no reconocido. Usa: v=0, v=20, calinfo, calclear, calreset");
  }
}

void atenderSerialCalibracion() {
  while (Serial.available()) {
    char c = (char)Serial.read();
    if (c == '\r') continue;
    if (c == '\n') {
      procesarComandoSerial(cmdSerial);
      cmdSerial = "";
    } else if (cmdSerial.length() < 32) {
      cmdSerial += c;
    }
  }
}

// --- INICIALIZACIÓN Y LECTURA MPU-9250 (SIN LIBRERÍAS EXTRA) ---
void iniciarMPU9250() {
  Wire.beginTransmission(MPU_ADDR);
  Wire.write(0x75); // Registro WHO_AM_I
  if (Wire.endTransmission(false) != 0) {
    mpuDisponible = false;
    return;
  }
  Wire.requestFrom(MPU_ADDR, 1);
  if (Wire.available()) {
    uint8_t who = Wire.read();
    // 0x71 = MPU9250, 0x73 = MPU9255, 0x70 = MPU6500
    if (who == 0x71 || who == 0x73 || who == 0x70) {
      // Despertar MPU-9250
      Wire.beginTransmission(MPU_ADDR);
      Wire.write(0x6B);
      Wire.write(0x00);
      Wire.endTransmission();
      delay(10);

      // Habilitar I2C Bypass para acceder al magnetómetro AK8963
      Wire.beginTransmission(MPU_ADDR);
      Wire.write(0x37);
      Wire.write(0x02);
      Wire.endTransmission();
      delay(10);

      // Verificar magnetómetro AK8963
      Wire.beginTransmission(MAG_ADDR);
      Wire.write(0x00);
      if (Wire.endTransmission(false) == 0) {
        Wire.requestFrom(MAG_ADDR, 1);
        if (Wire.available() && Wire.read() == 0x48) {
          // Modo medición continua 16-bit, 100Hz
          Wire.beginTransmission(MAG_ADDR);
          Wire.write(0x0A);
          Wire.write(0x16);
          Wire.endTransmission();
          magDisponible = true;
        }
      }
      mpuDisponible = true;
    }
  }
}

const char* obtenerPuntoCardinal(int grados) {
  if (grados >= 338 || grados < 23)  return "NORTE";
  if (grados >= 23  && grados < 68)  return "NE";
  if (grados >= 68  && grados < 113) return "ESTE";
  if (grados >= 113 && grados < 158) return "SE";
  if (grados >= 158 && grados < 203) return "SUR";
  if (grados >= 203 && grados < 248) return "SO";
  if (grados >= 248 && grados < 293) return "OESTE";
  return "NO";
}

void leerOrientacionMPU(int &rumbo, int &pitch, int &roll, bool &ok) {
  if (!mpuDisponible) {
    // Reintentar detección en caliente por si se conecta después
    iniciarMPU9250();
    if (!mpuDisponible) {
      ok = false;
      return;
    }
  }

  // Leer acelerómetro (registros 0x3B a 0x40)
  Wire.beginTransmission(MPU_ADDR);
  Wire.write(0x3B);
  if (Wire.endTransmission(false) != 0) {
    mpuDisponible = false;
    magDisponible = false;
    ok = false;
    return;
  }
  Wire.requestFrom(MPU_ADDR, 6);
  if (Wire.available() < 6) {
    ok = false;
    return;
  }

  uint8_t b[6];
  for (int i = 0; i < 6; i++) b[i] = Wire.read();
  int16_t ax = (int16_t)((b[0] << 8) | b[1]);
  int16_t ay = (int16_t)((b[2] << 8) | b[3]);
  int16_t az = (int16_t)((b[4] << 8) | b[5]);

  // Cálculo de inclinación X (Pitch) e Y (Roll) en grados
  float fax = (float)ax;
  float fay = (float)ay;
  float faz = (float)az;

  pitch = (int)round(atan2(fax, sqrt(fay * fay + faz * faz)) * 180.0 / PI);
  roll  = (int)round(atan2(fay, sqrt(fax * fax + faz * faz)) * 180.0 / PI);

  // Leer magnetómetro AK8963 para brújula
  rumbo = -1;
  if (magDisponible) {
    Wire.beginTransmission(MAG_ADDR);
    Wire.write(0x02); // Registro ST1
    if (Wire.endTransmission(false) == 0) {
      Wire.requestFrom(MAG_ADDR, 1);
      if (Wire.available() && (Wire.read() & 0x01)) {
        Wire.beginTransmission(MAG_ADDR);
        Wire.write(0x03); // HXL a HZH + ST2 (7 bytes obligatorios)
        Wire.endTransmission(false);
        Wire.requestFrom(MAG_ADDR, 7);
        if (Wire.available() == 7) {
          uint8_t m[7];
          for (int i = 0; i < 7; i++) m[i] = Wire.read();
          int16_t mxa = (int16_t)((m[1] << 8) | m[0]);   // eje X del AK8963
          int16_t mya = (int16_t)((m[3] << 8) | m[2]);   // eje Y del AK8963
          uint8_t st2 = m[6];
          if (!(st2 & 0x08)) { // Sin desbordamiento magnético
            // Alinear ejes AK8963 -> ejes del acelerometro MPU: X_mpu = Y_ak, Y_mpu = X_ak
            float mx = (float)mya - MAG_OFF_X;
            float my = (float)mxa - MAG_OFF_Y;
            float heading = atan2(my, mx) * 180.0 / PI + DECLINACION_MAG;
            while (heading < 0) heading += 360.0;
            while (heading >= 360.0) heading -= 360.0;
            rumbo = (int)round(heading) % 360;
          }
        }
      }
    }
  }
  ok = true;
}

// Lectura del DHT22 cada 2 segundos con persistencia del último valor válido
void leerDHT22() {
  if (millis() - tiempoUltimaLecturaDHT < 2000) return;
  tiempoUltimaLecturaDHT = millis();

  float t = dht.readTemperature();
  float h = dht.readHumidity();

  if (!isnan(t) && t >= -40.0 && t <= 80.0) ultimaTemp = t;
  if (!isnan(h) && h >= 0.0 && h <= 100.0)  ultimaHum = h;
}

// Inicializacion del BME280 en modo SPI
void iniciarBME() {
  if (bme.begin()) {
    bme.setSampling(Adafruit_BME280::MODE_NORMAL,
                    Adafruit_BME280::SAMPLING_X2,   // Temperatura
                    Adafruit_BME280::SAMPLING_X16,  // Presión
                    Adafruit_BME280::SAMPLING_X1,   // Humedad
                    Adafruit_BME280::FILTER_X16,
                    Adafruit_BME280::STANDBY_MS_500);
    bmeDisponible = true;
    Serial.println("[BME280] OK en SPI (25/26/27/32).");
  } else {
    bmeDisponible = false;
    Serial.println("[BME280] No responde en SPI. Revisar cableado. (Si el modulo es BMP280 el ID es 0x58 y esta libreria no lo acepta)");
  }
}

// Lectura de temperatura, humedad y presion del BME280 con reintento cada 30s
void leerBME(float &t, float &h, float &p) {
  if (!bmeDisponible) {
    if (millis() - tiempoReintentoBME > 30000) {
      tiempoReintentoBME = millis();
      iniciarBME();
    }
    if (!bmeDisponible) {
      t = NAN;
      h = NAN;
      p = NAN;
      return;
    }
  }

  float t_raw = bme.readTemperature();
  float h_raw = bme.readHumidity();
  float p_raw = bme.readPressure() / 100.0F; // Pa -> hPa

  t = (!isnan(t_raw) && t_raw >= -40.0 && t_raw <= 80.0) ? t_raw : NAN;
  h = (!isnan(h_raw) && h_raw >= 0.0   && h_raw <= 100.0) ? h_raw : NAN;
  p = (!isnan(p_raw) && p_raw >= 600.0 && p_raw <= 1150.0) ? p_raw : NAN;

  // Si todas las lecturas fallan, marcar para reintentar conexion en 30s
  if (isnan(t) && isnan(h) && isnan(p)) {
    bmeDisponible = false;
  }
}

// Actualización de las pantallas de la LCD 16x2 cada 5 segundos
void actualizarLCD(float t, float h, float p, float v_inst) {
  if (millis() - tiempoCambioPantalla >= INTERVALO_PANTALLA_MS) {
    tiempoCambioPantalla = millis();
    int totalPantallas = modoPortal ? 3 : 2;
    pantallaActual = (pantallaActual + 1) % totalPantallas;
  }

  char l1[20], l2[20];
  char deg = (char)223; // Símbolo '°' en LCD estándar HD44780

  if (pantallaActual == 0) {
    // --- PANTALLA 1: DATOS METEOROLÓGICOS ---
    // T:23.5C H:65%
    // P:1013Pa V:12kh
    char strT[8], strH[6], strP[8], strV[8];

    if (isnan(t)) snprintf(strT, sizeof(strT), "--.-C");
    else          snprintf(strT, sizeof(strT), "%.1fC", t);

    if (isnan(h)) snprintf(strH, sizeof(strH), "--%%");
    else          snprintf(strH, sizeof(strH), "%.0f%%", h);

    if (isnan(p)) snprintf(strP, sizeof(strP), "----Pa");
    else          snprintf(strP, sizeof(strP), "%.0fPa", p);

    snprintf(strV, sizeof(strV), "%.0fkh", v_inst);

    snprintf(l1, sizeof(l1), "T:%s H:%s", strT, strH);
    int len1 = strlen(l1);
    while (len1 < 15) l1[len1++] = ' ';
    l1[15] = (count > 0) ? '*' : ' ';
    l1[16] = '\0';

    snprintf(l2, sizeof(l2), "P:%s V:%s", strP, strV);
    int len2 = strlen(l2);
    while (len2 < 16) l2[len2++] = ' ';
    l2[16] = '\0';

  } else if (pantallaActual == 1) {
    // --- PANTALLA 2: ORIENTACIÓN (MPU-9250) ---
    // Conectado:                Desconectado:
    // B:180° SUR               B:---° ---
    // X: 12° Y: -5°            X:--° Y:--°
    int rumbo = -1, pitch = 0, roll = 0;
    bool mpuOk = false;
    leerOrientacionMPU(rumbo, pitch, roll, mpuOk);

    if (!mpuOk) {
      snprintf(l1, sizeof(l1), "B:---%c ---      ", deg);
      snprintf(l2, sizeof(l2), "X:--%c Y:--%c     ", deg, deg);
    } else {
      if (rumbo >= 0) {
        snprintf(l1, sizeof(l1), "B:%03d%c %-9s", rumbo, deg, obtenerPuntoCardinal(rumbo));
      } else {
        snprintf(l1, sizeof(l1), "B:---%c SIN MAG  ", deg);
      }
      snprintf(l2, sizeof(l2), "X:%3d%c Y:%3d%c   ", pitch, deg, roll, deg);
    }
    l1[16] = '\0';
    l2[16] = '\0';
  } else {
    // --- PANTALLA 3: MODO PORTAL CAUTIVO ---
    snprintf(l1, sizeof(l1), "AP: DAV-Mobile  ");
    snprintf(l2, sizeof(l2), "IP 192.168.4.1  ");
    l1[16] = '\0';
    l2[16] = '\0';
  }

  lcd.setCursor(0, 0); lcd.print(l1);
  lcd.setCursor(0, 1); lcd.print(l2);
}

// Envío a ArcGIS Online
bool procesarEnvio(float t, float h, float p, float v, int rumbo, int pitch, int roll, bool mpuOk, long long ts, const char* tipo) {
  if (isnan(t) || isnan(h) || isnan(v)) {
    Serial.printf("[%s] ERROR: lectura NaN detectada. Envio cancelado.\n", tipo);
    return false;
  }
  if (ts < 1700000000000LL) {
    Serial.printf("[%s] ERROR: reloj NTP no sincronizado.\n", tipo);
    return false;
  }

  if (WiFi.status() != WL_CONNECTED) return false;   // el buffer offline guarda el dato

  HTTPClient http;
  http.begin(serverName);
  http.setConnectTimeout(2000);
  http.setTimeout(3000);
  http.addHeader("Content-Type", "application/x-www-form-urlencoded");

  // Armado del JSON para ArcGIS Online
  String data = "f=json&features=[{\"attributes\":{";
  data += "\"temperatura\":" + String(t, 2) + ",";
  data += "\"humedad\":" + String(h, 2) + ",";
  if (!isnan(p)) {
    data += "\"presion\":" + String(p, 2) + ",";
  }
  if (mpuOk) {
    data += "\"inclinacion_x\":" + String(pitch) + ",";
    data += "\"inclinacion_y\":" + String(roll) + ",";
    if (rumbo >= 0) {
      data += "\"rumbo\":" + String(rumbo) + ",";
      data += "\"punto_cardinal\":\"" + String(obtenerPuntoCardinal(rumbo)) + "\",";
    }
  }
  data += "\"velocidad_viento\":" + String(v, 2) + ",";
  data += "\"fecha_lectura\":" + String(ts);
  data += "}}]";

  Serial.printf("[%s] T:%.1f H:%.1f P:%s V:%.1f B:%s ... ", tipo, t, h,
                isnan(p) ? "--" : String(p, 2).c_str(), v,
                (mpuOk && rumbo >= 0) ? (String(rumbo) + " " + obtenerPuntoCardinal(rumbo)).c_str() : "--");

  int httpCode = http.POST(data);
  bool success = false;

  if (httpCode == HTTP_CODE_OK) {
    String response = http.getString();
    if (response.indexOf("success\":true") > -1) {
      Serial.println("OK (ArcGIS recibio correctamente)");
      success = true;
      parpadeoLED();
    } else {
      Serial.println("ERROR JSON ArcGIS: " + response);
    }
  } else {
    Serial.printf("ERROR HTTP: %d - %s\n", httpCode, http.errorToString(httpCode).c_str());
  }

  http.end();
  return success;
}

// --- GESTOR DE REDES WIFI (NVS) ---
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
  for (int i = 0; i < numRedes; i++) {
    if (strcmp(redes[i].ssid, ssid) == 0) {
      pos = i;
      break;
    }
  }
  if (pos == -1) {
    if (numRedes < MAX_REDES) numRedes++;
    pos = numRedes - 1; // si la lista esta llena, sobreescribe la ultima (la mas antigua)
  }
  for (int i = pos; i > 0; i--) redes[i] = redes[i - 1]; // desplazar hacia abajo
  strncpy(redes[0].ssid, ssid, 32); redes[0].ssid[32] = '\0';
  strncpy(redes[0].pass, pass, 63); redes[0].pass[63] = '\0';
  guardarRedes();
}

void guardarPendiente(const String& s, const String& p) {
  prefs.begin("wifi", false);
  prefs.putString("ps", s);
  prefs.putString("pp", p);
  prefs.end();
}

void borrarPendiente() {
  prefs.begin("wifi", false);
  prefs.remove("ps");
  prefs.remove("pp");
  prefs.end();
}

bool intentarConectar(const char* ssid, const char* pass, unsigned long timeoutMs) {
  Serial.printf("[WIFI] Conectando a %s...\n", ssid);
  lcd.clear(); lcd.setCursor(0, 0); lcd.print("Conectando WiFi");
  lcd.setCursor(0, 1); lcd.print(String(ssid).substring(0, 16));
  WiFi.disconnect();
  delay(100);
  WiFi.begin(ssid, pass);
  unsigned long t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < timeoutMs) {
    delay(300);
    Serial.print(".");
  }
  Serial.println();
  return WiFi.status() == WL_CONNECTED;
}

// Escanea y conecta a la primera guardada visible (orden: principal primero). true si conecto.
bool conectarGuardadas() {
  lcd.clear(); lcd.setCursor(0, 0); lcd.print("Buscando redes..");
  int n = WiFi.scanNetworks();
  Serial.printf("[WIFI] %d redes encontradas.\n", n);
  bool ok = false;
  for (int i = 0; i < numRedes && !ok; i++) {
    for (int j = 0; j < n; j++) {
      if (WiFi.SSID(j) == String(redes[i].ssid)) {
        ok = intentarConectar(redes[i].ssid, redes[i].pass, 10000);
        break; // pasar a la siguiente guardada si fallo
      }
    }
  }
  WiFi.scanDelete();
  return ok;
}

void mantenerWiFi() {
  if (modoPortal) return;
  if (WiFi.status() == WL_CONNECTED) return;
  if (millis() - tiempoUltimoIntentoWiFi < 30000) return; // cada 30 s
  tiempoUltimoIntentoWiFi = millis();
  Serial.println("[WIFI] Conexion perdida. Buscando redes guardadas...");
  conectarGuardadas();
}

// --- PORTAL CAUTIVO (DAV-Mobile-Setup) ---
String escaparHTML(String s) {
  s.replace("&", "&amp;");
  s.replace("<", "&lt;");
  s.replace(">", "&gt;");
  s.replace("\"", "&quot;");
  return s;
}

void escanearParaPortal() {
  int n = WiFi.scanNetworks();
  listaRedesHTML = "";
  for (int i = 0; i < n; i++) {
    String s = WiFi.SSID(i);
    if (s.length() == 0) continue; // omitir ocultas
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
  if (servidor.hasArg("r") || listaRedesHTML.length() == 0) escanearParaPortal(); // ?r=1 = reescanear
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
  guardarPendiente(ssid, pass); // se prueba al reiniciar (el AP interferiria con la prueba aqui)
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
  dns.start(53, "*", WiFi.softAPIP()); // portal cautivo: todo dominio -> 192.168.4.1
  escanearParaPortal();
  servidor.on("/", HTTP_GET, paginaPrincipal);
  servidor.on("/save", HTTP_POST, paginaGuardar);
  servidor.onNotFound([]() { // redirigir cualquier URL al panel
    servidor.sendHeader("Location", "http://192.168.4.1/", true);
    servidor.send(302, "text/plain", "");
  });
  servidor.begin();
  tiempoPortalInicio = tiempoUltimoChequeoPortal = millis();
  Serial.println("[PORTAL] AP DAV-Mobile-Setup activo en 192.168.4.1");
}

void atenderPortal() { // llamar en cada vuelta de loop()
  if (!modoPortal) return;
  dns.processNextRequest();
  servidor.handleClient();
  // Cada 2 min, si nadie esta conectado al AP, ver si reaparecio una red guardada
  if (millis() - tiempoUltimoChequeoPortal > 120000 && WiFi.softAPgetStationNum() == 0) {
    tiempoUltimoChequeoPortal = millis();
    int n = WiFi.scanNetworks();
    for (int i = 0; i < numRedes; i++) {
      for (int j = 0; j < n; j++) {
        if (WiFi.SSID(j) == String(redes[i].ssid)) {
          Serial.println("[PORTAL] Red guardada visible. Reiniciando.");
          delay(300);
          ESP.restart();
        }
      }
    }
    WiFi.scanDelete();
  }
}

void bienvenidaImpacto() {
  String l2 = "Polivalente San Nicolas";
  lcd.clear();
  lcd.setCursor(0, 0); lcd.print("DAV Mobile");
  lcd.setCursor(0, 1); lcd.print("Station v5.0");
  delay(1500);
  lcd.clear();
  lcd.setCursor(0, 0); lcd.print("Liceo Excelencia");
  for (int i = 0; i <= (int)l2.length() - 16; i++) {
    lcd.setCursor(0, 1);
    lcd.print(l2.substring(i, i + 16));
    delay(150);
  }
  delay(400);
}

void setup() {
  Serial.begin(115200);
  delay(400);
  Serial.println("\n\n=================================================");
  Serial.println("  DAV MOBILE V5.0 (LCD + MPU9250 I2C | BME SPI)");
  Serial.println("=================================================");

  pinMode(LED_STATUS, OUTPUT);

  // 1. Iniciar bus I2C nativo para Pantalla LCD y MPU-9250 (Pines 21 y 22)
  Wire.begin(21, 22);
  Wire.setClock(100000);
  lcd.init();
  lcd.backlight();
  bienvenidaImpacto();

  // 2. Verificar MPU-9250 en I2C
  iniciarMPU9250();
  if (mpuDisponible) {
    Serial.println("[MPU9250] Sensor de orientacion detectado en I2C.");
  } else {
    Serial.println("[MPU9250] No conectado (Pantalla 2 mostrara guiones).");
  }

  // 3. Iniciar DHT22 en GPIO 13
  dht.begin();
  Serial.println("[DHT22] Sensor de temperatura/humedad iniciado en GPIO 13.");

  // 4. Iniciar BME280 en modo SPI (Pines 25, 26, 27, 32)
  iniciarBME();

  // 5. Conexión WiFi con NVS, escaneo y portal cautivo
  cargarRedes();
  WiFi.mode(WIFI_STA);
  WiFi.disconnect();
  delay(100);

  bool conectado = false;

  // 1) Red pendiente (recien elegida en el portal): probarla primero
  prefs.begin("wifi", false);
  String ps = prefs.getString("ps", "");
  String pp = prefs.getString("pp", "");
  prefs.end();
  if (ps.length() > 0) {
    conectado = intentarConectar(ps.c_str(), pp.c_str(), 12000);
    if (conectado) definirPrincipal(ps.c_str(), pp.c_str()); // queda como principal
    else avisoUltimoFallo = true;                            // clave mala o fuera de alcance
    borrarPendiente();
  }

  // 2) Redes guardadas visibles
  if (!conectado) conectado = conectarGuardadas();

  // 3) Nada disponible: portal cautivo
  if (!conectado) iniciarPortal();

  lcd.clear();
  if (WiFi.status() == WL_CONNECTED) {
    lcd.setCursor(0, 0); lcd.print("WiFi Conectado!");
    lcd.setCursor(0, 1); lcd.print(WiFi.SSID().substring(0, 16));
    Serial.printf("\n[WIFI] Conectado exitosamente a: %s (IP: %s)\n",
                  WiFi.SSID().c_str(), WiFi.localIP().toString().c_str());
  } else if (modoPortal) {
    lcd.setCursor(0, 0); lcd.print("AP: DAV-Mobile");
    lcd.setCursor(0, 1); lcd.print("IP 192.168.4.1");
    Serial.println("\n[WIFI] Modo Portal activo. Conectarse a DAV-Mobile-Setup.");
  } else {
    lcd.setCursor(0, 0); lcd.print("WiFi ERROR!");
    lcd.setCursor(0, 1); lcd.print("Modo Offline");
    Serial.println("\n[WIFI] Sin conexion a ninguna red. Modo offline activo.");
  }
  delay(1200);

  // 6. Sincronización NTP
  configTime(0, 0, "pool.ntp.org", "time.google.com");
  setenv("TZ", TZ_INFO, 1);
  tzset();
  if (WiFi.status() == WL_CONNECTED) {
    struct tm timeinfo;
    for (int i = 0; i < 12 && !getLocalTime(&timeinfo); i++) {
      delay(400);
    }
  }

  // 7. Configuración Anemómetro
  cargarCalibracionAnem();
  pinMode(ENCODER_PIN, INPUT_PULLUP);
  attachInterrupt(digitalPinToInterrupt(ENCODER_PIN), counter, RISING);
  tiempoUltimoCalculo = millis();
  tiempoCambioPantalla = millis();

  lcd.clear();
}

void loop() {
  atenderPortal();
  atenderSerialCalibracion();
  mantenerWiFi();
  leerDHT22();

  unsigned long ahora = millis();
  unsigned long dt_ms = ahora - tiempoUltimoCalculo;
  if (dt_ms < 1000) return;

  // Captura atómica de pulsos con tiempo transcurrido exacto
  noInterrupts();
  int pulsosCapturados = pulses;
  pulses = 0;
  interrupts();
  tiempoUltimoCalculo = ahora;

  float dt_seg = (float)dt_ms / 1000.0;
  float rps = ((float)pulsosCapturados / 20.0) / dt_seg;
  rpsHist[rpsHistIdx] = rps;
  rpsHistIdx = (rpsHistIdx + 1) % 3;
  if (rpsHistCount < 3) rpsHistCount++;

  float v_inst = (rps > 0.001) ? (anemA * rps + anemB) : 0.0; // km/h
  if (v_inst < 0.0) v_inst = 0.0;

  float tBME = NAN, hBME = NAN, p = NAN;
  leerBME(tBME, hBME, p);

  float tDHT = ultimaTemp;
  float hDHT = ultimaHum;

  // Sensor principal: BME280 | Respaldo: DHT22 (solo si BME280 falla o da NaN)
  float t = !isnan(tBME) ? tBME : tDHT;
  float h = !isnan(hBME) ? hBME : hDHT;

  // Comparacion por Monitor Serial entre BME280, DHT22 y Anemometro
  Serial.printf("[SENSORES] BME280 -> T:%s H:%s P:%s | DHT22 -> T:%s H:%s | Usando(%s/%s) | V:%.1f km/h (RPS:%.2f)\n",
                isnan(tBME) ? "--" : String(tBME, 1).c_str(),
                isnan(hBME) ? "--" : String(hBME, 1).c_str(),
                isnan(p)    ? "--" : String(p, 1).c_str(),
                isnan(tDHT) ? "--" : String(tDHT, 1).c_str(),
                isnan(hDHT) ? "--" : String(hDHT, 1).c_str(),
                !isnan(tBME) ? "BME" : (!isnan(tDHT) ? "DHT" : "--"),
                !isnan(hBME) ? "BME" : (!isnan(hDHT) ? "DHT" : "--"),
                v_inst, rps);

  // Actualización rotativa cada 5 segundos de LCD 16x2
  actualizarLCD(t, h, p, v_inst);

  sumaVelocidades += v_inst;
  segundosContados++;

  // Ciclo de transmisión cada 5 segundos
  if (segundosContados >= 5) {
    float promV = sumaVelocidades / (float)segundosContados;
    long long ts = (long long)time(NULL) * 1000LL;

    int rumbo = -1, pitch = 0, roll = 0;
    bool mpuOk = false;
    leerOrientacionMPU(rumbo, pitch, roll, mpuOk);

    if (procesarEnvio(t, h, p, promV, rumbo, pitch, roll, mpuOk, ts, "REALTIME")) {
      // Vaciar cola offline acumulada
      int lote = 0;
      while (count > 0 && lote < 5 && WiFi.status() == WL_CONNECTED) {
        Medicion m = buffer[tail];
        if (!procesarEnvio(m.temp, m.hum, m.presion, m.viento, m.rumbo, m.pitch, m.roll, m.mpuOk, m.timestamp, "BUFFER")) break;
        tail = (tail + 1) % BUFFER_SIZE;
        count--;
        lote++;
        delay(50);
      }
    } else if (!isnan(t) && !isnan(h) && ts >= 1700000000000LL) {
      if (count == BUFFER_SIZE) {
        tail = (tail + 1) % BUFFER_SIZE;
        count--;
      }
      buffer[head] = { t, h, p, promV, rumbo, pitch, roll, mpuOk, ts };
      head = (head + 1) % BUFFER_SIZE;
      count++;
      Serial.printf("[BUFFER] Guardado offline: %d/%d\n", count, BUFFER_SIZE);
    }

    sumaVelocidades = 0;
    segundosContados = 0;
  }
}