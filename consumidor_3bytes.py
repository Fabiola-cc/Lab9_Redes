"""
Laboratorio 9 - Redes
Consumidor con restricción de 3 bytes
"""

from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from collections import deque
from codificador import CodificadorIoT

# ============ CONFIGURACIÓN ============
KAFKA_SERVER = "iot.redesuvg.cloud:9092"
KAFKA_TOPIC = '22787'  # Tu carné

# Almacenar últimos 50 datos
temperaturas = deque(maxlen=50)
humedades = deque(maxlen=50)
direcciones_viento = deque(maxlen=50)

# Mapeo de direcciones
DIRECCIONES = {'N': 0, 'NO': 1, 'O': 2, 'SO': 3, 'S': 4, 'SE': 5, 'E': 6, 'NE': 7}
DIRECCIONES_NOMBRES = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']

# ============ CREAR CONSUMIDOR ============
print("=" * 60)
print("CONSUMIDOR IoT - RESTRICCIÓN 3 BYTES")
print("=" * 60)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset='latest',
    group_id='grupo-3bytes'
    # NO usar deserializer, recibimos bytes raw
)

print(f"✓ Conectado al topic: {KAFKA_TOPIC}")
print(f"✓ Esperando mensajes de 3 bytes...\n")

# ============ CONFIGURAR GRÁFICOS ============
plt.ion()
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8))
fig.suptitle('Estación Meteorológica - 3 BYTES', fontsize=14, fontweight='bold')

contador = 0

# ============ LOOP PRINCIPAL ============
try:
    while True:
        mensajes = consumer.poll(timeout_ms=1000)
        
        for topic_partition, records in mensajes.items():
            for msg in records:
                # Recibir bytes
                datos_codificados = msg.value
                
                # DECODIFICAR
                datos = CodificadorIoT.decode(datos_codificados)
                
                temp = datos['temperatura']
                hum = datos['humedad']
                viento = datos['direccion_viento']
                
                # Guardar
                temperaturas.append(temp)
                humedades.append(hum)
                direcciones_viento.append(DIRECCIONES[viento])
                
                contador += 1
                
                # Mostrar en consola
                print(f"[Mensaje #{contador}]")
                print(f"  Recibido: {len(datos_codificados)} bytes -> {datos_codificados.hex()}")
                print(f"  Decodificado: Temp={temp}°C, Hum={hum}%, Viento={viento}\n")
                
                # ============ ACTUALIZAR GRÁFICOS ============
                if len(temperaturas) > 0:
                    x = list(range(len(temperaturas)))
                    
                    ax1.clear()
                    ax2.clear()
                    ax3.clear()
                    
                    # TEMPERATURA
                    ax1.plot(x, list(temperaturas), 'r-o', linewidth=2, markersize=4)
                    ax1.set_ylabel('Temperatura (°C)')
                    ax1.set_title('TEMPERATURA')
                    ax1.grid(True, alpha=0.3)
                    ax1.set_ylim(0, 110)
                    
                    # HUMEDAD
                    ax2.plot(x, list(humedades), 'b-s', linewidth=2, markersize=4)
                    ax2.set_ylabel('Humedad (%)')
                    ax2.set_title('HUMEDAD')
                    ax2.grid(True, alpha=0.3)
                    ax2.set_ylim(0, 100)
                    
                    # VIENTO
                    ax3.scatter(x, list(direcciones_viento), s=80, c='green', marker='D')
                    ax3.set_ylabel('Dirección')
                    ax3.set_xlabel('Número de Lectura')
                    ax3.set_title('DIRECCIÓN DEL VIENTO')
                    ax3.set_yticks([0, 1, 2, 3, 4, 5, 6, 7])
                    ax3.set_yticklabels(DIRECCIONES_NOMBRES)
                    ax3.grid(True, alpha=0.3, axis='x')
                    
                    plt.tight_layout()
                    plt.pause(0.01)

except KeyboardInterrupt:
    print("\nDeteniendo consumidor...")
finally:
    consumer.close()
    plt.close()
    print("✓ Cerrado correctamente")