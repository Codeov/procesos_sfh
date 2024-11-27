import pyautogui
import time

# Configura el intervalo en segundos entre movimientos
intervalo = 5  # 1 minuto

try:
    while True:
        # Mueve el mouse en pequeños movimientos para simular actividad
        pyautogui.move(0, 100)  # Mueve el mouse hacia abajo 1 píxel
        time.sleep(0.5)
        pyautogui.move(0, -100)  # Mueve el mouse hacia arriba 1 píxel
        time.sleep(intervalo - 0.5)
except KeyboardInterrupt:
    print("Script terminado.")
