import pandas as pd
import matplotlib.pyplot as plt
import pickle
import os
import sys
from sklearn.model_selection import train_test_split

# Фикс импортов для запуска из корня
sys.path.append(os.getcwd())
try:
    from test_folder.data_loader_test2 import load_data_from_clickhouse
except ImportError:
    from data_loader import load_data_from_clickhouse

# Пути
MODEL_PATH = os.path.join(os.path.dirname(__file__), 'weather_model.pkl')
OUTPUT_IMAGE = os.path.join(os.path.dirname(__file__), 'model_results_plot.png')

def visualize_prediction():
    print("--- ВИЗУАЛИЗАЦИЯ РЕЗУЛЬТАТОВ МОДЕЛИ ---")

    # 1. Загрузка модели
    if not os.path.exists(MODEL_PATH):
        print(f"❌ Модель не найдена: {MODEL_PATH}")
        print("Сначала запустите train_model.py")
        return

    print("Загрузка модели...")
    with open(MODEL_PATH, 'rb') as f:
        model = pickle.load(f)

    # 2. Загрузка данных
    df = load_data_from_clickhouse()
    
    # 3. Подготовка тестовых данных
    # Важно повторить то же разбиение, что и при обучении (shuffle=False)
    X = df.drop(columns=['temperature_c'])
    y = df['temperature_c']
    
    # Нам нужны только тестовые данные (последние 20%)
    _, X_test, _, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    
    print("Выполнение предсказаний...")
    predictions = model.predict(X_test)

    # 4. Построение графиков
    print("Генерация графиков...")
    
    # Создаем полотно с двумя графиками
    plt.figure(figsize=(16, 8))

    # --- График 1: Динамика (берем последние 150 точек для наглядности) ---
    plt.subplot(1, 2, 1)
    subset_size = 150
    
    # Конвертируем в numpy для удобства срезов
    y_test_np = y_test.values
    preds_np = predictions
    
    plt.plot(y_test_np[:subset_size], label='Реальная (Факт)', color='blue', linewidth=2)
    plt.plot(preds_np[:subset_size], label='Предсказание (ML)', color='orange', linestyle='--', linewidth=2)
    
    plt.title(f'Сравнение факта и прогноза (фрагмент {subset_size} часов)')
    plt.ylabel('Температура (°C)')
    plt.xlabel('Часы (Time steps)')
    plt.legend()
    plt.grid(True, alpha=0.3)

    # --- График 2: Корреляция (Scatter Plot) ---
    plt.subplot(1, 2, 2)
    plt.scatter(y_test, predictions, alpha=0.5, s=10, color='green')
    
    # Рисуем идеальную диагональ
    min_val = min(y_test.min(), predictions.min())
    max_val = max(y_test.max(), predictions.max())
    plt.plot([min_val, max_val], [min_val, max_val], color='red', linestyle='--', label='Идеал')
    
    plt.title('Корреляция: Факт vs Прогноз')
    plt.xlabel('Реальная температура')
    plt.ylabel('Предсказанная температура')
    plt.legend()
    plt.grid(True, alpha=0.3)

    # Сохранение
    plt.tight_layout()
    plt.savefig(OUTPUT_IMAGE)
    print(f"✅ График сохранен в: {OUTPUT_IMAGE}")
    
    # Если запускаете локально, можно раскомментировать, чтобы показать окно
    # plt.show()

if __name__ == "__main__":
    visualize_prediction()